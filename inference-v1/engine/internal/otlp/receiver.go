package otlp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // accept gzip-compressed gRPC requests
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/observe"
)

const (
	// HTTPPath is where OTLP/HTTP exporters post traces.
	HTTPPath = "/v1/traces"

	maxRequestBytes   = 16 << 20
	contentTypeProto  = "application/x-protobuf"
	contentTypeJSON   = "application/json"
	retryAfterSeconds = "1"
)

// Sink stores received spans durably. The map goes from trace ID (32 hex
// characters) to a serialized TracesData, as produced by Split.
type Sink interface {
	PublishSpans(ctx context.Context, byTrace map[string][]byte) error
}

// Receiver accepts OTLP trace exports over gRPC and HTTP and hands the
// spans to a Sink. It answers once the sink has stored them, so an exporter
// that got a success can forget the batch.
type Receiver struct {
	coltracepb.UnimplementedTraceServiceServer

	sink     Sink
	reporter observe.Reporter

	grpcServer *grpc.Server
	httpServer *http.Server
}

// ReceiverConfig is what a Receiver is built from.
type ReceiverConfig struct {
	Sink     Sink
	Reporter observe.Reporter // defaults to observe.Discard
}

func NewReceiver(cfg ReceiverConfig) *Receiver {
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	r := &Receiver{sink: cfg.Sink, reporter: cfg.Reporter}

	r.grpcServer = grpc.NewServer(grpc.MaxRecvMsgSize(maxRequestBytes))
	coltracepb.RegisterTraceServiceServer(r.grpcServer, r)

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+HTTPPath, r.handleHTTP)
	r.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	return r
}

// HTTPHandler returns the OTLP/HTTP handler, for tests.
func (r *Receiver) HTTPHandler() http.Handler { return r.httpServer.Handler }

// Start binds both listeners and serves on them in the background. An empty
// address disables that protocol. It fails if an address cannot be bound.
func (r *Receiver) Start(grpcAddr, httpAddr string) error {
	var grpcLis, httpLis net.Listener
	var err error
	if grpcAddr != "" {
		if grpcLis, err = net.Listen("tcp", grpcAddr); err != nil {
			return fmt.Errorf("otlp: gRPC listener: %w", err)
		}
	}
	if httpAddr != "" {
		if httpLis, err = net.Listen("tcp", httpAddr); err != nil {
			if grpcLis != nil {
				grpcLis.Close()
			}
			return fmt.Errorf("otlp: HTTP listener: %w", err)
		}
	}
	if grpcLis != nil {
		go func() {
			if err := r.grpcServer.Serve(grpcLis); err != nil {
				r.reporter.Report(context.Background(), fault.Unavailable(fault.OTLP, "serve gRPC", err).AsSevere())
			}
		}()
	}
	if httpLis != nil {
		go func() {
			if err := r.httpServer.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				r.reporter.Report(context.Background(), fault.Unavailable(fault.OTLP, "serve HTTP", err).AsSevere())
			}
		}()
	}
	return nil
}

// Shutdown stops accepting exports and waits for the ones in progress,
// until ctx ends.
func (r *Receiver) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.grpcServer.GracefulStop()
		close(done)
	}()
	err := r.httpServer.Shutdown(ctx)
	select {
	case <-done:
	case <-ctx.Done():
		r.grpcServer.Stop()
		err = errors.Join(err, ctx.Err())
	}
	return err
}

// Export implements the OTLP gRPC TraceService.
func (r *Receiver) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	if err := r.accept(ctx, req); err != nil {
		// Unavailable tells the exporter to retry with backoff.
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (r *Receiver) accept(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) error {
	byTrace, skipped, err := Split(req.GetResourceSpans())
	if err != nil {
		return err
	}
	if skipped > 0 {
		r.reporter.Report(ctx, fault.Invalid(fault.OTLP, "accept spans",
			fault.Msg("spans without a valid trace or span ID were skipped")).With("spans", skipped))
	}
	if len(byTrace) == 0 {
		return nil
	}
	if err := r.sink.PublishSpans(ctx, byTrace); err != nil {
		r.reporter.Report(ctx, fault.New(fault.KindOf(err), fault.OTLP, "store spans", err).With("traces", len(byTrace)))
		return errors.New("span store unavailable, retry later")
	}
	return nil
}

func (r *Receiver) handleHTTP(w http.ResponseWriter, req *http.Request) {
	contentType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if contentType != contentTypeProto && contentType != contentTypeJSON {
		// The error has to be written in some encoding; JSON is readable.
		writeStatus(w, contentTypeJSON, http.StatusUnsupportedMediaType, codes.InvalidArgument,
			"Content-Type must be application/x-protobuf or application/json")
		return
	}

	body, err := readBody(w, req)
	if err != nil {
		code := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeStatus(w, contentType, code, codes.InvalidArgument, err.Error())
		return
	}

	var export coltracepb.ExportTraceServiceRequest
	if contentType == contentTypeJSON {
		err = unmarshalJSON(body, &export)
	} else {
		err = proto.Unmarshal(body, &export)
	}
	if err != nil {
		writeStatus(w, contentType, http.StatusBadRequest, codes.InvalidArgument, "invalid OTLP payload: "+err.Error())
		return
	}

	if err := r.accept(req.Context(), &export); err != nil {
		// 503 is one of the statuses OTLP exporters retry.
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeStatus(w, contentType, http.StatusServiceUnavailable, codes.Unavailable, err.Error())
		return
	}
	writeMessage(w, contentType, http.StatusOK, &coltracepb.ExportTraceServiceResponse{})
}

func readBody(w http.ResponseWriter, req *http.Request) ([]byte, error) {
	var reader io.Reader = http.MaxBytesReader(w, req.Body, maxRequestBytes)
	switch req.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("invalid gzip body: %w", err)
		}
		defer zr.Close()
		// Bound the decompressed size too.
		reader = io.LimitReader(zr, maxRequestBytes+1)
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", req.Header.Get("Content-Encoding"))
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if len(body) > maxRequestBytes {
		return nil, &http.MaxBytesError{Limit: maxRequestBytes}
	}
	return body, nil
}

// writeStatus writes an OTLP error response: a google.rpc.Status in the
// request's encoding.
func writeStatus(w http.ResponseWriter, contentType string, httpCode int, code codes.Code, msg string) {
	writeMessage(w, contentType, httpCode, &statuspb.Status{Code: int32(code), Message: msg})
}

func writeMessage(w http.ResponseWriter, contentType string, httpCode int, msg proto.Message) {
	var body []byte
	if contentType == contentTypeJSON {
		body, _ = protojson.Marshal(msg)
	} else {
		body, _ = proto.Marshal(msg)
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(httpCode)
	_, _ = w.Write(body)
}
