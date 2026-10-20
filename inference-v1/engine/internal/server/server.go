// Package server exposes the engine over HTTP: an asynchronous intake that
// publishes to the work queue, a synchronous scoring path, and health
// checks. It only translates between HTTP and the pipeline; the pipeline
// itself is in internal/pipeline.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/metrics"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	defaultMaxBodyBytes = 8 << 20
	readyTimeout        = 2 * time.Second
	syncFlushTimeout    = 5 * time.Second
	retryAfterSeconds   = "1"
)

// Options configures a Server.
type Options struct {
	Engine   *pipeline.Engine
	Queue    pipeline.Publisher
	Reporter observe.Reporter      // defaults to observe.Discard
	Metrics  common.RequestMetrics // defaults to recording nothing
	// MaxBodyBytes caps a request body; 0 means 8 MiB.
	MaxBodyBytes int64
}

// Server holds the HTTP handlers.
type Server struct {
	opts Options
	mux  *http.ServeMux
}

func New(opts Options) *Server {
	if opts.Reporter == nil {
		opts.Reporter = observe.Discard
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultMaxBodyBytes
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.Noop{}
	}
	s := &Server{opts: opts, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/traces", s.handleEnqueue)
	s.mux.HandleFunc("POST /v1/traces:score", s.handleScore)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("GET /docs", s.handleDocs)
	return s
}

// Handler serves the API, timing every request.
func (s *Server) Handler() http.Handler { return common.InstrumentHTTP("api", s.mux, s.opts.Metrics) }

// handleEnqueue answers 202 only after the queue has confirmed the trace is
// stored, so an accepted trace survives a restart of this process.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	t, ok := s.readTrace(w, r)
	if !ok {
		return
	}
	if err := s.opts.Queue.Publish(r.Context(), t); err != nil {
		s.opts.Reporter.Report(r.Context(), fault.New(fault.KindOf(err), fault.Server, "enqueue trace, rejected it", err).WithTrace(t.TraceID))
		writeFault(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"trace_id": t.TraceID})
}

func (s *Server) handleScore(w http.ResponseWriter, r *http.Request) {
	t, ok := s.readTrace(w, r)
	if !ok {
		return
	}
	out := s.opts.Engine.Process(r.Context(), t)
	if out.Exported {
		out.Exported = s.flush(r.Context(), t.TraceID)
	}
	writeJSON(w, http.StatusOK, resultOf(out))
}

// flush pushes the kept trace out now, so a demo sees it in the trace store
// as soon as the response arrives, not a batch interval later.
func (s *Server) flush(ctx context.Context, traceID string) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncFlushTimeout)
	defer cancel()
	if err := s.opts.Engine.Flush(ctx); err != nil {
		s.opts.Reporter.Report(ctx, fault.Unavailable(fault.Server, "flush exported trace", err).WithTrace(traceID))
		return false
	}
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if problem := s.notReady(ctx); problem != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": problem})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) notReady(ctx context.Context) string {
	if err := s.opts.Queue.Ready(); err != nil {
		return "queue is not reachable: " + err.Error()
	}
	if err := s.opts.Engine.Ready(ctx); err != nil {
		return "model is not reachable: " + err.Error()
	}
	return ""
}

// readTrace decodes and validates the request body, assigning a trace ID if
// the client sent none. On failure it writes the error response.
func (s *Server) readTrace(w http.ResponseWriter, r *http.Request) (trace.Trace, bool) {
	t, err := trace.Decode(http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes))
	if err != nil {
		writeDecodeError(w, err)
		return trace.Trace{}, false
	}
	if t.TraceID == "" {
		t.TraceID = trace.NewID()
	}
	t.OTLP = nil // spans come in through the OTLP receivers, not this API
	return t, true
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

// writeFault answers a failed request with the status its kind calls for.
// Anything worth retrying gets 503 with Retry-After.
func writeFault(w http.ResponseWriter, err error) {
	kind := fault.KindOf(err)
	switch {
	case kind == fault.KindFull:
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "queue full, retry later")
	case fault.Retryable(err):
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, retry later")
	case kind == fault.KindInvalid:
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
