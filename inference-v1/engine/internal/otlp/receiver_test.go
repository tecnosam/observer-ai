package otlp

import (
	"bytes"
	stdgzip "compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// memorySink keeps what the receiver publishes.
type memorySink struct {
	mu      sync.Mutex
	byTrace map[string][][]byte
	err     error
}

func (s *memorySink) PublishSpans(_ context.Context, byTrace map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.byTrace == nil {
		s.byTrace = map[string][][]byte{}
	}
	for id, data := range byTrace {
		s.byTrace[id] = append(s.byTrace[id], data)
	}
	return nil
}

// spanNames returns the names of the spans stored for the test trace.
func (s *memorySink) spanNames(t *testing.T) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, payload := range s.byTrace[testTraceHex] {
		var td tracepb.TracesData
		if err := proto.Unmarshal(payload, &td); err != nil {
			t.Fatal(err)
		}
		for _, rs := range td.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					names = append(names, sp.Name)
				}
			}
		}
	}
	return names
}

func exportRequest() *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{resourceSpans("observer_agent", adkRun()...)},
	}
}

func post(h http.Handler, contentType string, body []byte, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", HTTPPath, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestHTTPProtobuf(t *testing.T) {
	sink := &memorySink{}
	h := NewReceiver(sink, quiet).HTTPHandler()
	body, _ := proto.Marshal(exportRequest())

	w := post(h, "application/x-protobuf", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %q", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("Content-Type = %q", ct)
	}
	var resp coltracepb.ExportTraceServiceResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Errorf("response is not an ExportTraceServiceResponse: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Errorf("rejected spans = %d", resp.GetPartialSuccess().GetRejectedSpans())
	}
	if n := len(sink.spanNames(t)); n != 6 {
		t.Errorf("%d spans stored, want 6", n)
	}
}

func TestHTTPGzip(t *testing.T) {
	sink := &memorySink{}
	h := NewReceiver(sink, quiet).HTTPHandler()
	raw, _ := proto.Marshal(exportRequest())
	var zipped bytes.Buffer
	zw := stdgzip.NewWriter(&zipped)
	zw.Write(raw)
	zw.Close()

	if w := post(h, "application/x-protobuf", zipped.Bytes(), "Content-Encoding", "gzip"); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %q", w.Code, w.Body)
	}
	if n := len(sink.spanNames(t)); n != 6 {
		t.Errorf("%d spans stored, want 6", n)
	}

	if w := post(h, "application/x-protobuf", raw, "Content-Encoding", "gzip"); w.Code != http.StatusBadRequest {
		t.Errorf("body that is not gzip: status = %d, want 400", w.Code)
	}
	if w := post(h, "application/x-protobuf", raw, "Content-Encoding", "br"); w.Code != http.StatusBadRequest {
		t.Errorf("unsupported encoding: status = %d, want 400", w.Code)
	}
}

// OTLP/JSON writes IDs as hex and 64-bit integers as strings.
func TestHTTPJSON(t *testing.T) {
	sink := &memorySink{}
	h := NewReceiver(sink, quiet).HTTPHandler()
	body := `{
	  "resourceSpans": [{
	    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "json-agent"}}]},
	    "scopeSpans": [{
	      "scope": {"name": "manual"},
	      "spans": [{
	        "traceId": "` + testTraceHex + `",
	        "spanId": "00000000000000a1",
	        "name": "agent_run",
	        "kind": 1,
	        "startTimeUnixNano": "1760000000000000000",
	        "endTimeUnixNano": "1760000001000000000",
	        "attributes": [
	          {"key": "openinference.span.kind", "value": {"stringValue": "AGENT"}},
	          {"key": "input.value", "value": {"stringValue": "What is 2 + 2?"}},
	          {"key": "output.value", "value": {"stringValue": "4"}},
	          {"key": "llm.token_count.total", "value": {"intValue": "42"}}
	        ],
	        "status": {"code": 1},
	        "someFutureField": true
	      }, {
	        "traceId": "` + testTraceHex + `",
	        "spanId": "00000000000000a2",
	        "parentSpanId": "00000000000000a1",
	        "name": "calculator",
	        "startTimeUnixNano": 1760000000500000000,
	        "attributes": [
	          {"key": "openinference.span.kind", "value": {"stringValue": "TOOL"}},
	          {"key": "input.value", "value": {"stringValue": "{\"expression\": \"2 + 2\"}"}}
	        ]
	      }]
	    }]
	  }]
	}`

	w := post(h, "application/json; charset=utf-8", []byte(body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if strings.TrimSpace(w.Body.String()) != "{}" {
		t.Errorf("body = %s, want {}", w.Body)
	}

	got, err := Assemble(testTraceHex, sink.byTrace[testTraceHex])
	if err != nil {
		t.Fatal(err)
	}
	if got.Question != "What is 2 + 2?" || got.FinalAnswer != "4" {
		t.Errorf("assembled = %+v", got)
	}
	if len(got.ToolCalls) != 1 || got.ToolCalls[0].Name != "calculator" || string(got.ToolCalls[0].Args) != `{"expression": "2 + 2"}` {
		t.Errorf("tool calls = %+v", got.ToolCalls)
	}

	// The IDs and the timestamp survived the hex and string encodings.
	var td tracepb.TracesData
	if err := proto.Unmarshal(got.OTLP, &td); err != nil {
		t.Fatal(err)
	}
	spans := td.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 || len(spans[0].SpanId) != 8 || spans[0].SpanId[7] != 0xa1 || spans[1].ParentSpanId[7] != 0xa1 {
		t.Errorf("span IDs not decoded from hex: %x / %x", spans[0].SpanId, spans[1].ParentSpanId)
	}
	if spans[0].StartTimeUnixNano != 1760000000000000000 || spans[1].StartTimeUnixNano != 1760000000500000000 {
		t.Errorf("timestamps = %d, %d", spans[0].StartTimeUnixNano, spans[1].StartTimeUnixNano)
	}
}

func TestHTTPRejections(t *testing.T) {
	sink := &memorySink{}
	h := NewReceiver(sink, quiet).HTTPHandler()
	valid, _ := proto.Marshal(exportRequest())

	tests := []struct {
		name        string
		contentType string
		body        []byte
		wantCode    int
		wantMsg     string
	}{
		{"no content type", "", valid, http.StatusUnsupportedMediaType, "Content-Type must be"},
		{"text content type", "text/plain", valid, http.StatusUnsupportedMediaType, "Content-Type must be"},
		{"corrupt protobuf", "application/x-protobuf", []byte{0xff, 0xff, 0xff}, http.StatusBadRequest, "invalid OTLP payload"},
		{"corrupt JSON", "application/json", []byte(`{"resourceSpans": [`), http.StatusBadRequest, "invalid OTLP payload"},
		{"ID that is not hex", "application/json", []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"zz"}]}]}]}`), http.StatusBadRequest, "is not hex"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := post(h, tt.contentType, tt.body)
			if w.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantCode)
			}
			// Errors are a google.rpc.Status in the request's encoding.
			var st statuspb.Status
			var err error
			if tt.contentType == "application/x-protobuf" {
				err = proto.Unmarshal(w.Body.Bytes(), &st)
			} else {
				err = protojson.Unmarshal(w.Body.Bytes(), &st)
			}
			if err != nil {
				t.Fatalf("error body %q is not a Status: %v", w.Body, err)
			}
			if !strings.Contains(st.Message, tt.wantMsg) {
				t.Errorf("message = %q, want it to contain %q", st.Message, tt.wantMsg)
			}
		})
	}
	if len(sink.byTrace) != 0 {
		t.Error("a rejected request reached the sink")
	}

	req := httptest.NewRequest("GET", HTTPPath, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d, want 405", w.Code)
	}
}

// When the span store is down the exporter must be told to retry, in the
// way OTLP exporters understand: HTTP 503.
func TestHTTPSinkUnavailable(t *testing.T) {
	sink := &memorySink{err: errors.New("nats: no servers available")}
	h := NewReceiver(sink, quiet).HTTPHandler()
	body, _ := proto.Marshal(exportRequest())

	w := post(h, "application/x-protobuf", body)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if w.Header().Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q", w.Header().Get("Retry-After"))
	}
	var st statuspb.Status
	if err := proto.Unmarshal(w.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if codes.Code(st.Code) != codes.Unavailable || strings.Contains(st.Message, "nats") {
		t.Errorf("status = %v %q, want Unavailable without internal detail", codes.Code(st.Code), st.Message)
	}
}

// An export with nothing usable in it succeeds without touching the sink.
func TestEmptyExportIsAccepted(t *testing.T) {
	sink := &memorySink{err: errors.New("must not be called")}
	h := NewReceiver(sink, quiet).HTTPHandler()
	body, _ := proto.Marshal(&coltracepb.ExportTraceServiceRequest{})
	if w := post(h, "application/x-protobuf", body); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// startReceiver serves the receiver on loopback ports and returns them.
func startReceiver(t *testing.T, sink Sink) (grpcAddr, httpAddr string) {
	t.Helper()
	free := func() string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()
		return lis.Addr().String()
	}
	grpcAddr, httpAddr = free(), free()
	r := NewReceiver(sink, quiet)
	if err := r.Start(grpcAddr, httpAddr); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r.Shutdown(ctx)
	})
	return grpcAddr, httpAddr
}

func TestGRPCExport(t *testing.T) {
	sink := &memorySink{}
	grpcAddr, _ := startReceiver(t, sink)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := coltracepb.NewTraceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.Export(ctx, exportRequest()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if n := len(sink.spanNames(t)); n != 6 {
		t.Errorf("%d spans stored, want 6", n)
	}

	// Compressed, as the collector sends by default.
	if _, err := client.Export(ctx, exportRequest(), grpc.UseCompressor(gzip.Name)); err != nil {
		t.Fatalf("gzip Export: %v", err)
	}
	if n := len(sink.spanNames(t)); n != 12 {
		t.Errorf("%d spans stored after the second export, want 12", n)
	}

	sink.mu.Lock()
	sink.err = errors.New("nats: timeout")
	sink.mu.Unlock()
	_, err = client.Export(ctx, exportRequest())
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Export with the store down = %v, want Unavailable", err)
	}
}

func TestStartServesHTTPAndFailsOnBusyPort(t *testing.T) {
	sink := &memorySink{}
	grpcAddr, httpAddr := startReceiver(t, sink)

	body, _ := proto.Marshal(exportRequest())
	resp, err := http.Post("http://"+httpAddr+HTTPPath, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	// A second receiver cannot take the same ports.
	err = NewReceiver(sink, quiet).Start(grpcAddr, "")
	if err == nil || !strings.Contains(err.Error(), "gRPC listener") {
		t.Errorf("Start on a busy gRPC port = %v, want a listener error", err)
	}
	err = NewReceiver(sink, quiet).Start("", httpAddr)
	if err == nil || !strings.Contains(err.Error(), "HTTP listener") {
		t.Errorf("Start on a busy HTTP port = %v, want a listener error", err)
	}

	// Both protocols can be switched off.
	off := NewReceiver(sink, quiet)
	if err := off.Start("", ""); err != nil {
		t.Errorf("Start with both disabled = %v", err)
	}
	off.Shutdown(context.Background())
}
