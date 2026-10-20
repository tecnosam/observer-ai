package otlpbackend

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/export"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

func newRecorded(t *testing.T) (*Backend, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	return New(tp), rec
}

func sampleTrace() trace.Trace {
	return trace.Trace{
		TraceID:  "7f3a91c2e8",
		Question: "Which river flows through the capital of France?",
		ToolCalls: []trace.ToolCall{
			{Name: "search", Args: json.RawMessage(`{"query": "capital of France"}`), Status: json.RawMessage(`"ok"`)},
			{Name: "lookup", Args: json.RawMessage(`"Seine"`), Status: json.RawMessage(`"error"`)},
			{Name: "noop"},
		},
		Evidence:    []string{"Paris is the capital of France.", "The Seine flows through Paris."},
		FinalAnswer: "The Seine",
	}
}

func attrMap(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

func wantString(t *testing.T, attrs map[string]attribute.Value, key, want string) {
	t.Helper()
	v, ok := attrs[key]
	if !ok {
		t.Errorf("attribute %s missing", key)
		return
	}
	if v.Type() != attribute.STRING || v.AsString() != want {
		t.Errorf("attribute %s = %v, want %q", key, v.Emit(), want)
	}
}

func wantFloat(t *testing.T, attrs map[string]attribute.Value, key string, want float64) {
	t.Helper()
	v, ok := attrs[key]
	if !ok {
		t.Errorf("attribute %s missing", key)
		return
	}
	if v.Type() != attribute.FLOAT64 || v.AsFloat64() != want {
		t.Errorf("attribute %s = %v, want %v", key, v.Emit(), want)
	}
}

func TestExportKeptTrace(t *testing.T) {
	exp, rec := newRecorded(t)
	err := exp.Send(context.Background(), pipeline.Record{
		Trace: sampleTrace(),
		Decision: pipeline.Decision{
			Keep: true, Scored: true, PKeep: 0.95, PKeepRaw: 0.93,
			Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh,
		},
		Model:   "kev-latest",
		Latency: 93 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	spans := rec.Ended()
	if len(spans) != 4 {
		t.Fatalf("got %d spans, want 4 (root + 3 tool calls)", len(spans))
	}
	// Children end before the root.
	root := spans[3]
	if root.Name() != "agent_run" {
		t.Fatalf("last ended span = %q, want agent_run", root.Name())
	}
	if root.Parent().IsValid() {
		t.Error("root span has a parent")
	}

	a := attrMap(root.Attributes())
	wantString(t, a, "openinference.span.kind", "AGENT")
	wantString(t, a, "input.value", "Which river flows through the capital of France?")
	wantString(t, a, "output.value", "The Seine")
	wantString(t, a, "observer.source_trace_id", "7f3a91c2e8")
	wantString(t, a, "observer.evidence.0", "Paris is the capital of France.")
	wantString(t, a, "observer.evidence.1", "The Seine flows through Paris.")
	wantString(t, a, "eval.decision", "keep")
	wantFloat(t, a, "eval.p_keep", 0.95)
	wantFloat(t, a, "eval.p_keep_raw", 0.93)
	wantString(t, a, "eval.reason", "kev")
	wantString(t, a, "eval.severity", "high")
	wantString(t, a, "eval.model", "kev-latest")
	wantFloat(t, a, "eval.latency_ms", 93)
	if _, ok := a["observer.evidence.2"]; ok {
		t.Error("unexpected observer.evidence.2")
	}

	wantTools := []struct{ name, input, output string }{
		{"search", `{"query":"capital of France"}`, "ok"},
		{"lookup", `"Seine"`, "error"},
		{"noop", "", "ok"},
	}
	for i, want := range wantTools {
		span := spans[i]
		if span.Name() != want.name {
			t.Errorf("tool span %d name = %q, want %q", i, span.Name(), want.name)
		}
		if span.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("tool span %q parent = %s, want root %s", want.name, span.Parent().SpanID(), root.SpanContext().SpanID())
		}
		if span.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Errorf("tool span %q is in a different trace from the root", want.name)
		}
		ta := attrMap(span.Attributes())
		wantString(t, ta, "openinference.span.kind", "TOOL")
		wantString(t, ta, "tool.name", want.name)
		wantString(t, ta, "output.value", want.output)
		if want.input == "" {
			if _, ok := ta["input.value"]; ok {
				t.Errorf("tool span %q has input.value but the call had no args", want.name)
			}
		} else {
			wantString(t, ta, "input.value", want.input)
		}
	}
}

func TestExportFallbackKeepOmitsProbabilities(t *testing.T) {
	exp, rec := newRecorded(t)
	tr := trace.Trace{TraceID: "abc", Question: "q", FinalAnswer: "a"}
	_ = exp.Send(context.Background(), pipeline.Record{
		Trace:    tr,
		Decision: pipeline.Decision{Keep: true, Reason: pipeline.ReasonFallback, Severity: pipeline.SeverityNormal},
		Model:    "kev-latest",
		Latency:  time.Duration(12.5 * float64(time.Millisecond)),
	})

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want just the root", len(spans))
	}
	a := attrMap(spans[0].Attributes())
	wantString(t, a, "eval.reason", "fallback")
	wantString(t, a, "eval.severity", "normal")
	for _, key := range []string{"eval.p_keep", "eval.p_keep_raw", "observer.evidence.0"} {
		if _, ok := a[key]; ok {
			t.Errorf("attribute %s present, want it omitted", key)
		}
	}
}

func TestEachKeptTraceIsItsOwnTrace(t *testing.T) {
	exp, rec := newRecorded(t)
	keep := pipeline.Decision{Keep: true, Scored: true, PKeep: 0.9, PKeepRaw: 0.9, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}
	tr := trace.Trace{TraceID: "a", Question: "q", FinalAnswer: "a"}
	_ = exp.Send(context.Background(), pipeline.Record{Trace: tr, Decision: keep})
	_ = exp.Send(context.Background(), pipeline.Record{Trace: tr, Decision: keep})

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	if spans[0].SpanContext().TraceID() == spans[1].SpanContext().TraceID() {
		t.Error("two exports share a trace ID")
	}
}

// With the back end unreachable the export must not panic or block the
// caller, and the failure must be reported through the OTel error handler.
func TestBackendDownIsReportedNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/v1/traces"
	srv.Close()

	reported := &recordingReporter{}
	prev := otel.GetErrorHandler()
	t.Cleanup(func() { otel.SetErrorHandler(prev) })
	ReportErrors(reported)

	exp, err := Open(context.Background(), export.BackendConfig{Name: "down", Endpoint: url})
	if err != nil {
		t.Fatalf("NewBackend with the back end down: %v", err)
	}
	keep := pipeline.Decision{Keep: true, Scored: true, PKeep: 0.9, PKeepRaw: 0.9, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}
	if err := exp.Send(context.Background(), pipeline.Record{Trace: sampleTrace(), Decision: keep, Model: "kev-latest"}); err != nil {
		t.Errorf("Send of a rebuilt trace = %v, want it batched", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = exp.Shutdown(ctx) // an error here is fine; a hang or panic is not
	if len(reported.all()) == 0 {
		t.Error("export failure was not reported")
	}
}

// The real exporter delivers OTLP to the configured URL path, with its own
// headers and the configured resource attributes (Phoenix picks the project
// from openinference.project.name). The global OTEL_EXPORTER_OTLP_HEADERS
// is ignored, so one back end's credentials never reach another.
func TestBackendReceivesOTLP(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=leaked,x-global=1")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-global-traces=1")
	var path, contentType string
	var header http.Header
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, contentType, header = r.URL.Path, r.Header.Get("Content-Type"), r.Header.Clone()
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer srv.Close()

	exp, err := Open(context.Background(), export.BackendConfig{
		Name:               "phoenix",
		Endpoint:           srv.URL + "/v1/traces",
		Headers:            map[string]string{"Authorization": "Basic abc"},
		ResourceAttributes: map[string]string{"openinference.project.name": "my-project"},
	})
	if err != nil {
		t.Fatal(err)
	}
	keep := pipeline.Decision{Keep: true, Scored: true, PKeep: 0.9, PKeepRaw: 0.9, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}
	if err := exp.Send(context.Background(), pipeline.Record{Trace: sampleTrace(), Decision: keep, Model: "kev-latest"}); err != nil {
		t.Fatal(err)
	}
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if path != "/v1/traces" || contentType != "application/x-protobuf" {
		t.Errorf("request = %s (%s), want /v1/traces (application/x-protobuf)", path, contentType)
	}
	if got := header.Get("Authorization"); got != "Basic abc" {
		t.Errorf("Authorization = %q, want the back end's own", got)
	}
	if header.Get("x-global") != "" || header.Get("x-global-traces") != "" {
		t.Error("headers from OTEL_EXPORTER_OTLP_*HEADERS were sent")
	}
	// Protobuf stores strings as plain bytes, so the attribute names and
	// values can be checked without decoding the message.
	for _, want := range []string{"observer-engine", "openinference.project.name", "my-project", "agent_run", "eval.p_keep", "tool.name"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("OTLP payload does not contain %q", want)
		}
	}
}

// recordingReporter keeps every reported error, for assertions.
type recordingReporter struct {
	mu   sync.Mutex
	errs []error
}

func (r *recordingReporter) Report(_ context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *recordingReporter) all() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}
