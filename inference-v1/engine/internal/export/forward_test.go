package export

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/trace"
)

var forwardTraceID = []byte{0xab, 0xcd, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

// otlpTrace is sampleTrace as it would arrive through the OTLP receivers.
func otlpTrace(t *testing.T) trace.Trace {
	t.Helper()
	spans := otlp.SpansFromTrace(forwardTraceID, sampleTrace())
	byTrace, _, err := otlp.Split([]*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	id := hex.EncodeToString(forwardTraceID)
	tr, err := otlp.Assemble(id, [][]byte{byTrace[id]})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

type fakeUploader struct {
	uploads [][]*tracepb.ResourceSpans
	err     error
}

func (u *fakeUploader) UploadTraces(_ context.Context, spans []*tracepb.ResourceSpans) error {
	if u.err != nil {
		return u.err
	}
	u.uploads = append(u.uploads, spans)
	return nil
}

func attrsOf(kvs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := map[string]*commonpb.AnyValue{}
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

var keepHigh = policy.Decision{Keep: true, Scored: true, PKeep: 0.95, PKeepRaw: 0.93, Reason: policy.ReasonKev, Severity: policy.SeverityHigh}

func TestForwardKeptOTLPTrace(t *testing.T) {
	up := &fakeUploader{}
	exp, rec := newRecorded(t)
	WithForwarder(up, "observer-ai")(exp)

	ok := exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: keepHigh, Model: "kev-latest", LatencyMS: 93})
	if !ok {
		t.Fatal("Export returned false")
	}
	// The original spans are forwarded; nothing is rebuilt.
	if n := len(rec.Started()); n != 0 {
		t.Errorf("%d spans rebuilt through the SDK, want 0", n)
	}
	if len(up.uploads) != 1 {
		t.Fatalf("%d uploads, want 1", len(up.uploads))
	}

	var root *tracepb.Span
	var spans []*tracepb.Span
	for _, rs := range up.uploads[0] {
		if got := attrsOf(rs.Resource.Attributes)["openinference.project.name"].GetStringValue(); got != "observer-ai" {
			t.Errorf("project = %q, want observer-ai", got)
		}
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				spans = append(spans, s)
				if len(s.ParentSpanId) == 0 {
					root = s
				}
			}
		}
	}
	if len(spans) != 4 {
		t.Fatalf("forwarded %d spans, want the root and 3 tool spans", len(spans))
	}
	for _, s := range spans {
		if string(s.TraceId) != string(forwardTraceID) {
			t.Errorf("span %q trace ID = %x, want the original %x", s.Name, s.TraceId, forwardTraceID)
		}
	}

	a := attrsOf(root.Attributes)
	wantStrings := map[string]string{
		"eval.decision": "keep", "eval.reason": "kev", "eval.severity": "high", "eval.model": "kev-latest",
		"input.value": "Which river flows through the capital of France?", "output.value": "The Seine",
	}
	for k, want := range wantStrings {
		if got := a[k].GetStringValue(); got != want {
			t.Errorf("root attribute %s = %q, want %q", k, got, want)
		}
	}
	wantFloats := map[string]float64{"eval.p_keep": 0.95, "eval.p_keep_raw": 0.93, "eval.latency_ms": 93}
	for k, want := range wantFloats {
		if got := a[k].GetDoubleValue(); got != want {
			t.Errorf("root attribute %s = %v, want %v", k, got, want)
		}
	}
	// Tool spans are untouched.
	for _, s := range spans {
		if s != root {
			if _, ok := attrsOf(s.Attributes)["eval.decision"]; ok {
				t.Errorf("tool span %q got eval attributes", s.Name)
			}
		}
	}
}

func TestForwardSkipsDroppedAndOmitsProbabilitiesOnFallback(t *testing.T) {
	up := &fakeUploader{}
	exp, _ := newRecorded(t)
	WithForwarder(up, "observer-ai")(exp)

	drop := policy.Decision{Scored: true, PKeep: 0.1, PKeepRaw: 0.1, Reason: policy.ReasonBelowThreshold, Severity: policy.SeverityNone}
	if exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: drop}) {
		t.Error("Export returned true for a dropped trace")
	}
	if len(up.uploads) != 0 {
		t.Fatal("a dropped trace was forwarded")
	}

	fallback := policy.Decision{Keep: true, Reason: policy.ReasonFallback, Severity: policy.SeverityNormal}
	if !exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: fallback, Model: "kev-latest"}) {
		t.Fatal("Export returned false for a fallback keep")
	}
	for _, rs := range up.uploads[0] {
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				a := attrsOf(s.Attributes)
				if _, ok := a["eval.p_keep"]; ok {
					t.Errorf("span %q has eval.p_keep on a fallback keep", s.Name)
				}
			}
		}
	}
}

func TestForwardFailureIsReportedAndLogged(t *testing.T) {
	var logs strings.Builder
	up := &fakeUploader{err: errors.New("phoenix unreachable")}
	exp, _ := newRecorded(t)
	WithForwarder(up, "observer-ai")(exp)
	WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))(exp)

	if exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: keepHigh}) {
		t.Error("Export returned true although the upload failed")
	}
	if !strings.Contains(logs.String(), "trace export failed") || !strings.Contains(logs.String(), "phoenix unreachable") {
		t.Errorf("failure not logged: %s", logs.String())
	}

	// Corrupt stored spans fail the same way rather than panicking.
	bad := otlpTrace(t)
	bad.OTLP = []byte{0xff, 0xff}
	up.err = nil
	if exp.Export(context.Background(), Record{Trace: bad, Decision: keepHigh}) {
		t.Error("Export returned true for corrupt stored spans")
	}
}

// Without a forwarder (or without original spans) the trace is rebuilt.
func TestOTLPTraceIsRebuiltWithoutForwarder(t *testing.T) {
	exp, rec := newRecorded(t)
	if !exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: keepHigh}) {
		t.Fatal("Export returned false")
	}
	if n := len(rec.Ended()); n != 4 {
		t.Errorf("%d spans rebuilt, want 4", n)
	}
}

// The real exporter posts forwarded spans to the Phoenix endpoint as OTLP.
func TestPhoenixReceivesForwardedSpans(t *testing.T) {
	var got coltracepb.ExportTraceServiceRequest
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		if err := proto.Unmarshal(body, &got); err != nil {
			t.Errorf("payload is not OTLP: %v", err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer srv.Close()

	exp, err := NewPhoenix(context.Background(), srv.URL+"/v1/traces", "observer-ai", slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer exp.Shutdown(context.Background())

	if !exp.Export(context.Background(), Record{Trace: otlpTrace(t), Decision: keepHigh, Model: "kev-latest"}) {
		t.Fatal("Export returned false")
	}
	// Forwarding is synchronous: Phoenix has the spans before Export returns.
	if path != "/v1/traces" {
		t.Errorf("path = %q", path)
	}
	n := 0
	for _, rs := range got.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			n += len(ss.Spans)
		}
	}
	if n != 4 {
		t.Errorf("Phoenix received %d spans, want 4", n)
	}
}
