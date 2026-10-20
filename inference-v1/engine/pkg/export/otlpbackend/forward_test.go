package otlpbackend

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/export"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
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

var keepHigh = pipeline.Decision{Keep: true, Scored: true, PKeep: 0.95, PKeepRaw: 0.93, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}

func TestForwardKeptOTLPTrace(t *testing.T) {
	up := &fakeUploader{}
	exp, rec := newRecorded(t)
	WithForwarder(up, map[string]string{"openinference.project.name": "observer-ai"})(exp)

	if err := exp.Send(context.Background(), pipeline.Record{Trace: otlpTrace(t), Decision: keepHigh, Model: "kev-latest", Latency: 93 * time.Millisecond}); err != nil {
		t.Fatal(err)
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

func TestForwardOmitsProbabilitiesOnFallback(t *testing.T) {
	up := &fakeUploader{}
	exp, _ := newRecorded(t)
	WithForwarder(up, map[string]string{"openinference.project.name": "observer-ai"})(exp)

	fallback := pipeline.Decision{Keep: true, Reason: pipeline.ReasonFallback, Severity: pipeline.SeverityNormal}
	if err := exp.Send(context.Background(), pipeline.Record{Trace: otlpTrace(t), Decision: fallback, Model: "kev-latest"}); err != nil {
		t.Fatal(err)
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

func TestForwardFailureIsReturned(t *testing.T) {
	up := &fakeUploader{err: errors.New("backend unreachable")}
	exp, _ := newRecorded(t)
	WithForwarder(up, nil)(exp)

	err := exp.Send(context.Background(), pipeline.Record{Trace: otlpTrace(t), Decision: keepHigh})
	if fault.KindOf(err) != fault.KindUnavailable || !strings.Contains(err.Error(), "backend unreachable") {
		t.Errorf("error = %v, want an unavailable fault", err)
	}

	// Corrupt stored spans fail rather than panicking.
	bad := otlpTrace(t)
	bad.OTLP = []byte{0xff, 0xff}
	up.err = nil
	if err := exp.Send(context.Background(), pipeline.Record{Trace: bad, Decision: keepHigh}); fault.KindOf(err) != fault.KindInvalid {
		t.Errorf("error = %v for corrupt stored spans, want an invalid fault", err)
	}
}

// Without a forwarder (or without original spans) the trace is rebuilt.
func TestOTLPTraceIsRebuiltWithoutForwarder(t *testing.T) {
	exp, rec := newRecorded(t)
	if err := exp.Send(context.Background(), pipeline.Record{Trace: otlpTrace(t), Decision: keepHigh}); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Ended()); n != 4 {
		t.Errorf("%d spans rebuilt, want 4", n)
	}
}

// The real exporter posts forwarded spans to the back end as OTLP.
func TestBackendReceivesForwardedSpans(t *testing.T) {
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

	exp, err := Open(context.Background(), export.BackendConfig{Name: "phoenix", Endpoint: srv.URL + "/v1/traces"})
	if err != nil {
		t.Fatal(err)
	}
	defer exp.Shutdown(context.Background())

	if err := exp.Send(context.Background(), pipeline.Record{Trace: otlpTrace(t), Decision: keepHigh, Model: "kev-latest"}); err != nil {
		t.Fatal(err)
	}
	// Forwarding is synchronous: the back end has the spans before Send returns.
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
		t.Errorf("the back end received %d spans, want 4", n)
	}
}
