package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/observer-ai/engine/internal/export"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	testdata      = "../../testdata"
	testQuestions = `{"answer_wrong":{"type":"noul","instructions":"Is the final answer wrong or only partially correct for the user's question?"}}`
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func jsonLogger(w io.Writer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }

func readFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(append([]string{testdata}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeScorer is an in-process Scorer for handler tests.
type fakeScorer struct {
	score func(ctx context.Context, state string) (scorer.Result, error)
	ready error
}

func (f *fakeScorer) Score(ctx context.Context, state string) (scorer.Result, error) {
	return f.score(ctx, state)
}

func (f *fakeScorer) Ready(context.Context) error { return f.ready }

func fixedScore(p float64) func(context.Context, string) (scorer.Result, error) {
	return func(context.Context, string) (scorer.Result, error) {
		return scorer.Result{PRaw: p, Model: "kev-latest", LatencyMS: 12, Attempts: 1}, nil
	}
}

// fakeQueue is an in-memory Publisher that records what it was given.
type fakeQueue struct {
	mu         sync.Mutex
	published  []trace.Trace
	publishErr error
	readyErr   error
}

func (q *fakeQueue) Publish(_ context.Context, t trace.Trace) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.publishErr != nil {
		return q.publishErr
	}
	q.published = append(q.published, t)
	return nil
}

func (q *fakeQueue) Ready() error { return q.readyErr }

type harness struct {
	srv      *Server
	engine   *Engine
	queue    *fakeQueue
	recorder *tracetest.SpanRecorder
}

type harnessOpts struct {
	scorer  Scorer
	queue   Publisher // defaults to a fakeQueue
	rand    func() float64
	temp    float64
	maxBody int64
	logger  *slog.Logger
	// uploader, when set, receives traces that carry their original spans.
	uploader export.Uploader
}

func newHarness(t *testing.T, o harnessOpts) *harness {
	t.Helper()
	if o.rand == nil {
		o.rand = func() float64 { return 0.99 } // never take the random floor
	}
	if o.temp == 0 {
		o.temp = 1
	}
	if o.logger == nil {
		o.logger = quiet
	}
	fq := &fakeQueue{}
	if o.queue == nil {
		o.queue = fq
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	engine := &Engine{
		Scorer:      o.scorer,
		Policy:      policy.New(policy.Config{KeepThreshold: 0.5, RandomFloor: 0.02, SeverityHigh: 0.8}, o.rand),
		Exporter:    newExporter(tp, o.uploader),
		Temperature: o.temp,
		Model:       "kev-latest",
		Logger:      o.logger,
	}
	srv := New(Options{Engine: engine, Queue: o.queue, MaxBodyBytes: o.maxBody, Logger: quiet})
	return &harness{srv: srv, engine: engine, queue: fq, recorder: rec}
}

func newExporter(tp *sdktrace.TracerProvider, u export.Uploader) *export.Exporter {
	if u == nil {
		return export.New(tp)
	}
	return export.New(tp, export.WithForwarder(u, "observer-ai"), export.WithLogger(quiet))
}

func (h *harness) do(method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func decodeBody[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("response %q is not JSON: %v", w.Body.String(), err)
	}
	return v
}

func rootAttrs(t *testing.T, rec *tracetest.SpanRecorder) map[string]attribute.Value {
	t.Helper()
	for _, s := range rec.Ended() {
		if s.Name() == "agent_run" {
			m := map[string]attribute.Value{}
			for _, kv := range s.Attributes() {
				m[string(kv.Key)] = kv.Value
			}
			return m
		}
	}
	t.Fatal("no agent_run span recorded")
	return nil
}

func TestEnqueuePublishesToQueue(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})

	body := string(readFile(t, "traces", "correct_river.json"))
	w := h.do("POST", "/v1/traces", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", w.Code, w.Body)
	}
	if got := decodeBody[map[string]string](t, w); got["trace_id"] != "7f3a91c2e8" {
		t.Errorf("trace_id = %q, want 7f3a91c2e8", got["trace_id"])
	}

	// No trace_id: the server generates 32 hex chars and queues the trace
	// under that ID.
	w = h.do("POST", "/v1/traces", `{"question": "q2", "final_answer": "a2"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	generated := decodeBody[map[string]string](t, w)["trace_id"]
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(generated) {
		t.Errorf("generated trace_id = %q, want 32 hex chars", generated)
	}

	if len(h.queue.published) != 2 {
		t.Fatalf("%d traces published, want 2", len(h.queue.published))
	}
	first, second := h.queue.published[0], h.queue.published[1]
	if first.TraceID != "7f3a91c2e8" || first.FinalAnswer != "The Seine" || len(first.ToolCalls) != 1 || len(first.Evidence) != 2 {
		t.Errorf("published trace = %+v", first)
	}
	if second.TraceID != generated || second.Question != "q2" {
		t.Errorf("published trace = %+v, want the generated ID", second)
	}
	// Scoring happens in the workers, not in the request.
	if n := len(h.recorder.Started()); n != 0 {
		t.Errorf("%d spans started by the enqueue handler, want 0", n)
	}
}

func TestBadRequests(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	tests := []struct{ name, body, wantErr string }{
		{"not JSON", `{"question": `, "invalid JSON"},
		{"empty body", ``, "invalid JSON"},
		{"missing question", `{"final_answer": "a"}`, "question is required"},
		{"missing final_answer", `{"question": "q"}`, "final_answer is required"},
		{"wrong type", `{"question": ["q"], "final_answer": "a"}`, "invalid JSON"},
		{"tool call without name", `{"question": "q", "final_answer": "a", "tool_calls": [{}]}`, "tool_calls[0].name is required"},
	}
	for _, path := range []string{"/v1/traces", "/v1/traces:score"} {
		for _, tt := range tests {
			t.Run(path+"/"+tt.name, func(t *testing.T) {
				w := h.do("POST", path, tt.body)
				if w.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body %s", w.Code, w.Body)
				}
				if got := decodeBody[map[string]string](t, w)["error"]; !strings.Contains(got, tt.wantErr) {
					t.Errorf("error = %q, want it to contain %q", got, tt.wantErr)
				}
			})
		}
	}
	if n := len(h.recorder.Started()); n != 0 {
		t.Errorf("%d spans started for rejected requests", n)
	}
	if n := len(h.queue.published); n != 0 {
		t.Errorf("%d rejected requests reached the queue", n)
	}
}

func TestBodyTooLarge(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}, maxBody: 64})
	body := `{"question": "` + strings.Repeat("x", 200) + `", "final_answer": "a"}`
	if w := h.do("POST", "/v1/traces", body); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", w.Code)
	}
}

func TestMethodAndRouteMatching(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	if w := h.do("GET", "/v1/traces", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/traces = %d, want 405", w.Code)
	}
	if w := h.do("POST", "/v1/nope", "{}"); w.Code != http.StatusNotFound {
		t.Errorf("POST /v1/nope = %d, want 404", w.Code)
	}
}

func TestEnqueueWhenQueueRefuses(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr string
	}{
		{"full", queue.ErrFull, "queue full"},
		{"wrapped full", fmt.Errorf("publish: %w", queue.ErrFull), "queue full"},
		{"unavailable", fmt.Errorf("%w: nats: timeout", queue.ErrUnavailable), "queue unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
			h.queue.publishErr = tt.err

			w := h.do("POST", "/v1/traces", `{"question": "q", "final_answer": "a"}`)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", w.Code)
			}
			if got := w.Header().Get("Retry-After"); got != "1" {
				t.Errorf("Retry-After = %q, want \"1\"", got)
			}
			if got := decodeBody[map[string]string](t, w)["error"]; !strings.Contains(got, tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", got, tt.wantErr)
			}
			// NATS internals stay in the log, not in the response.
			if strings.Contains(w.Body.String(), "nats:") {
				t.Errorf("response leaks the queue error: %s", w.Body)
			}
		})
	}
}

// The sync path does not touch the queue, so it keeps working without it.
func TestScoreWorksWhenQueueIsDown(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	h.queue.publishErr = queue.ErrUnavailable
	h.queue.readyErr = errors.New("nats is reconnecting")

	w := h.do("POST", "/v1/traces:score", `{"question": "q", "final_answer": "a"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := decodeBody[Result](t, w); got.Decision != "keep" {
		t.Errorf("result = %+v", got)
	}
}

func TestHealthz(t *testing.T) {
	// Up even when Kev is down.
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{ready: errors.New("down")}})
	w := h.do("GET", "/healthz", "")
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := decodeBody[map[string]string](t, w)["status"]; got != "ok" {
		t.Errorf("status field = %q", got)
	}
}

func TestReadyz(t *testing.T) {
	fs := &fakeScorer{}
	h := newHarness(t, harnessOpts{scorer: fs})
	if w := h.do("GET", "/readyz", ""); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when Kev answers", w.Code)
	}

	fs.ready = errors.New("connection refused")
	w := h.do("GET", "/readyz", "")
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when Kev is down", w.Code)
	}
	if got := decodeBody[map[string]string](t, w)["error"]; !strings.Contains(got, "kev is not reachable: connection refused") {
		t.Errorf("error = %q", got)
	}

	// Kev back, queue gone.
	fs.ready = nil
	h.queue.readyErr = errors.New("nats is reconnecting")
	w = h.do("GET", "/readyz", "")
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when the queue is down", w.Code)
	}
	if got := decodeBody[map[string]string](t, w)["error"]; !strings.Contains(got, "queue is not reachable: nats is reconnecting") {
		t.Errorf("error = %q", got)
	}
}

// fakeKev is an httptest Kev that records the states it was asked about.
type fakeKev struct {
	*httptest.Server
	mu     sync.Mutex
	states []string
	calls  atomic.Int32
}

func newFakeKev(t *testing.T, handler func(w http.ResponseWriter, state string)) *fakeKev {
	t.Helper()
	k := &fakeKev{}
	k.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			w.Write([]byte(`{"models":[]}`))
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/systemone" {
			http.NotFound(w, r)
			return
		}
		k.calls.Add(1)
		var req struct {
			Model     string          `json:"model"`
			State     string          `json:"state"`
			Questions json.RawMessage `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.State == "" || len(req.Questions) == 0 {
			http.Error(w, `{"detail":"bad request"}`, http.StatusUnprocessableEntity)
			return
		}
		k.mu.Lock()
		k.states = append(k.states, req.State)
		k.mu.Unlock()
		handler(w, req.State)
	}))
	t.Cleanup(k.Close)
	return k
}

func (k *fakeKev) client(t *testing.T, maxRetries int) *scorer.Client {
	t.Helper()
	c, err := scorer.New(scorer.Config{
		Endpoint:         k.URL + "/v1/systemone",
		Model:            "kev-latest",
		Questions:        json.RawMessage(testQuestions),
		DecisionQuestion: "answer_wrong",
		Timeout:          2 * time.Second,
		MaxRetries:       maxRetries,
		Backoff:          []time.Duration{time.Millisecond},
		Logger:           quiet,
		Sleep:            func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// End to end through the sync path: real handler, real Kev client against a
// fake Kev, real exporter into a span recorder.
func TestScoreEndToEndKeep(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		w.Write(readFile(t, "kev", "success.json")) // noul 0.93, latency_ms 93
	})
	h := newHarness(t, harnessOpts{scorer: kev.client(t, 3), temp: 0.86})

	w := h.do("POST", "/v1/traces:score", string(readFile(t, "traces", "wrong_river.json")))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body)
	}

	// Every documented field is present in the JSON.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"trace_id", "decision", "p_keep", "p_keep_raw", "reason", "severity", "model", "latency_ms", "exported"} {
		if _, ok := fields[f]; !ok {
			t.Errorf("response is missing %q: %s", f, w.Body)
		}
	}

	got := decodeBody[Result](t, w)
	wantP := scorer.Calibrate(0.93, 0.86)
	if got.TraceID != "wrong-river-001" || got.Decision != "keep" || got.Reason != "kev" ||
		got.Severity != "high" || got.Model != "kev-latest" || got.LatencyMS != 93 || !got.Exported {
		t.Errorf("result = %+v", got)
	}
	if got.PKeepRaw == nil || *got.PKeepRaw != 0.93 || got.PKeep == nil || *got.PKeep != wantP {
		t.Errorf("p_keep = %v, p_keep_raw = %v; want %v and 0.93", deref(got.PKeep), deref(got.PKeepRaw), wantP)
	}
	if wantP <= 0.93 {
		t.Fatalf("test setup: T = 0.86 should sharpen 0.93, got %v", wantP)
	}

	// Kev was sent exactly the slice the Python training code produces.
	wantState := string(readFile(t, "golden", "wrong_river.txt"))
	if len(kev.states) != 1 || kev.states[0] != wantState {
		t.Errorf("state sent to Kev:\n%q\nwant:\n%q", kev.states, wantState)
	}

	// The exporter recorded the root span and one span per tool call.
	spans := h.recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("%d spans recorded, want 3", len(spans))
	}
	a := rootAttrs(t, h.recorder)
	if a["eval.decision"].AsString() != "keep" || a["eval.reason"].AsString() != "kev" ||
		a["eval.severity"].AsString() != "high" || a["eval.model"].AsString() != "kev-latest" {
		t.Errorf("eval attributes = %v", a)
	}
	if a["eval.p_keep"].AsFloat64() != wantP || a["eval.p_keep_raw"].AsFloat64() != 0.93 || a["eval.latency_ms"].AsFloat64() != 93 {
		t.Errorf("eval numbers: p_keep=%v p_keep_raw=%v latency=%v", a["eval.p_keep"].Emit(), a["eval.p_keep_raw"].Emit(), a["eval.latency_ms"].Emit())
	}
	if a["observer.source_trace_id"].AsString() != "wrong-river-001" {
		t.Errorf("observer.source_trace_id = %v", a["observer.source_trace_id"].Emit())
	}
}

func TestScoreEndToEndDrop(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		w.Write(readFile(t, "kev", "success_low.json")) // noul 0.12
	})
	h := newHarness(t, harnessOpts{scorer: kev.client(t, 3)})

	w := h.do("POST", "/v1/traces:score", string(readFile(t, "traces", "correct_river.json")))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	got := decodeBody[Result](t, w)
	if got.Decision != "drop" || got.Reason != "below_threshold" || got.Severity != "none" || got.Exported {
		t.Errorf("result = %+v", got)
	}
	if got.PKeep == nil || *got.PKeep != 0.12 || got.LatencyMS != 41.5 {
		t.Errorf("p_keep = %v, latency = %v", deref(got.PKeep), got.LatencyMS)
	}
	if n := len(h.recorder.Started()); n != 0 {
		t.Errorf("%d spans started for a dropped trace, want 0", n)
	}
}

func TestScoreEndToEndRandomFloorKeep(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		w.Write(readFile(t, "kev", "success_low.json"))
	})
	h := newHarness(t, harnessOpts{scorer: kev.client(t, 3), rand: func() float64 { return 0.001 }})

	got := decodeBody[Result](t, h.do("POST", "/v1/traces:score", `{"question": "q", "final_answer": "a"}`))
	if got.Decision != "keep" || got.Reason != "random_floor" || got.Severity != "normal" || !got.Exported {
		t.Errorf("result = %+v", got)
	}
}

// Kev failing on every attempt must not fail the request: the decision
// falls back to the random floor with reason "fallback".
func TestScoreEndToEndFallback(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	body := `{"trace_id": "t1", "question": "q", "final_answer": "a"}`

	t.Run("drop", func(t *testing.T) {
		kev.calls.Store(0)
		h := newHarness(t, harnessOpts{scorer: kev.client(t, 2), rand: func() float64 { return 0.5 }})
		w := h.do("POST", "/v1/traces:score", body)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		got := decodeBody[Result](t, w)
		if got.Decision != "drop" || got.Reason != "fallback" || got.Severity != "none" || got.Exported {
			t.Errorf("result = %+v", got)
		}
		if got.PKeep != nil || got.PKeepRaw != nil {
			t.Errorf("p_keep = %v, p_keep_raw = %v; want null for a fallback", deref(got.PKeep), deref(got.PKeepRaw))
		}
		if !strings.Contains(w.Body.String(), `"p_keep":null`) {
			t.Errorf("body = %s, want explicit nulls", w.Body)
		}
		if got.Model != "kev-latest" {
			t.Errorf("model = %q, want the configured model", got.Model)
		}
		if kev.calls.Load() != 3 {
			t.Errorf("Kev called %d times, want 3 (1 + 2 retries)", kev.calls.Load())
		}
		if n := len(h.recorder.Started()); n != 0 {
			t.Errorf("%d spans started for a dropped fallback", n)
		}
	})

	t.Run("keep", func(t *testing.T) {
		h := newHarness(t, harnessOpts{scorer: kev.client(t, 2), rand: func() float64 { return 0.001 }})
		got := decodeBody[Result](t, h.do("POST", "/v1/traces:score", body))
		if got.Decision != "keep" || got.Reason != "fallback" || got.Severity != "normal" || !got.Exported {
			t.Errorf("result = %+v", got)
		}
		a := rootAttrs(t, h.recorder)
		if a["eval.reason"].AsString() != "fallback" {
			t.Errorf("eval.reason = %v", a["eval.reason"].Emit())
		}
		if _, ok := a["eval.p_keep"]; ok {
			t.Error("eval.p_keep set on a fallback keep")
		}
	})
}

func TestReadyzAgainstFakeKev(t *testing.T) {
	kev := newFakeKev(t, func(http.ResponseWriter, string) {})
	h := newHarness(t, harnessOpts{scorer: kev.client(t, 0)})
	if w := h.do("GET", "/readyz", ""); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	kev.Close()
	if w := h.do("GET", "/readyz", ""); w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 with Kev down", w.Code)
	}
}

// failingExporter accepts spans but cannot deliver them.
type failingExporter struct{}

func (failingExporter) Export(_ context.Context, rec export.Record) bool { return rec.Decision.Keep }
func (failingExporter) Flush(context.Context) error                      { return errors.New("phoenix unreachable") }

// Phoenix being down must not fail the request; the response says the trace
// was kept but not exported.
func TestScoreWhenExportFlushFails(t *testing.T) {
	srv := New(Options{
		Engine: &Engine{
			Scorer:      &fakeScorer{score: fixedScore(0.9)},
			Policy:      policy.New(policy.Config{KeepThreshold: 0.5, RandomFloor: 0, SeverityHigh: 0.8}, nil),
			Exporter:    failingExporter{},
			Temperature: 1,
			Model:       "kev-latest",
			Logger:      quiet,
		},
		Queue: &fakeQueue{}, Logger: quiet,
	})

	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/traces:score",
		strings.NewReader(`{"question": "q", "final_answer": "a"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	got := decodeBody[Result](t, w)
	if got.Decision != "keep" || got.Exported {
		t.Errorf("result = %+v, want keep with exported=false", got)
	}
}

// One JSON log line per decision with the fields operators filter on.
func TestDecisionLogLine(t *testing.T) {
	var buf strings.Builder
	h := newHarness(t, harnessOpts{
		scorer: &fakeScorer{score: fixedScore(0.9)},
		logger: jsonLogger(&buf),
	})
	h.do("POST", "/v1/traces:score", `{"trace_id": "log-1", "question": "q", "final_answer": "a"}`)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1:\n%s", len(lines), buf.String())
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	want := map[string]any{"msg": "decision", "trace_id": "log-1", "decision": "keep", "p_keep": 0.9, "reason": "kev", "latency_ms": 12.0}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("log field %s = %v, want %v", k, line[k], v)
		}
	}
}

func deref(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}
