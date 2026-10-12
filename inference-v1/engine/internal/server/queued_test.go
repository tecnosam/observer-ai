package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/queue/queuetest"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/trace"
)

func TestProcessQueued(t *testing.T) {
	tr := trace.Trace{TraceID: "t1", Question: "q", FinalAnswer: "a"}
	failWith := func(err error) *fakeScorer {
		return &fakeScorer{score: func(context.Context, string) (scorer.Result, error) {
			return scorer.Result{Attempts: 4}, err
		}}
	}

	t.Run("scored", func(t *testing.T) {
		h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
		res, err := h.engine.ProcessQueued(context.Background(), tr)
		if err != nil {
			t.Fatal(err)
		}
		if res.Decision != "keep" || res.Reason != "kev" || !res.Exported {
			t.Errorf("result = %+v", res)
		}
	})

	// System errors come back as errors: no decision, no export, no
	// "decision" log line. The queue decides what happens next.
	systemErrors := map[string]error{
		"503":                &scorer.StatusError{Code: 503, Body: "model loading"},
		"connection refused": errors.New("dial tcp 127.0.0.1:8010: connect: connection refused"),
		"timeout":            context.DeadlineExceeded,
		"404":                &scorer.StatusError{Code: 404},
	}
	for name, scoreErr := range systemErrors {
		t.Run("system error "+name, func(t *testing.T) {
			var logs strings.Builder
			h := newHarness(t, harnessOpts{
				scorer: failWith(scoreErr),
				rand:   func() float64 { return 0 }, // a fallback would keep
				logger: jsonLogger(&logs),
			})
			res, err := h.engine.ProcessQueued(context.Background(), tr)
			if !errors.Is(err, scoreErr) {
				t.Fatalf("error = %v, want the scorer's error", err)
			}
			if res != (Result{}) {
				t.Errorf("result = %+v, want none", res)
			}
			if n := len(h.recorder.Started()); n != 0 {
				t.Errorf("%d spans exported for an undecided trace", n)
			}
			if strings.Contains(logs.String(), `"msg":"decision"`) {
				t.Errorf("a decision was logged: %s", logs.String())
			}
		})
	}

	// Kev rejecting the trace itself is not a system error. Retrying
	// cannot help, so the trace gets the fallback decision as before.
	t.Run("rejected by kev", func(t *testing.T) {
		h := newHarness(t, harnessOpts{
			scorer: failWith(&scorer.StatusError{Code: 422, Body: "state too long"}),
			rand:   func() float64 { return 0 },
		})
		res, err := h.engine.ProcessQueued(context.Background(), tr)
		if err != nil {
			t.Fatalf("error = %v, want a fallback decision", err)
		}
		if res.Decision != "keep" || res.Reason != "fallback" || res.PKeep != nil || !res.Exported {
			t.Errorf("result = %+v", res)
		}
	})

	// The sync path still always decides, system error or not.
	t.Run("sync path falls back on system errors", func(t *testing.T) {
		h := newHarness(t, harnessOpts{scorer: failWith(systemErrors["503"])})
		if res := h.engine.Process(context.Background(), tr); res.Decision != "drop" || res.Reason != "fallback" {
			t.Errorf("result = %+v", res)
		}
	})
}

// pipeline wires the real pieces together the way main does: HTTP handler,
// JetStream queue on an in-process NATS server, worker pool, Kev client
// against a fake Kev, and the exporter into a span recorder.
type pipeline struct {
	*harness
	js  jetstream.JetStream
	cfg queue.Config
}

func newPipeline(t *testing.T, kev *fakeKev, maxDeliver int) *pipeline {
	t.Helper()
	srv := queuetest.StartServer(t)
	cfg := queue.Config{
		URL:               srv.ClientURL(),
		ConnectTimeout:    5 * time.Second,
		Stream:            "TRACES",
		Subject:           "traces.in",
		Consumer:          "workers",
		MaxMessages:       100,
		AckWait:           30 * time.Second,
		MaxDeliver:        maxDeliver,
		RedeliveryBackoff: []time.Duration{20 * time.Millisecond},
		DLQStream:         "TRACES_DLQ",
		DLQSubject:        "traces.dlq",
		DLQMaxAge:         time.Hour,
		PollWait:          100 * time.Millisecond,
		Logger:            quiet,
	}
	q, err := queue.Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)

	h := newHarness(t, harnessOpts{scorer: kev.client(t, 1), queue: q, temp: 0.86})
	workers := q.StartWorkers(2, func(ctx context.Context, tr trace.Trace) error {
		_, err := h.engine.ProcessQueued(ctx, tr)
		return err
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		workers.Stop(ctx)
	})

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return &pipeline{harness: h, js: js, cfg: cfg}
}

func (p *pipeline) messages(t *testing.T, stream string) uint64 {
	t.Helper()
	s, err := p.js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestQueuedPipelineKeepAndDrop(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, state string) {
		if strings.Contains(state, "Lyon") {
			w.Write(readFile(t, "kev", "success.json")) // 0.93: keep
			return
		}
		w.Write(readFile(t, "kev", "success_low.json")) // 0.12: drop
	})
	p := newPipeline(t, kev, 3)

	for _, name := range []string{"wrong_river.json", "correct_river.json"} {
		if w := p.do("POST", "/v1/traces", string(readFile(t, "traces", name))); w.Code != http.StatusAccepted {
			t.Fatalf("%s: status = %d, want 202; body %s", name, w.Code, w.Body)
		}
	}

	waitUntil(t, "both traces to be scored and acknowledged", func() bool {
		return kev.calls.Load() == 2 && p.messages(t, p.cfg.Stream) == 0
	})
	// Only the kept trace is exported: its root span and two tool spans.
	waitUntil(t, "the kept trace to be exported", func() bool { return len(p.recorder.Ended()) == 3 })
	a := rootAttrs(t, p.recorder)
	if a["observer.source_trace_id"].AsString() != "wrong-river-001" || a["eval.reason"].AsString() != "kev" {
		t.Errorf("exported root span = %v", a)
	}
	if a["eval.p_keep"].AsFloat64() != scorer.Calibrate(0.93, 0.86) {
		t.Errorf("eval.p_keep = %v", a["eval.p_keep"].Emit())
	}

	// The worker sent Kev the exact slice Python would have built.
	want := string(readFile(t, "golden", "wrong_river.txt"))
	kev.mu.Lock()
	defer kev.mu.Unlock()
	found := false
	for _, s := range kev.states {
		found = found || s == want
	}
	if !found {
		t.Errorf("no state sent to Kev matches the golden slice; got %q", kev.states)
	}
	if n := p.messages(t, p.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0", n)
	}
}

// Kev answering 503 for good: the trace is redelivered max_deliver times,
// then parked in the dead-letter stream. No decision is invented for it.
func TestQueuedPipelineDeadLettersWhenKevStaysDown(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		http.Error(w, "model not available", http.StatusServiceUnavailable)
	})
	p := newPipeline(t, kev, 3)

	body := string(readFile(t, "traces", "wrong_river.json"))
	if w := p.do("POST", "/v1/traces", body); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}

	waitUntil(t, "the dead letter", func() bool { return p.messages(t, p.cfg.DLQStream) == 1 })
	waitUntil(t, "the work queue to empty", func() bool { return p.messages(t, p.cfg.Stream) == 0 })

	// 3 deliveries, each 1 attempt + 1 in-process retry.
	if got := kev.calls.Load(); got != 6 {
		t.Errorf("Kev called %d times, want 6", got)
	}
	if n := len(p.recorder.Started()); n != 0 {
		t.Errorf("%d spans exported for a trace that was never scored", n)
	}

	s, err := p.js.Stream(context.Background(), p.cfg.DLQStream)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := s.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := dead.Header.Get(queue.HeaderTraceID); got != "wrong-river-001" {
		t.Errorf("dead letter trace ID = %q", got)
	}
	if got := dead.Header.Get(queue.HeaderDeadReason); got != queue.ReasonSystemError {
		t.Errorf("dead letter reason = %q", got)
	}
	if got := dead.Header.Get(queue.HeaderDeadError); !strings.Contains(got, "HTTP 503") || !strings.Contains(got, "model not available") {
		t.Errorf("dead letter error = %q, want Kev's 503", got)
	}
	// Replaying the dead letter must rebuild the same slice.
	replay, err := trace.Decode(strings.NewReader(string(dead.Data)))
	if err != nil {
		t.Fatalf("dead letter body: %v", err)
	}
	if got, want := trace.BuildState(replay), string(readFile(t, "golden", "wrong_river.txt")); got != want {
		t.Errorf("replayed dead letter builds a different slice:\n%q\nwant\n%q", got, want)
	}
}

// Kev recovering between deliveries: the trace is scored normally and never
// reaches the dead-letter stream.
func TestQueuedPipelineRecoversWhenKevComesBack(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	var failures atomic.Int32
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		if down.Load() {
			if failures.Add(1) == 2 { // the whole first delivery has failed
				down.Store(false)
			}
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Write(readFile(t, "kev", "success.json"))
	})
	p := newPipeline(t, kev, 3)

	if w := p.do("POST", "/v1/traces", `{"trace_id": "t1", "question": "q", "final_answer": "a"}`); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	waitUntil(t, "the trace to be exported", func() bool { return len(p.recorder.Ended()) == 1 })
	waitUntil(t, "the work queue to empty", func() bool { return p.messages(t, p.cfg.Stream) == 0 })

	a := rootAttrs(t, p.recorder)
	if a["eval.reason"].AsString() != "kev" || a["observer.source_trace_id"].AsString() != "t1" {
		t.Errorf("exported root span = %v", a)
	}
	if n := p.messages(t, p.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0", n)
	}
}

// A 4xx from Kev is the trace's problem, not the system's: it gets a
// fallback decision on the first delivery and is not dead-lettered.
func TestQueuedPipelineFallsBackOnKevRejection(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write(readFile(t, "kev", "error_422.json"))
	})
	p := newPipeline(t, kev, 3)

	if w := p.do("POST", "/v1/traces", `{"trace_id": "t1", "question": "q", "final_answer": "a"}`); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	waitUntil(t, "the trace to be acknowledged", func() bool {
		return kev.calls.Load() >= 1 && p.messages(t, p.cfg.Stream) == 0
	})
	if got := kev.calls.Load(); got != 1 {
		t.Errorf("Kev called %d times, want 1: a 4xx is neither retried nor redelivered", got)
	}
	if n := p.messages(t, p.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0", n)
	}
}

// The payload on the wire is plain trace JSON, so other producers can
// publish to the subject directly.
func TestQueuePayloadIsTraceJSON(t *testing.T) {
	srv := queuetest.StartServer(t)
	cfg := queue.Config{
		URL: srv.ClientURL(), ConnectTimeout: 5 * time.Second,
		Stream: "TRACES", Subject: "traces.in", Consumer: "workers", MaxMessages: 10,
		AckWait: 30 * time.Second, MaxDeliver: 3,
		DLQStream: "TRACES_DLQ", DLQSubject: "traces.dlq", DLQMaxAge: time.Hour, Logger: quiet,
	}
	q, err := queue.Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}, queue: q})
	if w := h.do("POST", "/v1/traces", string(readFile(t, "traces", "correct_river.json"))); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d", w.Code)
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	s, err := js.Stream(context.Background(), "TRACES")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := s.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if payload["trace_id"] != "7f3a91c2e8" || payload["final_answer"] != "The Seine" {
		t.Errorf("payload = %s", msg.Data)
	}
	if got := msg.Header.Get(queue.HeaderTraceID); got != "7f3a91c2e8" {
		t.Errorf("header %s = %q", queue.HeaderTraceID, got)
	}
}
