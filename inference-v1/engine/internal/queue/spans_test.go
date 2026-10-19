package queue_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	traceA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	traceB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func withSpans(idle, maxWait time.Duration) func(*queue.Config) {
	return func(c *queue.Config) {
		c.Spans = queue.SpanConfig{
			Stream:         "SPANS",
			Subject:        "spans",
			MaxAge:         time.Hour,
			PendingStream:  "PENDING",
			PendingSubject: "pending",
			IdleTimeout:    idle,
			MaxWait:        maxWait,
		}
	}
}

// joinBatches stands in for the OTLP assembler: the batches are plain text,
// the trace's question is the batches joined in arrival order, and a batch
// saying "unscorable" makes the whole trace unscorable.
func joinBatches(traceID string, batches [][]byte) (trace.Trace, error) {
	var parts []string
	for _, b := range batches {
		if string(b) == "unscorable" {
			return trace.Trace{}, errors.New("trace is not scorable: no question")
		}
		parts = append(parts, string(b))
	}
	return trace.Trace{TraceID: traceID, Question: strings.Join(parts, "+"), FinalAnswer: "a", OTLP: []byte("original spans")}, nil
}

// collector gathers the traces that reach the work queue.
type collector struct {
	mu     sync.Mutex
	traces []trace.Trace
}

func (c *collector) handle(_ context.Context, t trace.Trace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, t)
	return nil
}

func (c *collector) snapshot() []trace.Trace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]trace.Trace(nil), c.traces...)
}

func (f *fixture) startAssemblers(t *testing.T, n int) {
	t.Helper()
	a := f.q.StartAssemblers(n, joinBatches)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitFor)
		defer cancel()
		a.Stop(ctx)
	})
}

func publishSpans(t *testing.T, q *queue.Queue, traceID, batch string) {
	t.Helper()
	if err := q.PublishSpans(context.Background(), map[string][]byte{traceID: []byte(batch)}); err != nil {
		t.Fatalf("PublishSpans: %v", err)
	}
}

func TestSpansAreAssembledAfterIdleTimeout(t *testing.T) {
	const idle = 300 * time.Millisecond
	f := setup(t, withSpans(idle, 10*time.Second))
	got := &collector{}
	f.start(t, 1, got.handle)
	f.startAssemblers(t, 2)

	// Three batches for one trace, each arriving before the trace goes quiet.
	start := time.Now()
	publishSpans(t, f.q, traceA, "children")
	time.Sleep(idle / 3)
	publishSpans(t, f.q, traceA, "more")
	time.Sleep(idle / 3)
	publishSpans(t, f.q, traceA, "root")
	lastSpan := time.Now()

	eventually(t, "the assembled trace", func() bool { return len(got.snapshot()) == 1 })
	tr := got.snapshot()[0]
	if tr.TraceID != traceA || tr.Question != "children+more+root" {
		t.Errorf("assembled trace = %+v, want all three batches in order", tr)
	}
	if string(tr.OTLP) != "original spans" {
		t.Errorf("OTLP payload = %q, want it carried through the queue", tr.OTLP)
	}
	if waited := time.Since(lastSpan); waited < idle-50*time.Millisecond {
		t.Errorf("assembled %v after the last span, want at least the idle timeout %v", waited, idle)
	}
	if total := time.Since(start); total > 5*time.Second {
		t.Errorf("assembly took %v", total)
	}

	// Spans and marker are gone once the trace is queued.
	eventually(t, "spans to be purged", func() bool { return f.messages(t, "SPANS") == 0 })
	eventually(t, "the marker to be acknowledged", func() bool { return f.messages(t, "PENDING") == 0 })
	time.Sleep(2 * idle)
	if n := len(got.snapshot()); n != 1 {
		t.Errorf("%d traces assembled, want exactly 1", n)
	}
}

func TestSpanBatchesOfDifferentTracesStaySeparate(t *testing.T) {
	f := setup(t, withSpans(150*time.Millisecond, 10*time.Second))
	got := &collector{}
	f.start(t, 2, got.handle)
	f.startAssemblers(t, 2)

	// One export carrying spans of two traces.
	err := f.q.PublishSpans(context.Background(), map[string][]byte{traceA: []byte("a1"), traceB: []byte("b1")})
	if err != nil {
		t.Fatal(err)
	}
	publishSpans(t, f.q, traceA, "a2")

	eventually(t, "both traces", func() bool { return len(got.snapshot()) == 2 })
	questions := map[string]string{}
	for _, tr := range got.snapshot() {
		questions[tr.TraceID] = tr.Question
	}
	if questions[traceA] != "a1+a2" || questions[traceB] != "b1" {
		t.Errorf("assembled = %v", questions)
	}
}

// However many batches arrive, a trace has one pending marker.
func TestOneMarkerPerTrace(t *testing.T) {
	f := setup(t, withSpans(time.Hour, time.Hour)) // never assembles during the test
	for range 5 {
		publishSpans(t, f.q, traceA, "batch")
	}
	publishSpans(t, f.q, traceB, "batch")
	if n := f.messages(t, "SPANS"); n != 6 {
		t.Errorf("%d span batches stored, want 6", n)
	}
	if n := f.messages(t, "PENDING"); n != 2 {
		t.Errorf("%d pending markers, want 2 (one per trace)", n)
	}
}

// A trace whose spans never stop arriving is still assembled at max_wait.
func TestMaxWaitForcesAssembly(t *testing.T) {
	const maxWait = 600 * time.Millisecond
	f := setup(t, withSpans(250*time.Millisecond, maxWait))
	got := &collector{}
	f.start(t, 1, got.handle)
	f.startAssemblers(t, 1)

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a span every 100ms, well inside the idle timeout
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				f.q.PublishSpans(context.Background(), map[string][]byte{traceA: []byte("s")})
			}
		}
	}()
	publishSpans(t, f.q, traceA, "s")

	eventually(t, "the trace to be assembled despite the steady spans", func() bool { return len(got.snapshot()) >= 1 })
	elapsed := time.Since(start)
	close(stop)
	wg.Wait()
	if elapsed < maxWait-100*time.Millisecond {
		t.Errorf("assembled after %v, before max_wait %v", elapsed, maxWait)
	}
	if elapsed > maxWait+2*time.Second {
		t.Errorf("assembled after %v, long past max_wait %v", elapsed, maxWait)
	}
}

func TestUnscorableSpansAreDiscarded(t *testing.T) {
	f := setup(t, withSpans(100*time.Millisecond, 10*time.Second))
	got := &collector{}
	f.start(t, 1, got.handle)
	f.startAssemblers(t, 1)

	publishSpans(t, f.q, traceA, "unscorable")
	publishSpans(t, f.q, traceB, "fine")

	eventually(t, "the scorable trace", func() bool { return len(got.snapshot()) == 1 })
	eventually(t, "all spans to be purged", func() bool { return f.messages(t, "SPANS") == 0 })
	eventually(t, "all markers to be acknowledged", func() bool { return f.messages(t, "PENDING") == 0 })
	if tr := got.snapshot()[0]; tr.TraceID != traceB {
		t.Errorf("queued trace = %s, want only the scorable one", tr.TraceID)
	}
	if n := f.messages(t, f.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0: unscorable spans are not a system error", n)
	}
}

// Spans that turn up after their trace was assembled are assembled as a new
// (partial) trace rather than being left behind.
func TestLateSpansAreNotStranded(t *testing.T) {
	f := setup(t, withSpans(100*time.Millisecond, 10*time.Second))
	got := &collector{}
	f.start(t, 1, got.handle)
	f.startAssemblers(t, 1)

	publishSpans(t, f.q, traceA, "on time")
	eventually(t, "the first assembly", func() bool { return len(got.snapshot()) == 1 })
	eventually(t, "the marker to clear", func() bool { return f.messages(t, "PENDING") == 0 })

	publishSpans(t, f.q, traceA, "late")
	eventually(t, "the late spans to be assembled", func() bool { return len(got.snapshot()) == 2 })
	if q := got.snapshot()[1].Question; q != "late" {
		t.Errorf("second assembly = %q, want only the late batch", q)
	}
	eventually(t, "spans to be purged", func() bool { return f.messages(t, "SPANS") == 0 })
}

// With the work queue full the spans wait; nothing is lost and the trace is
// queued once there is room.
func TestAssemblyWaitsWhenWorkQueueIsFull(t *testing.T) {
	f := setup(t, func(c *queue.Config) {
		withSpans(100*time.Millisecond, 10*time.Second)(c)
		c.MaxMessages = 1
	})
	mustPublish(t, f.q, sample("blocker")) // fills the work queue; no workers yet
	f.startAssemblers(t, 1)

	publishSpans(t, f.q, traceA, "waiting")
	time.Sleep(600 * time.Millisecond) // several assembly attempts
	if n := f.messages(t, "SPANS"); n != 1 {
		t.Fatalf("%d span batches stored, want the trace's spans kept while the queue is full", n)
	}
	if n := f.messages(t, f.cfg.Stream); n != 1 {
		t.Fatalf("%d messages in the work queue, want only the blocker", n)
	}

	got := &collector{}
	f.start(t, 1, got.handle)
	eventually(t, "the assembled trace once there is room", func() bool {
		for _, tr := range got.snapshot() {
			if tr.TraceID == traceA && tr.Question == "waiting" {
				return true
			}
		}
		return false
	})
	eventually(t, "spans to be purged", func() bool { return f.messages(t, "SPANS") == 0 })
}

// Spans stored before a restart are assembled by the next process.
func TestSpansSurviveAssemblerRestart(t *testing.T) {
	f := setup(t, withSpans(100*time.Millisecond, 10*time.Second))
	publishSpans(t, f.q, traceA, "before restart")

	second, err := queue.Connect(context.Background(), f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	got := &collector{}
	w := second.StartWorkers(1, got.handle)
	a := second.StartAssemblers(1, joinBatches)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitFor)
		defer cancel()
		a.Stop(ctx)
		w.Stop(ctx)
	}()
	eventually(t, "the trace to be assembled after the restart", func() bool { return len(got.snapshot()) == 1 })
}

func TestPublishSpansFailures(t *testing.T) {
	// Not configured for spans.
	plain := setup(t, nil)
	if err := plain.q.PublishSpans(context.Background(), map[string][]byte{traceA: []byte("x")}); err == nil {
		t.Error("PublishSpans without span storage configured succeeded")
	}

	// NATS down: the receiver must see an error so the exporter retries.
	f := setup(t, withSpans(time.Hour, time.Hour))
	f.srv.Shutdown()
	f.srv.WaitForShutdown()
	eventually(t, "the client to notice the disconnect", func() bool { return f.q.Ready() != nil })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := f.q.PublishSpans(ctx, map[string][]byte{traceA: []byte("x")})
	if !fault.Retryable(err) {
		t.Errorf("PublishSpans with NATS down = %v, want an unavailable fault", err)
	}
}
