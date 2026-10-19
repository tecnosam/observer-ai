package queue_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/trace"
)

// faults keeps every error the broker reports.
type faults struct {
	mu   sync.Mutex
	errs []*fault.Error
}

func (f *faults) Report(_ context.Context, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fe, _ := fault.As(err)
	f.errs = append(f.errs, fe)
}

func (f *faults) ops() []fault.Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fault.Op
	for _, e := range f.errs {
		out = append(out, e.Op)
	}
	return out
}

func newMemory(t *testing.T, mutate func(*queue.Config)) (*queue.MemoryBroker, *faults) {
	t.Helper()
	cfg := testConfig("")
	reported := &faults{}
	cfg.Reporter = reported
	if mutate != nil {
		mutate(&cfg)
	}
	b := queue.NewMemory(cfg)
	t.Cleanup(b.Close)
	return b, reported
}

func startMemory(t *testing.T, b *queue.MemoryBroker, n int, h queue.Handler) queue.Stopper {
	t.Helper()
	w := b.StartWorkers(n, h)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	return w
}

func TestMemoryPublishedTraceReachesAWorker(t *testing.T) {
	b, _ := newMemory(t, nil)
	c := &collector{}
	startMemory(t, b, 2, c.handle)

	for _, id := range []string{"t1", "t2", "t3"} {
		if err := b.Publish(context.Background(), sample(id)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "three traces handled", func() bool { return len(c.snapshot()) == 3 })
	if err := b.Ready(); err != nil {
		t.Errorf("Ready = %v", err)
	}
}

func TestMemoryPublishWhenFull(t *testing.T) {
	b, _ := newMemory(t, func(c *queue.Config) { c.MaxMessages = 2 })
	for _, id := range []string{"t1", "t2"} {
		if err := b.Publish(context.Background(), sample(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Publish(context.Background(), sample("t3")); fault.KindOf(err) != fault.KindFull {
		t.Fatalf("third publish = %v, want a full fault", err)
	}

	// Handled traces free their place.
	c := &collector{}
	startMemory(t, b, 1, c.handle)
	eventually(t, "the queue to drain", func() bool { return len(c.snapshot()) == 2 })
	if err := b.Publish(context.Background(), sample("t3")); err != nil {
		t.Errorf("publish after draining = %v", err)
	}
}

func TestMemorySystemErrorIsRedeliveredThenSucceeds(t *testing.T) {
	b, reported := newMemory(t, nil)
	var calls atomic.Int32
	startMemory(t, b, 1, func(context.Context, trace.Trace) error {
		if calls.Add(1) == 1 {
			return fault.Unavailable(fault.Scorer, "score", errors.New("kev down"))
		}
		return nil
	})
	if err := b.Publish(context.Background(), sample("t1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the second delivery", func() bool { return calls.Load() == 2 })
	if ops := reported.ops(); len(ops) != 1 || ops[0] != "process trace, will redeliver" {
		t.Errorf("reported %v", ops)
	}
}

func TestMemoryPersistentSystemErrorIsDeadLettered(t *testing.T) {
	b, reported := newMemory(t, nil) // MaxDeliver 3
	var calls atomic.Int32
	startMemory(t, b, 1, func(context.Context, trace.Trace) error {
		calls.Add(1)
		return fault.Unavailable(fault.Scorer, "score", errors.New("kev down"))
	})
	if err := b.Publish(context.Background(), sample("t1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the dead letter", func() bool { return len(reported.ops()) == 3 })
	time.Sleep(100 * time.Millisecond) // no fourth delivery
	if n := calls.Load(); n != 3 {
		t.Errorf("%d deliveries, want max_deliver (3)", n)
	}
	reported.mu.Lock()
	dead := reported.errs[2]
	reported.mu.Unlock()
	if dead.Op != "dead-letter trace" || !dead.Severe || dead.TraceID != "t1" || dead.Attrs["reason"] != queue.ReasonSystemError {
		t.Errorf("dead letter = %+v", dead)
	}
}

func TestMemoryTraceWithoutIDGetsOne(t *testing.T) {
	b, _ := newMemory(t, nil)
	c := &collector{}
	startMemory(t, b, 1, c.handle)
	if err := b.Publish(context.Background(), trace.Trace{Question: "q", FinalAnswer: "a"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the trace", func() bool { return len(c.snapshot()) == 1 })
	if id := c.snapshot()[0].TraceID; len(id) != 32 {
		t.Errorf("trace ID = %q, want 32 hex characters", id)
	}
}

// When Stop runs out of time the work is cancelled and the trace goes back
// on the queue, without counting as a delivery.
func TestMemoryStopTimeoutReturnsTrace(t *testing.T) {
	b, _ := newMemory(t, nil)
	started := make(chan struct{})
	w := b.StartWorkers(1, func(ctx context.Context, _ trace.Trace) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err := b.Publish(context.Background(), sample("t1")); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}

	c := &collector{}
	startMemory(t, b, 1, c.handle)
	eventually(t, "the returned trace", func() bool { return len(c.snapshot()) == 1 })
}

func TestMemoryClosedBrokerRefuses(t *testing.T) {
	b, _ := newMemory(t, nil)
	b.Close()
	if err := b.Publish(context.Background(), sample("t1")); fault.KindOf(err) != fault.KindUnavailable {
		t.Errorf("Publish = %v", err)
	}
	if err := b.PublishSpans(context.Background(), map[string][]byte{"t1": []byte("a")}); fault.KindOf(err) != fault.KindUnavailable {
		t.Errorf("PublishSpans = %v", err)
	}
	if b.Ready() == nil {
		t.Error("Ready = nil after Close")
	}
}

func startMemoryAssembly(t *testing.T, b *queue.MemoryBroker) *collector {
	t.Helper()
	c := &collector{}
	startMemory(t, b, 1, c.handle)
	a := b.StartAssemblers(2, joinBatches)
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return c
}

func TestMemorySpansAreAssembledAfterIdleTimeout(t *testing.T) {
	b, _ := newMemory(t, withSpans(100*time.Millisecond, time.Minute))
	c := startMemoryAssembly(t, b)

	publish := func(id, batch string) {
		if err := b.PublishSpans(context.Background(), map[string][]byte{id: []byte(batch)}); err != nil {
			t.Fatal(err)
		}
	}
	publish("t1", "a")
	publish("t2", "x")
	publish("t1", "b")
	time.Sleep(30 * time.Millisecond)
	if n := len(c.snapshot()); n != 0 {
		t.Fatalf("%d traces assembled before the idle timeout", n)
	}
	eventually(t, "both traces assembled", func() bool { return len(c.snapshot()) == 2 })
	byID := map[string]string{}
	for _, tr := range c.snapshot() {
		byID[tr.TraceID] = tr.Question
	}
	if byID["t1"] != "a+b" || byID["t2"] != "x" {
		t.Errorf("assembled %v", byID)
	}
}

func TestMemoryMaxWaitForcesAssembly(t *testing.T) {
	b, _ := newMemory(t, withSpans(200*time.Millisecond, 300*time.Millisecond))
	c := startMemoryAssembly(t, b)

	start := time.Now()
	stop := make(chan struct{})
	defer close(stop)
	go func() { // a span every 50ms: never quiet for the idle timeout
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				_ = b.PublishSpans(context.Background(), map[string][]byte{"t1": []byte("s")})
			}
		}
	}()
	eventually(t, "the trace assembled at max_wait", func() bool { return len(c.snapshot()) >= 1 })
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("assembled after %v, want about max_wait", elapsed)
	}
}

func TestMemoryUnscorableSpansAreDiscarded(t *testing.T) {
	b, _ := newMemory(t, withSpans(50*time.Millisecond, time.Minute))
	c := startMemoryAssembly(t, b)
	if err := b.PublishSpans(context.Background(), map[string][]byte{"bad": []byte("unscorable"), "good": []byte("a")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the good trace", func() bool { return len(c.snapshot()) == 1 })
	time.Sleep(100 * time.Millisecond)
	if got := c.snapshot(); len(got) != 1 || got[0].TraceID != "good" {
		t.Errorf("assembled %v", got)
	}
}

// An assembled trace that finds the queue full waits for room rather than
// being lost.
func TestMemoryAssemblyWaitsWhenQueueIsFull(t *testing.T) {
	b, reported := newMemory(t, func(c *queue.Config) {
		withSpans(30*time.Millisecond, time.Minute)(c)
		c.MaxMessages = 1
	})
	if err := b.Publish(context.Background(), sample("blocker")); err != nil {
		t.Fatal(err)
	}
	a := b.StartAssemblers(1, joinBatches)
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := b.PublishSpans(context.Background(), map[string][]byte{"t1": []byte("a")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the full queue to be reported", func() bool { return len(reported.ops()) > 0 })

	c := &collector{}
	startMemory(t, b, 1, c.handle)
	eventually(t, "both traces handled", func() bool { return len(c.snapshot()) == 2 })
}
