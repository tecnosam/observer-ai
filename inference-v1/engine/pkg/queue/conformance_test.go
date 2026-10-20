package queue_test

import (
	"context"
	"errors"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/queue"
	"github.com/observer-ai/engine/pkg/queue/memq"
	"github.com/observer-ai/engine/pkg/queue/natsq"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
	"github.com/observer-ai/engine/pkg/trace"
)

// The conformance suite runs every scenario against each AbstractBroker,
// so the in-process broker and JetStream keep the same rules. A behaviour
// only one of them has (dead letters stored in a stream, a closed broker)
// is tested in that broker's own file.

// brokers builds each implementation with the shared test settings.
var brokers = map[string]func(t *testing.T, cfg queue.Config) queue.AbstractBroker{
	"memory": func(t *testing.T, cfg queue.Config) queue.AbstractBroker {
		b := memq.New(cfg)
		t.Cleanup(b.Close)
		return b
	},
	"nats": func(t *testing.T, cfg queue.Config) queue.AbstractBroker {
		cfg.URL = queuetest.StartServer(t).ClientURL()
		b, err := natsq.Connect(context.Background(), cfg)
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		t.Cleanup(b.Close)
		return b
	},
}

// conformance runs scenario once per broker, each with its own fresh broker.
func conformance(t *testing.T, mutate func(*queue.Config), scenario func(t *testing.T, b queue.AbstractBroker, reported *queuetest.Faults)) {
	for name, build := range brokers {
		t.Run(name, func(t *testing.T) {
			cfg := queuetest.Config("")
			reported := &queuetest.Faults{}
			cfg.Reporter = reported
			if mutate != nil {
				mutate(&cfg)
			}
			scenario(t, build(t, cfg), reported)
		})
	}
}

func workers(t *testing.T, b queue.AbstractBroker, n int, h queue.Handler) queue.Stopper {
	t.Helper()
	w := b.StartWorkers(n, h)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	return w
}

func publish(t *testing.T, b queue.AbstractBroker, tr trace.Trace) {
	t.Helper()
	if err := b.Publish(context.Background(), tr); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func depth(t *testing.T, b queue.AbstractBroker) int64 {
	t.Helper()
	n, err := b.Depth(context.Background())
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	return n
}

func TestConformanceEveryTraceReachesAWorker(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		c := &queuetest.Collector{}
		workers(t, b, 2, c.Handle)
		for _, id := range []string{"t1", "t2", "t3"} {
			publish(t, b, queuetest.Sample(id))
		}
		queuetest.Eventually(t, "three traces handled", func() bool { return len(c.Snapshot()) == 3 })
		queuetest.Eventually(t, "the queue to empty", func() bool { return depth(t, b) == 0 })
		if err := b.Ready(); err != nil {
			t.Errorf("Ready = %v", err)
		}
	})
}

func TestConformanceDepthCountsWaitingTraces(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		publish(t, b, queuetest.Sample("t1"))
		publish(t, b, queuetest.Sample("t2"))
		if n := depth(t, b); n != 2 {
			t.Errorf("Depth = %d, want 2", n)
		}
	})
}

func TestConformanceFullQueueRefuses(t *testing.T) {
	conformance(t, func(c *queue.Config) { c.MaxMessages = 2 }, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		publish(t, b, queuetest.Sample("t1"))
		publish(t, b, queuetest.Sample("t2"))
		if err := b.Publish(context.Background(), queuetest.Sample("t3")); fault.KindOf(err) != fault.KindFull {
			t.Errorf("third publish = %v, want a full fault", err)
		}
	})
}

func TestConformanceSystemErrorIsRedeliveredThenSucceeds(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, reported *queuetest.Faults) {
		var calls atomic.Int32
		workers(t, b, 1, func(context.Context, trace.Trace) error {
			if calls.Add(1) == 1 {
				return fault.Unavailable(fault.Scorer, "score", errors.New("kev down"))
			}
			return nil
		})
		publish(t, b, queuetest.Sample("t1"))
		queuetest.Eventually(t, "the second delivery", func() bool { return calls.Load() == 2 })
		queuetest.Eventually(t, "the queue to empty", func() bool { return depth(t, b) == 0 })
		if ops := reported.Ops(); len(ops) != 1 || ops[0] != "process trace, will redeliver" {
			t.Errorf("reported %v", ops)
		}
	})
}

func TestConformancePersistentErrorIsDeadLettered(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, reported *queuetest.Faults) { // MaxDeliver 3
		var calls atomic.Int32
		workers(t, b, 1, func(context.Context, trace.Trace) error {
			calls.Add(1)
			return fault.Unavailable(fault.Scorer, "score", errors.New("kev down"))
		})
		publish(t, b, queuetest.Sample("t1"))
		queuetest.Eventually(t, "the dead letter", func() bool { return len(reported.Ops()) == 3 })
		time.Sleep(100 * time.Millisecond) // and no fourth delivery
		if n := calls.Load(); n != 3 {
			t.Errorf("%d deliveries, want max_deliver (3)", n)
		}
		dead := reported.All()[2]
		if dead.Op != "dead-letter trace" || !dead.Severe || dead.TraceID != "t1" || dead.Attrs["reason"] != queue.ReasonSystemError {
			t.Errorf("dead letter = %+v", dead)
		}
		queuetest.Eventually(t, "the queue to empty", func() bool { return depth(t, b) == 0 })
	})
}

var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestConformanceTraceWithoutIDGetsOne(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		c := &queuetest.Collector{}
		workers(t, b, 1, c.Handle)
		publish(t, b, trace.Trace{Question: "q", FinalAnswer: "a"})
		queuetest.Eventually(t, "the trace", func() bool { return len(c.Snapshot()) == 1 })
		if id := c.Snapshot()[0].TraceID; !traceIDPattern.MatchString(id) {
			t.Errorf("trace ID = %q, want 32 hex characters", id)
		}
	})
}

// A worker whose Stop runs out of time gives its trace back, and the next
// worker gets it.
func TestConformanceStopTimeoutReturnsTrace(t *testing.T) {
	conformance(t, nil, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		started := make(chan struct{})
		w := b.StartWorkers(1, func(ctx context.Context, _ trace.Trace) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		publish(t, b, queuetest.Sample("t1"))
		<-started
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := w.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop = %v", err)
		}
		c := &queuetest.Collector{}
		workers(t, b, 1, c.Handle)
		queuetest.Eventually(t, "the returned trace", func() bool { return len(c.Snapshot()) == 1 })
	})
}

func assemblers(t *testing.T, b queue.AbstractBroker) *queuetest.Collector {
	t.Helper()
	c := &queuetest.Collector{}
	workers(t, b, 1, c.Handle)
	a := b.StartAssemblers(2, queuetest.JoinBatches)
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return c
}

func publishSpansTo(t *testing.T, b queue.AbstractBroker, traceID, batch string) {
	t.Helper()
	if err := b.PublishSpans(context.Background(), map[string][]byte{traceID: []byte(batch)}); err != nil {
		t.Fatal(err)
	}
}

func TestConformanceSpansAreAssembledAfterIdleTimeout(t *testing.T) {
	conformance(t, queuetest.WithSpans(300*time.Millisecond, time.Minute), func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		c := assemblers(t, b)
		publishSpansTo(t, b, "t1", "a")
		publishSpansTo(t, b, "t2", "x")
		publishSpansTo(t, b, "t1", "b")
		time.Sleep(50 * time.Millisecond)
		if n := len(c.Snapshot()); n != 0 {
			t.Fatalf("%d traces assembled before the idle timeout", n)
		}
		queuetest.Eventually(t, "both traces assembled", func() bool { return len(c.Snapshot()) == 2 })
		byID := map[string]string{}
		for _, tr := range c.Snapshot() {
			byID[tr.TraceID] = tr.Question
		}
		if byID["t1"] != "a+b" || byID["t2"] != "x" {
			t.Errorf("assembled %v, want each trace's batches joined in order", byID)
		}
	})
}

func TestConformanceMaxWaitForcesAssembly(t *testing.T) {
	conformance(t, queuetest.WithSpans(300*time.Millisecond, 500*time.Millisecond), func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		c := assemblers(t, b)
		stop := make(chan struct{})
		defer close(stop)
		go func() { // a span every 100ms: never quiet for the idle timeout
			for {
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Millisecond):
					_ = b.PublishSpans(context.Background(), map[string][]byte{"t1": []byte("s")})
				}
			}
		}()
		queuetest.Eventually(t, "the trace assembled at max_wait", func() bool { return len(c.Snapshot()) >= 1 })
	})
}

func TestConformanceUnscorableSpansAreDiscarded(t *testing.T) {
	conformance(t, queuetest.WithSpans(100*time.Millisecond, time.Minute), func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		c := assemblers(t, b)
		if err := b.PublishSpans(context.Background(), map[string][]byte{"bad": []byte("unscorable"), "good": []byte("a")}); err != nil {
			t.Fatal(err)
		}
		queuetest.Eventually(t, "the good trace", func() bool { return len(c.Snapshot()) == 1 })
		time.Sleep(300 * time.Millisecond)
		if got := c.Snapshot(); len(got) != 1 || got[0].TraceID != "good" {
			t.Errorf("assembled %v", got)
		}
	})
}

// With the work queue full an assembled trace waits for room; it is not lost.
func TestConformanceAssemblyWaitsWhenQueueIsFull(t *testing.T) {
	conformance(t, func(c *queue.Config) {
		queuetest.WithSpans(100*time.Millisecond, 10*time.Second)(c)
		c.MaxMessages = 1
	}, func(t *testing.T, b queue.AbstractBroker, _ *queuetest.Faults) {
		publish(t, b, queuetest.Sample("blocker")) // fills the work queue; no workers yet
		a := b.StartAssemblers(1, queuetest.JoinBatches)
		t.Cleanup(func() { _ = a.Stop(context.Background()) })
		publishSpansTo(t, b, "t1", "waiting")
		time.Sleep(500 * time.Millisecond) // several assembly attempts

		c := &queuetest.Collector{}
		workers(t, b, 1, c.Handle)
		queuetest.Eventually(t, "both traces handled", func() bool { return len(c.Snapshot()) == 2 })
	})
}
