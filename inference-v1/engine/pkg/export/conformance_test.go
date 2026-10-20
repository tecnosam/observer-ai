package export_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/export/memq"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// The conformance suite runs every scenario against each
// export.AbstractQueueFactory, so the in-process export queue and JetStream keep
// the same rules. Where a given-up record goes (reported only, or also a
// dead-letter stream) differs by design and is tested in each one's file.

var queueFactories = map[string]func(t *testing.T) export.AbstractQueueFactory{
	"memory": func(*testing.T) export.AbstractQueueFactory { return memq.Factory{} },
	"nats": func(t *testing.T) export.AbstractQueueFactory {
		f, _ := natsQueues(t)
		return f
	},
}

// sender is a back end's Send that records calls and can fail or block.
type sender struct {
	mu      sync.Mutex
	sent    []string
	calls   atomic.Int32
	fail    func(call int32) error // nil: always succeed
	gate    chan struct{}          // non-nil: block until closed
	started chan struct{}          // non-nil: told as each send begins
}

func (s *sender) send(_ context.Context, rec pipeline.Record) error {
	n := s.calls.Add(1)
	if s.started != nil {
		s.started <- struct{}{}
	}
	if s.gate != nil {
		<-s.gate
	}
	if s.fail != nil {
		if err := s.fail(n); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, rec.Trace.TraceID)
	return nil
}

func (s *sender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// queueConformance runs scenario once per factory, with a fresh sender
// and a queue named "backend" sending to it.
func queueConformance(t *testing.T, cfg export.QueueConfig, newSender func() *sender, scenario func(t *testing.T, q export.AbstractQueue, s *sender, reported *recordingReporter)) {
	for name, factory := range queueFactories {
		t.Run(name, func(t *testing.T) {
			s, reported := newSender(), &recordingReporter{}
			c := cfg
			c.Name, c.Send, c.Reporter = "backend", s.send, reported
			if c.Backoff == nil {
				c.Backoff = []time.Duration{10 * time.Millisecond}
			}
			q, err := factory(t).Queue(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = q.Shutdown(context.Background()) })
			scenario(t, q, s, reported)
		})
	}
}

func enqueue(t *testing.T, q export.AbstractQueue, id string) error {
	t.Helper()
	return q.Enqueue(context.Background(), kept(id))
}

func flush(t *testing.T, q export.AbstractQueue) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func queueDepth(t *testing.T, q export.AbstractQueue) int64 {
	t.Helper()
	n, err := q.Depth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestQueueConformanceSendsEveryRecord(t *testing.T) {
	queueConformance(t, export.QueueConfig{}, func() *sender { return &sender{} }, func(t *testing.T, q export.AbstractQueue, s *sender, _ *recordingReporter) {
		for _, id := range []string{"t1", "t2", "t3"} {
			if err := enqueue(t, q, id); err != nil {
				t.Fatal(err)
			}
		}
		flush(t, q)
		if n := s.count(); n != 3 {
			t.Errorf("sent %d, want 3 before Flush returned", n)
		}
		if n := queueDepth(t, q); n != 0 {
			t.Errorf("Depth = %d after Flush, want 0", n)
		}
	})
}

// Depth and the bound both count records being sent, not only waiting ones.
func TestQueueConformanceBoundAndDepthCountInFlight(t *testing.T) {
	cfg := export.QueueConfig{Size: 2, Concurrency: 1}
	queueConformance(t, cfg, func() *sender { return &sender{gate: make(chan struct{}), started: make(chan struct{}, 1)} },
		func(t *testing.T, q export.AbstractQueue, s *sender, _ *recordingReporter) {
			defer close(s.gate)
			if err := enqueue(t, q, "t1"); err != nil {
				t.Fatal(err)
			}
			<-s.started // t1 is being sent
			if err := enqueue(t, q, "t2"); err != nil {
				t.Fatal(err)
			}
			if err := enqueue(t, q, "t3"); fault.KindOf(err) != fault.KindFull {
				t.Errorf("third enqueue = %v, want a full fault", err)
			}
			if n := queueDepth(t, q); n != 2 {
				t.Errorf("Depth = %d, want 2 (one sending, one waiting)", n)
			}
		})
}

func TestQueueConformanceRetryThenSucceed(t *testing.T) {
	failOnce := func(call int32) error {
		if call == 1 {
			return fault.Unavailable(fault.Export, "forward spans", errors.New("503"))
		}
		return nil
	}
	queueConformance(t, export.QueueConfig{}, func() *sender { return &sender{fail: failOnce} }, func(t *testing.T, q export.AbstractQueue, s *sender, reported *recordingReporter) {
		if err := enqueue(t, q, "t1"); err != nil {
			t.Fatal(err)
		}
		flush(t, q)
		if s.calls.Load() != 2 || s.count() != 1 {
			t.Errorf("calls %d, sent %d; want a failure then a success", s.calls.Load(), s.count())
		}
		errs := reported.all()
		if len(errs) != 1 {
			t.Fatalf("reported %v, want one retry", errs)
		}
		if f, _ := fault.As(errs[0]); f.Op != "export trace, will retry" || f.Severe || f.Attrs["exporter"] != "backend" {
			t.Errorf("retry report = %+v", f)
		}
	})
}

func TestQueueConformanceGivesUpAfterMaxDeliver(t *testing.T) {
	alwaysFail := func(int32) error { return fault.Unavailable(fault.Export, "forward spans", errors.New("503")) }
	queueConformance(t, export.QueueConfig{MaxDeliver: 2}, func() *sender { return &sender{fail: alwaysFail} }, func(t *testing.T, q export.AbstractQueue, s *sender, reported *recordingReporter) {
		if err := enqueue(t, q, "t1"); err != nil {
			t.Fatal(err)
		}
		flush(t, q)
		if n := s.calls.Load(); n != 2 {
			t.Errorf("tried %d times, want max_deliver (2)", n)
		}
		errs := reported.all()
		last, _ := fault.As(errs[len(errs)-1])
		if !strings.HasPrefix(string(last.Op), "export trace, ") || last.Op == "export trace, will retry" || !last.Severe ||
			last.TraceID != "t1" || last.Attrs["exporter"] != "backend" {
			t.Errorf("give-up report = %+v", last)
		}
		if n := queueDepth(t, q); n != 0 {
			t.Errorf("Depth = %d after giving up, want 0", n)
		}
	})
}

// An error a retry cannot fix is given up on the first try.
func TestQueueConformanceGivesUpAtOnceOnInvalid(t *testing.T) {
	invalid := func(int32) error { return fault.Invalid(fault.Export, "annotate stored spans", errors.New("corrupt")) }
	queueConformance(t, export.QueueConfig{}, func() *sender { return &sender{fail: invalid} }, func(t *testing.T, q export.AbstractQueue, s *sender, reported *recordingReporter) {
		if err := enqueue(t, q, "t1"); err != nil {
			t.Fatal(err)
		}
		flush(t, q)
		if n := s.calls.Load(); n != 1 {
			t.Errorf("tried %d times, want once", n)
		}
		if errs := reported.all(); len(errs) != 1 || fault.KindOf(errs[0]) != fault.KindInvalid {
			t.Errorf("reported %v", errs)
		}
	})
}

// After Shutdown a queue takes nothing more.
func TestQueueConformanceShutdownStopsTaking(t *testing.T) {
	queueConformance(t, export.QueueConfig{}, func() *sender { return &sender{} }, func(t *testing.T, q export.AbstractQueue, _ *sender, _ *recordingReporter) {
		if err := q.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := enqueue(t, q, "t1"); fault.KindOf(err) != fault.KindUnavailable {
			t.Errorf("Enqueue after Shutdown = %v, want unavailable", err)
		}
	})
}
