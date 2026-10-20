package memq_test

import (
	"context"
	"testing"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/queue"
	"github.com/observer-ai/engine/pkg/queue/memq"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
)

func newMemory(t *testing.T, mutate func(*queue.Config)) (*memq.Broker, *queuetest.Faults) {
	t.Helper()
	cfg := queuetest.Config("")
	reported := &queuetest.Faults{}
	cfg.Reporter = reported
	if mutate != nil {
		mutate(&cfg)
	}
	b := memq.New(cfg)
	t.Cleanup(b.Close)
	return b, reported
}

// A closed broker refuses traces and spans and is not ready.
func TestClosedBrokerRefuses(t *testing.T) {
	b, _ := newMemory(t, nil)
	b.Close()
	if err := b.Publish(context.Background(), queuetest.Sample("t1")); fault.KindOf(err) != fault.KindUnavailable {
		t.Errorf("Publish = %v", err)
	}
	if err := b.PublishSpans(context.Background(), map[string][]byte{"t1": []byte("a")}); fault.KindOf(err) != fault.KindUnavailable {
		t.Errorf("PublishSpans = %v", err)
	}
	if b.Ready() == nil {
		t.Error("Ready = nil after Close")
	}
}

// An assembled trace that finds the queue full waits for room rather than
// being lost.
