package queuetest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/queue"
	"github.com/observer-ai/engine/pkg/trace"
)

// WaitFor bounds every Eventually.
const WaitFor = 10 * time.Second

// Config is a broker config fit for tests: small, fast retries, short polls.
func Config(url string) queue.Config {
	return queue.Config{
		URL:               url,
		ConnectTimeout:    5 * time.Second,
		Stream:            "TRACES",
		Subject:           "traces.in",
		Consumer:          "workers",
		MaxMessages:       1000,
		AckWait:           30 * time.Second,
		MaxDeliver:        3,
		RedeliveryBackoff: []time.Duration{20 * time.Millisecond},
		DLQStream:         "TRACES_DLQ",
		DLQSubject:        "traces.dlq",
		DLQMaxAge:         time.Hour,
		PollWait:          100 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// WithSpans turns span assembly on, with the given timeouts.
func WithSpans(idle, maxWait time.Duration) func(*queue.Config) {
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

// JoinBatches stands in for the OTLP assembler: the batches are plain text,
// the trace's question is the batches joined in arrival order, and a batch
// saying "unscorable" makes the whole trace unscorable.
func JoinBatches(traceID string, batches [][]byte) (trace.Trace, error) {
	var parts []string
	for _, b := range batches {
		if string(b) == "unscorable" {
			return trace.Trace{}, errors.New("trace is not scorable: no question")
		}
		parts = append(parts, string(b))
	}
	return trace.Trace{TraceID: traceID, Question: strings.Join(parts, "+"), FinalAnswer: "a", OTLP: []byte("original spans")}, nil
}

// Sample is a small valid trace.
func Sample(id string) trace.Trace {
	return trace.Trace{TraceID: id, Question: "q " + id, FinalAnswer: "a"}
}

// Eventually waits up to WaitFor for cond.
func Eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(WaitFor)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Collector is a queue.Handler that keeps every trace it is given.
type Collector struct {
	mu     sync.Mutex
	traces []trace.Trace
}

func (c *Collector) Handle(_ context.Context, t trace.Trace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, t)
	return nil
}

func (c *Collector) Snapshot() []trace.Trace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]trace.Trace(nil), c.traces...)
}

// Faults is an observe.Reporter that keeps every error reported.
type Faults struct {
	mu   sync.Mutex
	errs []*fault.Error
}

func (f *Faults) Report(_ context.Context, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fe, _ := fault.As(err)
	f.errs = append(f.errs, fe)
}

func (f *Faults) All() []*fault.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fault.Error(nil), f.errs...)
}

// Ops is the operation of each reported error, in order.
func (f *Faults) Ops() []fault.Op {
	var out []fault.Op
	for _, e := range f.All() {
		out = append(out, e.Op)
	}
	return out
}
