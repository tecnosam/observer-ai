package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	memoryStream         = "memory"
	memoryMinScan        = 10 * time.Millisecond
	memoryMaxScan        = time.Second
	memoryAssembleBuffer = 64
)

// errBrokerClosed is returned by a MemoryBroker after Close.
var errBrokerClosed = errors.New("queue: broker is closed")

// MemoryBroker is an in-process AbstractBroker for running without NATS. It
// keeps the same rules as the JetStream queue (a bounded queue, redelivery
// with backoff up to MaxDeliver, then a dead letter; spans assembled after
// IdleTimeout of quiet or MaxWait) but nothing survives a restart, and
// engines cannot share the work. A dead letter is reported to the error
// sinks, not stored.
type MemoryBroker struct {
	cfg    Config
	logger *slog.Logger

	jobs   chan *memoryItem // never closed: redelivery timers may still send
	size   atomic.Int64     // traces queued, waiting to be redelivered, or being handled
	seq    atomic.Uint64
	closed atomic.Bool

	mu    sync.Mutex
	spans map[string]*pendingSpans
}

type memoryItem struct {
	trace    trace.Trace
	count    int // deliveries so far
	sequence uint64
}

type pendingSpans struct {
	batches     [][]byte
	first, last time.Time
}

// NewMemory builds an in-process broker. Of cfg it reads MaxMessages,
// MaxDeliver, RedeliveryBackoff, Spans.IdleTimeout and Spans.MaxWait.
func NewMemory(cfg Config) *MemoryBroker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	return &MemoryBroker{
		cfg:    cfg,
		logger: cfg.Logger,
		jobs:   make(chan *memoryItem, max(cfg.MaxMessages, 1)),
		spans:  map[string]*pendingSpans{},
	}
}

// Publish queues t. It is refused with fault.KindFull at MaxMessages.
func (b *MemoryBroker) Publish(_ context.Context, t trace.Trace) error {
	if b.closed.Load() {
		return fault.Unavailable(fault.Queue, "publish trace", errBrokerClosed).WithTrace(t.TraceID)
	}
	if b.size.Add(1) > int64(cap(b.jobs)) {
		b.size.Add(-1)
		return fault.Full(fault.Queue, "publish trace", fault.Msg("queue is at max_messages (%d)", cap(b.jobs))).WithTrace(t.TraceID)
	}
	b.jobs <- &memoryItem{trace: t, sequence: b.seq.Add(1)} // never blocks: size <= cap
	return nil
}

// PublishSpans stores span batches until their trace goes quiet. It is
// refused with fault.KindFull when maxPendingAssemblies traces are waiting.
func (b *MemoryBroker) PublishSpans(_ context.Context, byTrace map[string][]byte) error {
	if b.closed.Load() {
		return fault.Unavailable(fault.Queue, "store spans", errBrokerClosed)
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	for traceID, data := range byTrace {
		p := b.spans[traceID]
		if p == nil {
			if len(b.spans) >= maxPendingAssemblies {
				return fault.Full(fault.Queue, "store spans", fault.Msg("%d traces are waiting to be assembled", len(b.spans))).WithTrace(traceID)
			}
			p = &pendingSpans{first: now}
			b.spans[traceID] = p
		}
		p.batches = append(p.batches, data)
		p.last = now
	}
	return nil
}

// Ready reports an error once the broker is closed.
func (b *MemoryBroker) Ready() error {
	if b.closed.Load() {
		return errBrokerClosed
	}
	return nil
}

// Close refuses further traces and spans and says how many were lost.
func (b *MemoryBroker) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.mu.Lock()
	pending := len(b.spans)
	b.mu.Unlock()
	if queued := b.size.Load(); queued > 0 || pending > 0 {
		b.logger.Warn("in-memory queue closed with work left, it is lost",
			"traces_queued", queued, "traces_assembling", pending)
	}
}

func (b *MemoryBroker) report(err *fault.Error) {
	b.cfg.Reporter.Report(context.Background(), err)
}

// --- workers ---

type memoryWorkers struct {
	b       *MemoryBroker
	handler Handler
	*common.Pool
}

// StartWorkers starts n workers taking traces from the queue.
func (b *MemoryBroker) StartWorkers(n int, handler Handler) Stopper {
	w := &memoryWorkers{b: b, handler: handler, Pool: common.NewPool()}
	w.Start(n, w.run)
	return w
}

func (w *memoryWorkers) run() {
	for {
		select {
		case <-w.Stopped():
			return
		case it := <-w.b.jobs:
			if w.Stopping() {
				w.b.jobs <- it // both were ready and the job won: leave it queued
				return
			}
			w.handle(it)
		}
	}
}

func (w *memoryWorkers) handle(it *memoryItem) {
	it.count++
	if it.trace.TraceID == "" {
		it.trace.TraceID = sequenceID(memoryStream, it.sequence)
	}
	err := w.handler(w.WorkCtx(), it.trace)
	switch {
	case err == nil:
		w.b.size.Add(-1)
	case w.WorkCtx().Err() != nil:
		it.count-- // not a real attempt
		w.b.jobs <- it
		w.b.logger.Warn("trace returned to the queue on shutdown", "trace_id", it.trace.TraceID)
	case w.b.cfg.retry().Exhausted(it.count):
		w.b.size.Add(-1)
		w.b.report(fault.New(fault.KindOf(err), fault.Queue, "dead-letter trace", err).AsSevere().
			WithTrace(it.trace.TraceID).With("reason", ReasonSystemError).With("deliveries", it.count).With("dlq_stream", memoryStream))
	default:
		delay := w.b.cfg.retry().Delay(it.count)
		w.b.report(fault.New(fault.KindOf(err), fault.Queue, "process trace, will redeliver", err).
			WithTrace(it.trace.TraceID).With("delivery", it.count).With("max_deliver", w.b.cfg.MaxDeliver).With("retry_in", delay.String()))
		time.AfterFunc(delay, func() { w.b.jobs <- it })
	}
}

// --- assemblers ---

type memoryAssemblers struct {
	b        *MemoryBroker
	assemble AssembleFunc
	due      chan memoryAssembly
	*common.Pool
}

type memoryAssembly struct {
	traceID string
	batches [][]byte
}

// StartAssemblers starts a scanner that picks out traces gone quiet, and n
// assemblers that turn them into queued traces.
func (b *MemoryBroker) StartAssemblers(n int, assemble AssembleFunc) Stopper {
	a := &memoryAssemblers{b: b, assemble: assemble, due: make(chan memoryAssembly, memoryAssembleBuffer), Pool: common.NewPool()}
	a.Start(1, a.scan)
	a.Start(n, a.run)
	return a
}

// scan takes every complete trace out of the span store, at a quarter of
// the idle timeout. Spans arriving later start a new trace, as on JetStream.
func (a *memoryAssemblers) scan() {
	sc := a.b.cfg.Spans
	ticker := time.NewTicker(min(max(sc.IdleTimeout/4, memoryMinScan), memoryMaxScan))
	defer ticker.Stop()
	for {
		select {
		case <-a.Stopped():
			return
		case now := <-ticker.C:
			for _, due := range a.b.takeComplete(now) {
				select {
				case a.due <- due:
				case <-a.Stopped():
					return
				}
			}
		}
	}
}

func (b *MemoryBroker) takeComplete(now time.Time) []memoryAssembly {
	sc := b.cfg.Spans
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []memoryAssembly
	for traceID, p := range b.spans {
		if now.Sub(p.last) >= sc.IdleTimeout || now.Sub(p.first) >= sc.MaxWait {
			out = append(out, memoryAssembly{traceID: traceID, batches: p.batches})
			delete(b.spans, traceID)
		}
	}
	return out
}

func (a *memoryAssemblers) run() {
	for {
		select {
		case <-a.Stopped():
			return
		case due := <-a.due:
			a.handle(due)
		}
	}
}

func (a *memoryAssemblers) handle(due memoryAssembly) {
	t, err := a.assemble(due.traceID, due.batches)
	if err != nil {
		a.b.logger.Info("spans discarded", "trace_id", due.traceID, "batches", len(due.batches), "reason", err.Error())
		return
	}
	// A full queue clears as the workers catch up: keep trying until stopped.
	for {
		err := a.b.Publish(context.Background(), t)
		if err == nil {
			break
		}
		a.b.report(fault.New(fault.KindOf(err), fault.Queue, "queue assembled trace, will retry", err).WithTrace(t.TraceID))
		select {
		case <-a.Stopped():
			return
		case <-time.After(assemblerRetryDelay):
		}
	}
	a.b.logger.Info("trace assembled from spans",
		"trace_id", due.traceID, "batches", len(due.batches), "tool_calls", len(t.ToolCalls), "evidence", len(t.Evidence))
}
