package queue

import (
	"context"

	"github.com/observer-ai/engine/pkg/trace"
)

// AbstractBroker is the work queue and span store the engine runs on. The
// HTTP API and the assemblers publish traces to it, the OTLP receivers store
// spans in it, and the workers take traces from it. Queue implements it on
// NATS JetStream, MemoryBroker in process.
type AbstractBroker interface {
	// Publish queues one trace. A full queue is fault.KindFull; any other
	// failure fault.KindUnavailable.
	Publish(ctx context.Context, t trace.Trace) error
	// PublishSpans stores span batches, keyed by trace ID, until their trace
	// is assembled.
	PublishSpans(ctx context.Context, byTrace map[string][]byte) error
	// StartWorkers starts n workers handing queued traces to handler.
	StartWorkers(n int, handler Handler) Stopper
	// StartAssemblers starts n assemblers turning completed span sets into
	// queued traces.
	StartAssemblers(n int, assemble AssembleFunc) Stopper
	// Ready reports whether the broker can take traces.
	Ready() error
	// Close releases the broker. Stop its workers and assemblers first.
	Close()
}

// Stopper is a running pool of workers or assemblers.
type Stopper interface {
	// Stop stops the pool after the work in flight, or when ctx ends.
	Stop(ctx context.Context) error
}

var (
	_ AbstractBroker = (*Queue)(nil)
	_ AbstractBroker = (*MemoryBroker)(nil)
)
