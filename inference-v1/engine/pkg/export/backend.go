// Package export sends kept traces to trace back ends over OTLP/HTTP: Phoenix,
// Langfuse, an OpenTelemetry Collector, or anything else that accepts OTLP
// traces. Each configured back end is an AbstractBackend; the Dispatcher
// queues every kept trace to all of them and sends asynchronously, so a slow
// or unreachable back end never holds up scoring.
package export

import (
	"context"
	"time"

	"github.com/observer-ai/engine/pkg/pipeline"
)

// AbstractBackend sends traces to one destination.
type AbstractBackend interface {
	Send(ctx context.Context, rec pipeline.Record) error
	// Flush sends anything buffered now.
	Flush(ctx context.Context) error
	// Shutdown flushes and releases the backend.
	Shutdown(ctx context.Context) error
}

// BackendConfig is one destination, as the config describes it.
type BackendConfig struct {
	Name     string
	Endpoint string            // the full OTLP/HTTP traces URL
	Headers  map[string]string // sent with every request
	// ResourceAttributes are set on the resource of every span sent.
	ResourceAttributes map[string]string
	QueueSize          int // traces waiting to be sent; 0 means DefaultQueueSize
	Concurrency        int // traces sent at once; 0 means DefaultConcurrency
	// MaxDeliver is how many times a trace is tried; 0 means
	// DefaultMaxDeliver. Backoff is the wait before each retry; nil means
	// DefaultBackoff.
	MaxDeliver int
	Backoff    []time.Duration
}

const (
	DefaultQueueSize   = 1024
	DefaultConcurrency = 4
)

// AbstractBackendFactory builds a backend from its configuration.
type AbstractBackendFactory interface {
	Build(ctx context.Context, cfg BackendConfig) (AbstractBackend, error)
}

// OTLPBackendFactory builds OTLP/HTTP backends.
type OTLPBackendFactory struct{}

func (OTLPBackendFactory) Build(ctx context.Context, cfg BackendConfig) (AbstractBackend, error) {
	return NewOTLPBackend(ctx, cfg)
}
