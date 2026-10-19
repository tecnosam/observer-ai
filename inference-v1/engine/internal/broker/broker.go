// Package broker chooses where the engine keeps its queues. An
// AbstractFactory builds a matching family: the trace queue and span store
// (queue.AbstractBroker) and the export queues (export.AbstractQueueFactory),
// both on NATS JetStream or both in process.
package broker

import (
	"context"
	"log/slog"

	"github.com/observer-ai/engine/internal/export"
	"github.com/observer-ai/engine/internal/natsconn"
	"github.com/observer-ai/engine/internal/observe"
	"github.com/observer-ai/engine/internal/queue"
)

// Kind names a broker in logs.
type Kind string

const (
	KindNATS   Kind = "nats"
	KindMemory Kind = "memory"
)

// AbstractFactory builds the engine's queues on one broker.
type AbstractFactory interface {
	Kind() Kind
	// Ingest builds the trace queue and span store.
	Ingest(ctx context.Context, cfg queue.Config) (queue.AbstractBroker, error)
	// ExportQueues builds the export queues. The streams are read only by NATS.
	ExportQueues(streams export.NATSStreams) export.AbstractQueueFactory
	// Close releases what the factory holds. Close what it built first.
	Close()
}

// Config is what New chooses a broker from.
type Config struct {
	// URL is the NATS server. Empty means in process.
	URL      string
	Logger   *slog.Logger
	Reporter observe.Reporter
}

// New returns the NATS factory when cfg.URL is set, else the in-memory one.
func New(cfg Config) (AbstractFactory, error) {
	if cfg.URL == "" {
		return Memory{}, nil
	}
	conn, err := natsconn.Dial(natsconn.Config{URL: cfg.URL, Logger: cfg.Logger, Reporter: cfg.Reporter})
	if err != nil {
		return nil, err
	}
	return &NATS{Conn: conn}, nil
}

// NATS keeps everything in JetStream on one shared connection: queued
// traces, waiting spans and queued exports survive a restart, and engines
// pointed at the same server share the work.
type NATS struct {
	Conn *natsconn.Conn
}

func (*NATS) Kind() Kind { return KindNATS }

func (n *NATS) Ingest(ctx context.Context, cfg queue.Config) (queue.AbstractBroker, error) {
	return queue.NewNATS(ctx, n.Conn, cfg)
}

func (n *NATS) ExportQueues(streams export.NATSStreams) export.AbstractQueueFactory {
	return export.NATSQueueFactory{Conn: n.Conn, Streams: streams}
}

func (n *NATS) Close() { n.Conn.Close() }

// Memory keeps everything in process: nothing to run beside the engine, but
// queued work is lost when it stops, and each engine works alone.
type Memory struct{}

func (Memory) Kind() Kind { return KindMemory }

func (Memory) Ingest(_ context.Context, cfg queue.Config) (queue.AbstractBroker, error) {
	return queue.NewMemory(cfg), nil
}

func (Memory) ExportQueues(export.NATSStreams) export.AbstractQueueFactory {
	return export.MemoryQueueFactory{}
}

func (Memory) Close() {}
