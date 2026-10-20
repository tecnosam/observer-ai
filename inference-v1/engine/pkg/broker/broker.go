// Package broker chooses where the engine keeps its queues. An
// AbstractFactory builds a matching family: the trace queue and span store
// (queue.AbstractBroker) and the export queues (export.AbstractQueueFactory),
// both on NATS JetStream or both in process.
package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/export"
	exportmem "github.com/observer-ai/engine/pkg/export/memq"
	exportnats "github.com/observer-ai/engine/pkg/export/natsq"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/queue"
	tracemem "github.com/observer-ai/engine/pkg/queue/memq"
	tracenats "github.com/observer-ai/engine/pkg/queue/natsq"
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
	ExportQueues(streams exportnats.Streams) export.AbstractQueueFactory
	// Close releases what the factory holds. Close what it built first.
	Close()
}

// Config is what New chooses a broker from.
type Config struct {
	// URL is the NATS server. Empty means in process.
	URL      string
	Auth     NATSAuth
	TLS      NATSTLS
	Logger   *slog.Logger
	Reporter observe.Reporter
}

// New returns the NATS factory when cfg.URL is set, else the in-memory one.
func New(cfg Config) (AbstractFactory, error) {
	if cfg.URL == "" {
		return Memory{}, nil
	}
	opts, err := natsOptions(cfg.Auth, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("nats: %w", err)
	}
	conn, err := common.DialNATS(common.NATSConfig{URL: cfg.URL, Logger: cfg.Logger, Reporter: cfg.Reporter, Options: opts})
	if err != nil {
		return nil, err
	}
	return &NATS{conn: conn}, nil
}

// NATS keeps everything in JetStream on one shared connection: queued
// traces, waiting spans and queued exports survive a restart, and engines
// pointed at the same server share the work.
type NATS struct {
	conn *common.Conn
}

func (*NATS) Kind() Kind { return KindNATS }

func (n *NATS) Ingest(ctx context.Context, cfg queue.Config) (queue.AbstractBroker, error) {
	return tracenats.New(ctx, n.conn, cfg)
}

func (n *NATS) ExportQueues(streams exportnats.Streams) export.AbstractQueueFactory {
	return exportnats.Factory{Conn: n.conn, Streams: streams}
}

func (n *NATS) Close() { n.conn.Close() }

// Memory keeps everything in process: nothing to run beside the engine, but
// queued work is lost when it stops, and each engine works alone.
type Memory struct{}

func (Memory) Kind() Kind { return KindMemory }

func (Memory) Ingest(_ context.Context, cfg queue.Config) (queue.AbstractBroker, error) {
	return tracemem.New(cfg), nil
}

func (Memory) ExportQueues(exportnats.Streams) export.AbstractQueueFactory {
	return exportmem.Factory{}
}

func (Memory) Close() {}
