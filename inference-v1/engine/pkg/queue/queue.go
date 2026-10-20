// Package queue is the work queue between the receivers (HTTP API, OTLP)
// and the workers, and the store where OTLP spans wait to be assembled into
// traces. AbstractBroker is what the engine uses; Queue implements it on
// NATS JetStream and MemoryBroker in process.
//
// On JetStream, traces are published to a work-queue stream, pulled by a
// pool of workers through one durable consumer, and moved to a dead-letter
// stream when they keep failing with system errors.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	publishTimeout = 5 * time.Second
	setupTimeout   = 5 * time.Second
	setupRetryWait = 500 * time.Millisecond

	defaultPollWait = 2 * time.Second

	// HeaderTraceID is set on every queued and dead-lettered message.
	HeaderTraceID = "Observer-Trace-Id"
)

// Config configures a broker. URL is read only by Connect; the other
// JetStream settings (streams, subjects, the consumer, ack_wait) only by the
// NATS implementation.
type Config struct {
	URL string
	// ConnectTimeout bounds the wait for NATS at start-up. After that the
	// client reconnects on its own for as long as the process runs.
	ConnectTimeout time.Duration

	Stream      string
	Subject     string
	Consumer    string
	MaxMessages int64

	AckWait           time.Duration
	MaxDeliver        int
	RedeliveryBackoff []time.Duration

	DLQStream  string
	DLQSubject string
	DLQMaxAge  time.Duration

	// Spans configures assembling traces from OTLP spans. Leave it zero
	// when the engine runs without the OTLP receivers.
	Spans SpanConfig

	// PollWait is how long one pull waits for a message, which also bounds
	// how long an idle worker takes to notice a stop. Defaults to 2s.
	PollWait time.Duration

	Logger *slog.Logger
	// Reporter receives every error the queue meets while running. Defaults
	// to observe.Discard.
	Reporter observe.Reporter
}

// retry is the redelivery policy for a trace that hit a system error.
func (c Config) retry() common.RetryPolicy {
	return common.RetryPolicy{MaxDeliver: c.MaxDeliver, Backoff: c.RedeliveryBackoff}
}

// Queue publishes traces to JetStream and hands them to workers.
type Queue struct {
	cfg      Config
	conn     *common.Conn
	ownsConn bool // Close closes conn only when Connect opened it
	js       jetstream.JetStream
	cons     jetstream.Consumer
	logger   *slog.Logger

	// Set only when cfg.Spans is enabled.
	spans       jetstream.Stream
	pendingCons jetstream.Consumer
}

// Connect connects to cfg.URL and sets up the queue on its own connection,
// which Close closes.
func Connect(ctx context.Context, cfg Config) (*Queue, error) {
	conn, err := common.DialNATS(common.NATSConfig{URL: cfg.URL, Logger: cfg.Logger, Reporter: cfg.Reporter})
	if err != nil {
		return nil, fmt.Errorf("queue: %w", err)
	}
	q, err := NewNATS(ctx, conn, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	q.ownsConn = true
	return q, nil
}

// NewNATS creates or updates the trace stream, the dead-letter stream and
// the workers' durable consumer on a shared connection. It keeps trying
// until cfg.ConnectTimeout, so the engine can start alongside NATS. Close
// leaves the connection open.
func NewNATS(ctx context.Context, conn *common.Conn, cfg Config) (*Queue, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	if cfg.PollWait <= 0 {
		cfg.PollWait = defaultPollWait
	}
	q := &Queue{cfg: cfg, conn: conn, js: conn.JS, logger: cfg.Logger}
	if err := common.RetryUntil(ctx, cfg.ConnectTimeout, setupRetryWait, q.setup); err != nil {
		return nil, fmt.Errorf("queue: NATS JetStream not ready after %s: %w", cfg.ConnectTimeout, err)
	}
	return q, nil
}

func (q *Queue) setup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	if err := q.conn.EnsureStreams(ctx, q.traceStream(), q.deadLetterStream()); err != nil {
		return err
	}
	// MaxDeliver stays unlimited on the server: the workers enforce the
	// limit themselves by dead-lettering. A server-side limit would leave a
	// trace stranded in the stream if a worker died on its last delivery.
	cons, err := q.js.CreateOrUpdateConsumer(ctx, q.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       q.cfg.Consumer,
		Description:   "Observer engine worker pool",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       q.cfg.AckWait,
		MaxDeliver:    -1,
		FilterSubject: q.cfg.Subject,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", q.cfg.Consumer, err)
	}
	q.cons = cons
	if q.cfg.Spans.enabled() {
		return q.setupSpans(ctx)
	}
	return nil
}

// traceStream is the work queue: a message is removed once a worker
// acknowledges it. DiscardNew makes a full stream refuse publishes instead
// of silently evicting traces nobody has scored yet.
func (q *Queue) traceStream() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        q.cfg.Stream,
		Description: "Agent traces waiting for a keep/drop decision",
		Subjects:    []string{q.cfg.Subject},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
		MaxMsgs:     q.cfg.MaxMessages,
		Discard:     jetstream.DiscardNew,
	}
}

func (q *Queue) deadLetterStream() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        q.cfg.DLQStream,
		Description: "Traces that could not be scored because of system errors",
		Subjects:    []string{q.cfg.DLQSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      q.cfg.DLQMaxAge,
	}
}

// Publish stores t in the trace stream and returns once NATS has confirmed
// it. A full stream is fault.KindFull; any other failure is
// fault.KindUnavailable.
func (q *Queue) Publish(ctx context.Context, t trace.Trace) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("queue: encode trace: %w", err)
	}
	return q.publishTrace(ctx, t.TraceID, data)
}

func (q *Queue) publishTrace(ctx context.Context, traceID string, data []byte, opts ...jetstream.PublishOpt) error {
	msg := nats.NewMsg(q.cfg.Subject)
	msg.Data = data
	msg.Header.Set(HeaderTraceID, traceID)

	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if _, err := q.js.PublishMsg(ctx, msg, opts...); err != nil {
		if common.IsStreamFull(err) {
			return fault.Full(fault.Queue, "publish trace", err).WithTrace(traceID)
		}
		return fault.Unavailable(fault.Queue, "publish trace", err).WithTrace(traceID)
	}
	return nil
}

// Ready reports whether the connection to NATS is up.
func (q *Queue) Ready() error {
	return q.conn.Ready()
}

// report sends an error the queue met to the configured Reporter.
func (q *Queue) report(err *fault.Error) {
	q.cfg.Reporter.Report(context.Background(), err)
}

// Close closes the connection if Connect opened it. Stop the workers first.
func (q *Queue) Close() {
	if q.ownsConn {
		q.conn.Close()
	}
}
