// Package queue is the NATS JetStream work queue between the HTTP API and
// the workers. Traces are published to a work-queue stream, pulled by a pool
// of workers through one durable consumer, and moved to a dead-letter stream
// when they keep failing with system errors.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/trace"
)

const (
	connectionName = "observer-engine"
	publishTimeout = 5 * time.Second
	setupTimeout   = 5 * time.Second
	setupRetryWait = 500 * time.Millisecond
	reconnectWait  = time.Second

	defaultPollWait = 2 * time.Second

	// HeaderTraceID is set on every queued and dead-lettered message.
	HeaderTraceID = "Observer-Trace-Id"
)

var (
	// ErrFull means the stream is at queue.max_messages.
	ErrFull = errors.New("queue full")
	// ErrUnavailable means NATS did not accept the trace.
	ErrUnavailable = errors.New("queue unavailable")
)

// Config configures a Queue.
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
}

// Queue publishes traces to JetStream and hands them to workers.
type Queue struct {
	cfg    Config
	nc     *nats.Conn
	js     jetstream.JetStream
	cons   jetstream.Consumer
	logger *slog.Logger

	// Set only when cfg.Spans is enabled.
	spans       jetstream.Stream
	pendingCons jetstream.Consumer
}

// Connect connects to NATS and creates or updates the trace stream, the
// dead-letter stream and the workers' durable consumer. It keeps trying
// until cfg.ConnectTimeout, so the engine can start alongside NATS.
func Connect(ctx context.Context, cfg Config) (*Queue, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.PollWait <= 0 {
		cfg.PollWait = defaultPollWait
	}
	logger := cfg.Logger

	nc, err := nats.Connect(cfg.URL,
		nats.Name(connectionName),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(reconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warn("nats disconnected", "error", err.Error())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("queue: connect to %s: %w", cfg.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: %w", err)
	}
	q := &Queue{cfg: cfg, nc: nc, js: js, logger: logger}

	ctx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	for {
		err := q.setup(ctx)
		if err == nil {
			return q, nil
		}
		select {
		case <-ctx.Done():
			nc.Close()
			return nil, fmt.Errorf("queue: NATS JetStream at %s not ready after %s: %w", cfg.URL, cfg.ConnectTimeout, err)
		case <-time.After(setupRetryWait):
		}
	}
}

func (q *Queue) setup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	// Work queue: a message is removed once a worker acknowledges it.
	// DiscardNew makes a full stream refuse publishes instead of silently
	// evicting traces nobody has scored yet.
	if _, err := q.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        q.cfg.Stream,
		Description: "Agent traces waiting for a keep/drop decision",
		Subjects:    []string{q.cfg.Subject},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
		MaxMsgs:     q.cfg.MaxMessages,
		Discard:     jetstream.DiscardNew,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", q.cfg.Stream, err)
	}

	if _, err := q.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        q.cfg.DLQStream,
		Description: "Traces that could not be scored because of system errors",
		Subjects:    []string{q.cfg.DLQSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      q.cfg.DLQMaxAge,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", q.cfg.DLQStream, err)
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

// Publish stores t in the trace stream and returns once NATS has confirmed
// it. It returns ErrFull when the stream is at its limit and ErrUnavailable
// for any other failure.
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
		if isStreamFull(err) {
			return ErrFull
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

// isStreamFull recognises the server refusing a publish because the stream
// is at MaxMsgs. JetStream reports it as a store failure whose description
// carries the reason; there is no dedicated error code.
func isStreamFull(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && strings.Contains(apiErr.Description, "maximum messages exceeded")
}

// Ready reports whether the connection to NATS is up.
func (q *Queue) Ready() error {
	if !q.nc.IsConnected() {
		return fmt.Errorf("nats is %s", strings.ToLower(q.nc.Status().String()))
	}
	return nil
}

// Close closes the connection. Stop the workers first.
func (q *Queue) Close() {
	q.nc.Close()
}
