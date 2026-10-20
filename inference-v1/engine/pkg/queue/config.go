// Package queue is the work queue between the receivers (HTTP API, OTLP)
// and the workers, and the store where OTLP spans wait to be assembled into
// traces. AbstractBroker is what the engine uses; natsq implements it on
// NATS JetStream and memq in process. This package holds what both share:
// the interfaces, the settings, and the rules they keep alike.
package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"
	"time"

	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/trace"
)

// Config configures a broker. URL is read only by natsq.Connect; the other
// JetStream settings (streams, subjects, the consumer, ack_wait) only by
// natsq.
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

	// Replicas is how many copies of each stream a NATS cluster keeps;
	// 0 or 1 is one. More than the cluster has servers fails at start-up.
	Replicas int

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

// SpanConfig configures how OTLP spans are stored and assembled into traces.
type SpanConfig struct {
	// Stream holds spans, one subject per trace: <Subject>.<trace id>.
	Stream  string
	Subject string
	// MaxAge removes spans that were never assembled.
	MaxAge time.Duration

	// PendingStream holds one marker per trace that has spans waiting.
	PendingStream  string
	PendingSubject string

	// IdleTimeout is how long a trace must receive no spans before it is
	// considered complete.
	IdleTimeout time.Duration
	// MaxWait assembles a trace this long after its first span even if
	// spans are still arriving.
	MaxWait time.Duration
}

// Enabled reports whether span storage is configured.
func (c SpanConfig) Enabled() bool { return c.Stream != "" }

// Handler processes one trace. Returning an error means a system error: the
// trace was not processed and should be tried again later. A handler that
// reaches a decision, by whatever route, returns nil.
type Handler func(ctx context.Context, t trace.Trace) error

// AssembleFunc builds a trace from the span batches collected for it. An
// error means the spans do not make a trace worth scoring; they are then
// discarded.
type AssembleFunc func(traceID string, batches [][]byte) (trace.Trace, error)

// Dead-letter reasons.
const (
	ReasonSystemError    = "system_error"    // kept failing until max_deliver
	ReasonInvalidPayload = "invalid_payload" // not a valid trace; never retried
)

// Rules both brokers keep alike.
const (
	// MaxPendingAssemblies bounds the traces waiting to be assembled.
	MaxPendingAssemblies = 100_000
	// AssemblerRetryDelay is the wait before a failed assembly is tried again.
	AssemblerRetryDelay = 5 * time.Second
)

// SequenceID derives a 32-hex-character trace ID from a trace's place in
// its queue, for a trace published without one, so it keeps the same ID
// however often it is redelivered.
func SequenceID(queue string, sequence uint64) string {
	sum := sha256.Sum256([]byte(queue + ":" + strconv.FormatUint(sequence, 10)))
	return hex.EncodeToString(sum[:16])
}
