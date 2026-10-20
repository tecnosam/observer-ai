package export

import (
	"context"
	"errors"
	"time"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// DefaultMaxDeliver is how many times a record is tried by default.
const DefaultMaxDeliver = 3

// ErrShutDown is returned by a queue's Enqueue after Shutdown.
var ErrShutDown = errors.New("export queue is shut down")

// DefaultBackoff is the wait before each retry of a failed send; the last
// value repeats.
var DefaultBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

// SendFunc delivers one record to a back end.
type SendFunc func(ctx context.Context, rec pipeline.Record) error

// QueueConfig is one back end's queue.
type QueueConfig struct {
	Name        string // the back end's name; names its stream on NATS
	Size        int    // records waiting; past it Enqueue is refused
	Concurrency int    // records sent at once
	// MaxDeliver is how many times a record is tried before it is given
	// up (dead-lettered on NATS). Errors that a retry cannot fix are given
	// up at once.
	MaxDeliver int
	Backoff    []time.Duration
	Send       SendFunc
	Reporter   observe.Reporter
}

// AbstractQueue holds the records waiting for one back end and feeds them to
// its Send. A Dispatcher has one per back end.
type AbstractQueue interface {
	// Enqueue queues rec. A full queue is fault.KindFull.
	Enqueue(ctx context.Context, rec pipeline.Record) error
	// Flush waits until every queued record has been sent or given up.
	Flush(ctx context.Context) error
	// Shutdown stops taking records, sends what is queued within ctx, and
	// stops the workers.
	Shutdown(ctx context.Context) error
	// Depth is how many records wait to be sent, retries included.
	Depth(ctx context.Context) (int64, error)
}

// AbstractQueueFactory builds the queue for each back end.
type AbstractQueueFactory interface {
	Queue(ctx context.Context, cfg QueueConfig) (AbstractQueue, error)
}

// WithDefaults fills in every setting left zero.
func (cfg QueueConfig) WithDefaults() QueueConfig {
	cfg.Size = common.OrDefault(cfg.Size, DefaultQueueSize)
	cfg.Concurrency = common.OrDefault(cfg.Concurrency, DefaultConcurrency)
	cfg.MaxDeliver = common.OrDefault(cfg.MaxDeliver, DefaultMaxDeliver)
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	return cfg
}

// RetryDelay is the wait before the next try, after attempts tries.
func (cfg QueueConfig) RetryDelay(attempts int) time.Duration {
	return cfg.retry().Delay(attempts)
}

func (cfg QueueConfig) retry() common.RetryPolicy {
	return common.RetryPolicy{MaxDeliver: cfg.MaxDeliver, Backoff: cfg.Backoff}
}

// Outcome is what happens to a record after one send.
type Outcome int

const (
	Sent    Outcome = iota
	Retried         // sent again after a delay
	GivenUp         // a retry cannot help, or max_deliver is reached
)

// Settle decides the outcome of a send and reports a retry. Every
// AbstractQueue uses it, so they retry alike. The caller reports a give-up,
// since what it does with the record differs.
func Settle(cfg QueueConfig, rec pipeline.Record, attempts int, err error) (Outcome, time.Duration) {
	switch {
	case err == nil:
		return Sent, 0
	case !fault.Retryable(err) || cfg.retry().Exhausted(attempts):
		return GivenUp, 0
	}
	delay := cfg.retry().Delay(attempts)
	cfg.Reporter.Report(context.Background(), SendFault(cfg, rec, attempts, "export trace, will retry", err).With("retry_in", delay.String()))
	return Retried, delay
}

// SendFault describes a failed send of rec to the back end.
func SendFault(cfg QueueConfig, rec pipeline.Record, attempts int, op fault.Op, err error) *fault.Error {
	return fault.New(fault.KindOf(err), fault.Export, op, err).WithTrace(rec.Trace.TraceID).
		With("exporter", cfg.Name).With("delivery", attempts).With("max_deliver", cfg.MaxDeliver)
}
