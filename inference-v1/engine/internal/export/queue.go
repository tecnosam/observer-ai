package export

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/observe"
	"github.com/observer-ai/engine/internal/pipeline"
)

// DefaultMaxDeliver is how many times a record is tried by default.
const DefaultMaxDeliver = 3

var errShutDown = errors.New("export queue is shut down")

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
}

// AbstractQueueFactory builds the queue for each back end.
type AbstractQueueFactory interface {
	Queue(ctx context.Context, cfg QueueConfig) (AbstractQueue, error)
}

// MemoryQueueFactory keeps export queues in process: fast, but what is
// queued is lost if the engine stops before sending it.
type MemoryQueueFactory struct{}

func (MemoryQueueFactory) Queue(_ context.Context, cfg QueueConfig) (AbstractQueue, error) {
	return newMemoryQueue(withQueueDefaults(cfg)), nil
}

func withQueueDefaults(cfg QueueConfig) QueueConfig {
	cfg.Size = orDefault(cfg.Size, DefaultQueueSize)
	cfg.Concurrency = orDefault(cfg.Concurrency, DefaultConcurrency)
	cfg.MaxDeliver = orDefault(cfg.MaxDeliver, DefaultMaxDeliver)
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	return cfg
}

// memoryQueue is a bounded channel drained by a fixed number of workers.
// A failed send is retried after its backoff, outside the bound.
type memoryQueue struct {
	cfg QueueConfig

	mu      sync.RWMutex // guards closed against Enqueue racing Shutdown
	closed  bool
	jobs    chan *memoryJob // never closed: retry timers may still send
	stop    chan struct{}
	pending sync.WaitGroup // records queued, waiting to retry, or being sent
	workers sync.WaitGroup
}

type memoryJob struct {
	rec      pipeline.Record
	attempts int
}

func newMemoryQueue(cfg QueueConfig) *memoryQueue {
	q := &memoryQueue{cfg: cfg, jobs: make(chan *memoryJob, cfg.Size), stop: make(chan struct{})}
	for range cfg.Concurrency {
		q.workers.Add(1)
		go q.work()
	}
	return q
}

func (q *memoryQueue) Enqueue(_ context.Context, rec pipeline.Record) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return fault.Unavailable(fault.Export, "queue trace for export", errShutDown)
	}
	q.pending.Add(1)
	select {
	case q.jobs <- &memoryJob{rec: rec}:
		return nil
	default:
		q.pending.Done()
		return fault.Full(fault.Export, "queue trace for export, skipped it", fault.Msg("queue_size (%d) reached", q.cfg.Size))
	}
}

func (q *memoryQueue) work() {
	defer q.workers.Done()
	for {
		select {
		case <-q.stop:
			return
		case job := <-q.jobs:
			q.send(job)
		}
	}
}

func (q *memoryQueue) send(job *memoryJob) {
	job.attempts++
	err := q.cfg.Send(context.Background(), job.rec)
	result, delay := settle(q.cfg, job.rec, job.attempts, err)
	if result == givenUp {
		q.cfg.Reporter.Report(context.Background(), sendFault(q.cfg, job.rec, job.attempts, "export trace, gave up", err).AsSevere())
	}
	if result != retried {
		q.pending.Done()
		return
	}
	time.AfterFunc(delay, func() {
		select {
		case q.jobs <- job:
		case <-q.stop:
			q.pending.Done() // shut down before the retry was due
		}
	})
}

func (q *memoryQueue) Flush(ctx context.Context) error {
	return wait(ctx, &q.pending)
}

func (q *memoryQueue) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.mu.Unlock()
	if alreadyClosed {
		return nil
	}
	err := wait(ctx, &q.pending)
	close(q.stop)
	q.workers.Wait()
	return err
}

// outcome is what happens to a record after one send.
type outcome int

const (
	sent    outcome = iota
	retried         // sent again after a delay
	givenUp         // a retry cannot help, or max_deliver is reached
)

// settle decides the outcome of a send and reports a retry. The caller
// reports a give-up, since what it does with the record differs.
func settle(cfg QueueConfig, rec pipeline.Record, attempts int, err error) (outcome, time.Duration) {
	switch {
	case err == nil:
		return sent, 0
	case !fault.Retryable(err) || attempts >= cfg.MaxDeliver:
		return givenUp, 0
	}
	delay := backoffFor(cfg.Backoff, attempts)
	cfg.Reporter.Report(context.Background(), sendFault(cfg, rec, attempts, "export trace, will retry", err).With("retry_in", delay.String()))
	return retried, delay
}

// sendFault describes a failed send of rec to the back end.
func sendFault(cfg QueueConfig, rec pipeline.Record, attempts int, op fault.Op, err error) *fault.Error {
	return fault.New(fault.KindOf(err), fault.Export, op, err).WithTrace(rec.Trace.TraceID).
		With("exporter", cfg.Name).With("delivery", attempts).With("max_deliver", cfg.MaxDeliver)
}

func backoffFor(backoff []time.Duration, attempts int) time.Duration {
	if len(backoff) == 0 {
		return 0
	}
	return backoff[min(max(attempts-1, 0), len(backoff)-1)]
}
