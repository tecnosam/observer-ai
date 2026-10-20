// Package memq is the export.AbstractQueue in process: fast, but what is
// queued is lost if the engine stops before sending it. The conformance
// suite in package export checks it keeps natsq's rules.
package memq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// Factory builds in-process export queues.
type Factory struct{}

func (Factory) Queue(_ context.Context, cfg export.QueueConfig) (export.AbstractQueue, error) {
	return newMemoryQueue(cfg.WithDefaults()), nil
}

// memoryQueue is a bounded channel drained by a fixed number of workers.
// Size bounds every record not yet sent, retries included.
type memoryQueue struct {
	cfg export.QueueConfig

	mu      sync.RWMutex // guards closed against Enqueue racing Shutdown
	closed  bool
	jobs    chan *memoryJob // never closed: retry timers may still send
	pool    *common.Pool
	pending sync.WaitGroup // records queued, waiting to retry, or being sent
	depth   atomic.Int64   // the same count, readable
}

type memoryJob struct {
	rec      pipeline.Record
	attempts int
}

func newMemoryQueue(cfg export.QueueConfig) *memoryQueue {
	q := &memoryQueue{cfg: cfg, jobs: make(chan *memoryJob, cfg.Size), pool: common.NewPool()}
	q.pool.Start(cfg.Concurrency, q.work)
	return q
}

func (q *memoryQueue) Enqueue(_ context.Context, rec pipeline.Record) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return fault.Unavailable(fault.Export, "queue trace for export", export.ErrShutDown)
	}
	// The bound counts every record not yet sent, as JetStream's does:
	// queued, being sent, or waiting to be retried.
	if !q.reserve() {
		return fault.Full(fault.Export, "queue trace for export, skipped it", fault.Msg("queue_size (%d) reached", q.cfg.Size))
	}
	q.jobs <- &memoryJob{rec: rec} // never blocks: the channel holds Size, and depth <= Size
	return nil
}

func (q *memoryQueue) work() {
	for {
		select {
		case <-q.pool.Stopped():
			return
		case job := <-q.jobs:
			q.send(job)
		}
	}
}

func (q *memoryQueue) send(job *memoryJob) {
	job.attempts++
	err := q.cfg.Send(context.Background(), job.rec)
	result, delay := export.Settle(q.cfg, job.rec, job.attempts, err)
	if result == export.GivenUp {
		q.cfg.Reporter.Report(context.Background(), export.SendFault(q.cfg, job.rec, job.attempts, "export trace, gave up", err).AsSevere())
	}
	if result != export.Retried {
		q.add(-1)
		return
	}
	time.AfterFunc(delay, func() {
		select {
		case q.jobs <- job:
		case <-q.pool.Stopped():
			q.add(-1) // shut down before the retry was due
		}
	})
}

// reserve takes a place in the queue if one is free, atomically, so
// concurrent Enqueues never go past Size.
func (q *memoryQueue) reserve() bool {
	for {
		n := q.depth.Load()
		if n >= int64(q.cfg.Size) {
			return false
		}
		if q.depth.CompareAndSwap(n, n+1) {
			q.pending.Add(1)
			return true
		}
	}
}

// add moves the pending count and its readable copy together.
func (q *memoryQueue) add(n int) {
	q.depth.Add(int64(n))
	q.pending.Add(n)
}

func (q *memoryQueue) Depth(context.Context) (int64, error) { return q.depth.Load(), nil }

func (q *memoryQueue) Flush(ctx context.Context) error {
	return common.Wait(ctx, &q.pending)
}

func (q *memoryQueue) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.mu.Unlock()
	if alreadyClosed {
		return nil
	}
	err := common.Wait(ctx, &q.pending)
	_ = q.pool.Stop(context.Background()) // the sends in flight end on their own timeout
	return err
}
