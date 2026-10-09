package server

import (
	"context"
	"errors"
	"sync"

	"github.com/observer-ai/engine/internal/trace"
)

var (
	errQueueFull = errors.New("queue full")
	errClosed    = errors.New("shutting down")
)

// pool is a fixed set of workers reading from a bounded queue.
type pool struct {
	queue chan trace.Trace
	done  chan struct{} // closed when every worker has exited

	mu     sync.RWMutex // guards closed and sends on queue
	closed bool
}

func newPool(workers, queueSize int, work func(trace.Trace)) *pool {
	p := &pool{
		queue: make(chan trace.Trace, queueSize),
		done:  make(chan struct{}),
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range p.queue {
				work(t)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(p.done)
	}()
	return p
}

// submit queues t without blocking.
func (p *pool) submit(t trace.Trace) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errClosed
	}
	select {
	case p.queue <- t:
		return nil
	default:
		return errQueueFull
	}
}

// close stops intake. Workers finish what is already queued.
func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
}

// wait blocks until the workers have drained the queue or ctx ends.
func (p *pool) wait(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
