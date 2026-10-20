package common

import (
	"context"
	"sync"
)

// Pool is a set of goroutines that stop together. Work runs under WorkCtx,
// which is cancelled only when Stop runs out of time, so work in flight can
// finish on a normal stop.
type Pool struct {
	stop       chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
	workCtx    context.Context
	cancelWork context.CancelFunc
}

func NewPool() *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{stop: make(chan struct{}), workCtx: ctx, cancelWork: cancel}
}

// Start runs n copies of run. Each should return once Stopped is closed.
func (p *Pool) Start(n int, run func()) {
	for range n {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			run()
		}()
	}
}

// Stopped is closed when Stop is called.
func (p *Pool) Stopped() <-chan struct{} { return p.stop }

// Stopping reports whether Stop has been called.
func (p *Pool) Stopping() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}

// WorkCtx is the context work should run under.
func (p *Pool) WorkCtx() context.Context { return p.workCtx }

// Stop stops taking work and waits for the work in flight. If ctx ends
// first, that work is cancelled and Stop returns ctx's error once it has
// returned.
func (p *Pool) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.stop) })
	err := Wait(ctx, &p.wg)
	if err != nil {
		p.cancelWork()
		p.wg.Wait()
	}
	p.cancelWork()
	return err
}
