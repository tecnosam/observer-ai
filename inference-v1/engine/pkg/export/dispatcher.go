package export

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/metrics"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// DispatcherConfig is what a Dispatcher is built from.
type DispatcherConfig struct {
	Backends []BackendConfig
	Factory  AbstractBackendFactory
	// Queues builds each back end's queue: in process, or on NATS
	// JetStream. Defaults to MemoryQueueFactory.
	Queues   AbstractQueueFactory
	Reporter observe.Reporter // defaults to observe.Discard
	Metrics  SendMetrics      // defaults to recording nothing
}

// SendMetrics records each send to a back end, for latency and error rates.
type SendMetrics interface {
	SendFinished(ctx context.Context, s metrics.Send)
}

// Dispatcher implements pipeline.Exporter over any number of back ends.
// Export queues a kept trace for every back end and returns at once; each
// back end's queue feeds it from its own workers. A back end whose queue is
// full skips the trace and the skip is reported, so one slow back end never
// holds up scoring or the others.
type Dispatcher struct {
	routes   []route
	reporter observe.Reporter
}

// route is one back end and the queue that feeds it.
type route struct {
	name    string
	backend AbstractBackend
	queue   AbstractQueue
}

// NewDispatcher builds every configured back end and its queue. If one
// cannot be built, those already built are shut down.
func NewDispatcher(ctx context.Context, cfg DispatcherConfig) (*Dispatcher, error) {
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	if cfg.Queues == nil {
		cfg.Queues = MemoryQueueFactory{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.Noop{}
	}
	d := &Dispatcher{reporter: cfg.Reporter}
	for _, bc := range cfg.Backends {
		r, err := buildRoute(ctx, cfg, bc)
		if err != nil {
			_ = d.Shutdown(context.Background())
			return nil, fault.Invalid(fault.Export, "build exporter", err).With("exporter", bc.Name)
		}
		d.routes = append(d.routes, r)
	}
	return d, nil
}

func buildRoute(ctx context.Context, cfg DispatcherConfig, bc BackendConfig) (route, error) {
	backend, err := cfg.Factory.Build(ctx, bc)
	if err != nil {
		return route{}, err
	}
	q, err := cfg.Queues.Queue(ctx, QueueConfig{
		Name:        bc.Name,
		Size:        bc.QueueSize,
		Concurrency: bc.Concurrency,
		MaxDeliver:  bc.MaxDeliver,
		Backoff:     bc.Backoff,
		Send:        timed(bc.Name, backend.Send, cfg.Metrics),
		Reporter:    cfg.Reporter,
	})
	if err != nil {
		_ = backend.Shutdown(ctx)
		return route{}, err
	}
	return route{name: bc.Name, backend: backend, queue: q}, nil
}

// timed records every call to send.
func timed(exporter string, send SendFunc, m SendMetrics) SendFunc {
	return func(ctx context.Context, rec pipeline.Record) error {
		start := time.Now()
		err := send(ctx, rec)
		m.SendFinished(ctx, metrics.Send{Exporter: exporter, Duration: time.Since(start), Err: err})
		return err
	}
}

// Export queues a kept trace for every back end. It reports whether at
// least one took it; dropped traces are sent nowhere.
func (d *Dispatcher) Export(ctx context.Context, rec pipeline.Record) bool {
	if !rec.Decision.Keep {
		return false
	}
	accepted := false
	for _, r := range d.routes {
		if err := r.queue.Enqueue(ctx, rec); err != nil {
			d.reporter.Report(ctx, r.fault("queue trace for export, skipped it", err).WithTrace(rec.Trace.TraceID))
			continue
		}
		accepted = true
	}
	return accepted
}

// Flush waits until every queued trace has been sent or given up, then
// flushes every back end.
func (d *Dispatcher) Flush(ctx context.Context) error {
	return d.each(func(r route) error {
		if err := r.queue.Flush(ctx); err != nil {
			return r.fault("wait for queued exports", err)
		}
		return r.wrap("flush", r.backend.Flush(ctx))
	})
}

// Shutdown stops taking traces, sends what is queued within ctx, and shuts
// every back end down.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	return d.each(func(r route) error {
		return errors.Join(r.wrap("finish queued exports", r.queue.Shutdown(ctx)), r.wrap("shut down", r.backend.Shutdown(ctx)))
	})
}

// each runs fn for every route at once, so one slow back end does not use
// up the others' time.
func (d *Dispatcher) each(fn func(route) error) error {
	errs := make([]error, len(d.routes))
	var wg sync.WaitGroup
	for i, r := range d.routes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(r)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (r route) fault(op fault.Op, err error) *fault.Error {
	return fault.New(fault.KindOf(err), fault.Export, op, err).With("exporter", r.name)
}

func (r route) wrap(op fault.Op, err error) error {
	if err == nil {
		return nil
	}
	return r.fault(op, err)
}
