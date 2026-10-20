package export_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/export/memq"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

// fakeBackend records what it is sent. A non-nil gate holds every Send until
// it is closed; started receives a value as each Send begins.
type fakeBackend struct {
	gate    chan struct{}
	started chan struct{}
	err     error

	mu       sync.Mutex
	sent     []string
	flushed  bool
	shutdown bool
}

func (b *fakeBackend) Send(_ context.Context, rec pipeline.Record) error {
	if b.started != nil {
		b.started <- struct{}{}
	}
	if b.gate != nil {
		<-b.gate
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, rec.Trace.TraceID)
	return b.err
}

func (b *fakeBackend) Flush(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushed = true
	return nil
}

func (b *fakeBackend) Shutdown(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shutdown = true
	return nil
}

func (b *fakeBackend) state() (sent []string, flushed, shutdown bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.sent...), b.flushed, b.shutdown
}

// fakeFactory hands out prebuilt backends by name, or fails for a name it
// does not know.
type fakeFactory map[string]*fakeBackend

func (f fakeFactory) Build(_ context.Context, cfg export.BackendConfig) (export.AbstractBackend, error) {
	if b, ok := f[cfg.Name]; ok {
		return b, nil
	}
	return nil, errors.New("unknown back end")
}

func newDispatcher(t *testing.T, factory fakeFactory, reporter *recordingReporter, backends ...export.BackendConfig) *export.Dispatcher {
	t.Helper()
	cfg := export.DispatcherConfig{Backends: backends, Factory: factory, Queues: memq.Factory{}}
	if reporter != nil {
		cfg.Reporter = reporter
	}
	d, err := export.NewDispatcher(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	return d
}

func TestDispatcherSendsToEveryBackend(t *testing.T) {
	a, b := &fakeBackend{}, &fakeBackend{}
	d := newDispatcher(t, fakeFactory{"a": a, "b": b}, nil, export.BackendConfig{Name: "a"}, export.BackendConfig{Name: "b"})

	for _, id := range []string{"t1", "t2"} {
		if !d.Export(context.Background(), kept(id)) {
			t.Fatalf("Export(%s) returned false", id)
		}
	}
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, backend := range map[string]*fakeBackend{"a": a, "b": b} {
		sent, flushed, _ := backend.state()
		if len(sent) != 2 || !flushed {
			t.Errorf("%s: sent %v, flushed %v; want both traces, flushed", name, sent, flushed)
		}
	}
}

func TestDispatcherSendsDroppedTracesNowhere(t *testing.T) {
	a := &fakeBackend{}
	d := newDispatcher(t, fakeFactory{"a": a}, nil, export.BackendConfig{Name: "a"})
	for _, dec := range []pipeline.Decision{
		{Scored: true, PKeep: 0.1, PKeepRaw: 0.12, Reason: pipeline.ReasonBelowThreshold, Severity: pipeline.SeverityNone},
		{Reason: pipeline.ReasonFallback, Severity: pipeline.SeverityNone},
	} {
		if d.Export(context.Background(), pipeline.Record{Trace: trace.Trace{TraceID: "x"}, Decision: dec}) {
			t.Errorf("Export returned true for a dropped trace (%s)", dec.Reason)
		}
	}
	_ = d.Flush(context.Background())
	if sent, _, _ := a.state(); len(sent) != 0 {
		t.Errorf("dropped traces sent: %v", sent)
	}
}

// Export returns before the back end has the trace: a slow back end does not
// hold up the caller.
func TestDispatcherDoesNotWaitForTheBackend(t *testing.T) {
	slow := &fakeBackend{gate: make(chan struct{})}
	d := newDispatcher(t, fakeFactory{"slow": slow}, nil, export.BackendConfig{Name: "slow"})

	done := make(chan bool)
	go func() { done <- d.Export(context.Background(), kept("t1")) }()
	select {
	case ok := <-done:
		if !ok {
			t.Error("Export returned false")
		}
	case <-time.After(time.Second):
		t.Fatal("Export waited for the back end")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Flush with a send in flight = %v, want the deadline", err)
	}
	close(slow.gate)
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent, _, _ := slow.state(); len(sent) != 1 {
		t.Errorf("sent %v, want t1", sent)
	}
}

// A full queue skips the trace for that back end only, and reports it. The
// bound counts the trace being sent, as JetStream's does.
func TestDispatcherFullQueueSkipsAndReports(t *testing.T) {
	stuck := &fakeBackend{gate: make(chan struct{}), started: make(chan struct{}, 1)}
	fine := &fakeBackend{}
	reported := &recordingReporter{}
	d := newDispatcher(t, fakeFactory{"stuck": stuck, "fine": fine}, reported,
		export.BackendConfig{Name: "stuck", QueueSize: 1, Concurrency: 1}, export.BackendConfig{Name: "fine"})
	released := false
	release := func() {
		if !released {
			released = true
			close(stuck.gate)
		}
	}
	defer release()

	d.Export(context.Background(), kept("t1")) // being sent: fills the queue of 1
	<-stuck.started
	if !d.Export(context.Background(), kept("t2")) {
		t.Error("Export returned false although one back end took the trace")
	}

	errs := reported.all()
	if len(errs) != 1 || fault.KindOf(errs[0]) != fault.KindFull {
		t.Fatalf("reported %v, want one full-queue fault", errs)
	}
	if f, _ := fault.As(errs[0]); f.Attrs["exporter"] != "stuck" || f.TraceID != "t2" {
		t.Errorf("fault = %+v, want exporter stuck and trace t2", f)
	}

	release()
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent, _, _ := stuck.state(); len(sent) != 1 {
		t.Errorf("stuck back end sent %v, want t1 only", sent)
	}
	if sent, _, _ := fine.state(); len(sent) != 2 {
		t.Errorf("other back end sent %v, want both", sent)
	}
}

// A failed send is retried with backoff, then given up and reported.
func TestDispatcherRetriesThenGivesUp(t *testing.T) {
	failing := &fakeBackend{err: fault.Unavailable(fault.Export, "forward spans", errors.New("connection refused"))}
	reported := &recordingReporter{}
	d := newDispatcher(t, fakeFactory{"langfuse": failing}, reported,
		export.BackendConfig{Name: "langfuse", MaxDeliver: 3, Backoff: []time.Duration{time.Millisecond}})

	d.Export(context.Background(), kept("t1"))
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent, _, _ := failing.state(); len(sent) != 3 {
		t.Errorf("tried %d times, want max_deliver (3)", len(sent))
	}
	errs := reported.all()
	if len(errs) != 3 {
		t.Fatalf("reported %v, want two retries and a give-up", errs)
	}
	last, _ := fault.As(errs[2])
	if last.Op != "export trace, gave up" || !last.Severe || last.Attrs["exporter"] != "langfuse" || last.TraceID != "t1" {
		t.Errorf("give-up = %+v", last)
	}
	if first, _ := fault.As(errs[0]); first.Op != "export trace, will retry" || first.Severe {
		t.Errorf("retry = %+v", first)
	}
}

func TestDispatcherRetrySucceeds(t *testing.T) {
	flaky := &flakyBackend{fails: 1}
	d, err := export.NewDispatcher(context.Background(), export.DispatcherConfig{
		Backends: []export.BackendConfig{{Name: "a", Backoff: []time.Duration{time.Millisecond}}},
		Factory:  fixedFactory{flaky},
		Queues:   memq.Factory{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	d.Export(context.Background(), kept("t1"))
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := flaky.calls.Load(); n != 2 {
		t.Errorf("%d sends, want a failure then a success", n)
	}
}

// An error a retry cannot fix is given up at once.
func TestDispatcherGivesUpOnInvalid(t *testing.T) {
	bad := &fakeBackend{err: fault.Invalid(fault.Export, "annotate stored spans", errors.New("corrupt"))}
	reported := &recordingReporter{}
	d := newDispatcher(t, fakeFactory{"a": bad}, reported, export.BackendConfig{Name: "a"})
	d.Export(context.Background(), kept("t1"))
	if err := d.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent, _, _ := bad.state(); len(sent) != 1 {
		t.Errorf("tried %d times, want once", len(sent))
	}
	if errs := reported.all(); len(errs) != 1 || fault.KindOf(errs[0]) != fault.KindInvalid {
		t.Errorf("reported %v", errs)
	}
}

// flakyBackend fails its first fails sends.
type flakyBackend struct {
	fails int32
	calls atomic.Int32
}

func (b *flakyBackend) Send(context.Context, pipeline.Record) error {
	if b.calls.Add(1) <= b.fails {
		return fault.Unavailable(fault.Export, "forward spans", errors.New("503"))
	}
	return nil
}
func (*flakyBackend) Flush(context.Context) error    { return nil }
func (*flakyBackend) Shutdown(context.Context) error { return nil }

// fixedFactory hands out one prebuilt back end.
type fixedFactory struct{ backend export.AbstractBackend }

func (f fixedFactory) Build(context.Context, export.BackendConfig) (export.AbstractBackend, error) {
	return f.backend, nil
}

// Shutdown sends what is queued, shuts every back end down, and takes no
// more traces.
func TestDispatcherShutdown(t *testing.T) {
	a := &fakeBackend{}
	d := newDispatcher(t, fakeFactory{"a": a}, nil, export.BackendConfig{Name: "a"})
	d.Export(context.Background(), kept("t1"))
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent, _, shutdown := a.state(); len(sent) != 1 || !shutdown {
		t.Errorf("sent %v, shut down %v", sent, shutdown)
	}
	if d.Export(context.Background(), kept("t2")) {
		t.Error("Export after Shutdown returned true")
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}

// A back end that cannot be built fails start-up, and the ones already built
// are shut down.
func TestDispatcherBuildFailure(t *testing.T) {
	a := &fakeBackend{}
	_, err := export.NewDispatcher(context.Background(), export.DispatcherConfig{
		Backends: []export.BackendConfig{{Name: "a"}, {Name: "missing"}},
		Factory:  fakeFactory{"a": a},
		Queues:   memq.Factory{},
	})
	if fault.KindOf(err) != fault.KindInvalid {
		t.Fatalf("error = %v, want an invalid fault", err)
	}
	if f, _ := fault.As(err); f.Attrs["exporter"] != "missing" {
		t.Errorf("fault = %+v, want exporter missing", f)
	}
	if _, _, shutdown := a.state(); !shutdown {
		t.Error("the back end already built was not shut down")
	}
}

func TestDispatcherWithNoBackends(t *testing.T) {
	d := newDispatcher(t, fakeFactory{}, nil)
	if d.Export(context.Background(), kept("t1")) {
		t.Error("Export returned true with nowhere to send")
	}
	if err := d.Flush(context.Background()); err != nil {
		t.Error(err)
	}
}
