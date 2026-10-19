package observe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/observer-ai/engine/internal/fault"
)

// Reporter is what components are given to report errors with.
type Reporter interface {
	Report(ctx context.Context, err error)
}

// ErrorCounter counts reported errors, for metrics.
type ErrorCounter interface {
	ErrorReported(kind fault.Kind, component fault.Component)
}

// PipelineConfig is what a Pipeline is built from.
type PipelineConfig struct {
	Sinks   []AbstractSink
	Counter ErrorCounter // optional
	// Fallback receives a line when a sink fails, so a broken sink is visible
	// without taking the engine down. Defaults to os.Stderr.
	Fallback io.Writer
	Now      func() time.Time // defaults to time.Now
}

// Pipeline is the Reporter the engine runs with: every error becomes an
// Entry, is counted, and goes to every sink.
type Pipeline struct {
	cfg PipelineConfig
}

func NewPipeline(cfg PipelineConfig) *Pipeline {
	if cfg.Fallback == nil {
		cfg.Fallback = os.Stderr
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Pipeline{cfg: cfg}
}

// BuildPipeline builds every configured sink with factory. If one cannot be
// built, the ones already built are closed.
func BuildPipeline(factory AbstractSinkFactory, sinks []SinkConfig, counter ErrorCounter) (*Pipeline, error) {
	built := make([]AbstractSink, 0, len(sinks))
	for _, cfg := range sinks {
		sink, err := factory.Build(cfg)
		if err != nil {
			closeAll(built)
			return nil, fault.Invalid(fault.Config, "build error sink", err)
		}
		built = append(built, sink)
	}
	return NewPipeline(PipelineConfig{Sinks: built, Counter: counter}), nil
}

func (p *Pipeline) Report(_ context.Context, err error) {
	if err == nil {
		return
	}
	entry := entryFor(err, p.cfg.Now())
	p.count(entry)
	for _, sink := range p.cfg.Sinks {
		p.write(sink, entry)
	}
}

func (p *Pipeline) count(e Entry) {
	if p.cfg.Counter != nil {
		p.cfg.Counter.ErrorReported(e.Kind, e.Component)
	}
}

func (p *Pipeline) write(sink AbstractSink, e Entry) {
	if err := sink.Write(e); err != nil {
		fmt.Fprintf(p.cfg.Fallback, "observe: error sink failed: %v (entry: %s %s)\n", err, e.Component, e.Op)
	}
}

// Close closes every sink.
func (p *Pipeline) Close() error {
	return closeAll(p.cfg.Sinks)
}

func closeAll(sinks []AbstractSink) error {
	var errs []error
	for _, s := range sinks {
		errs = append(errs, s.Close())
	}
	return errors.Join(errs...)
}

// Discard is a Reporter that drops everything, for tests and tools.
var Discard Reporter = discard{}

type discard struct{}

func (discard) Report(context.Context, error) {}
