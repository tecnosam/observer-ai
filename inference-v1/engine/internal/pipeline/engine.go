package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/observe"
	"github.com/observer-ai/engine/internal/trace"
)

// Deps are the parts the Engine is given.
type Deps struct {
	Scorer   Scorer
	Policy   Policy
	Exporter Exporter
	Reporter observe.Reporter // defaults to observe.Discard
	Metrics  Metrics          // defaults to recording nothing
	Logger   *slog.Logger     // decision log lines; defaults to slog.Default()
}

// Settings tune how the Engine treats a score.
type Settings struct {
	// Temperature calibrates the raw probability (1 = unchanged).
	Temperature Temperature
	// Model names the scorer in fallback decisions, when it reported nothing.
	Model ModelName
}

// Engine runs one trace through slice, score, calibrate, decide and export.
type Engine struct {
	deps     Deps
	settings Settings
}

func NewEngine(deps Deps, settings Settings) *Engine {
	if deps.Reporter == nil {
		deps.Reporter = observe.Discard
	}
	if deps.Metrics == nil {
		deps.Metrics = noMetrics{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Engine{deps: deps, settings: settings}
}

// Process decides one trace on the synchronous path. It always decides: if
// the model cannot score the trace, the policy's fallback does.
func (e *Engine) Process(ctx context.Context, t trace.Trace) Outcome {
	out, _ := e.run(ctx, t, false)
	return out
}

// ProcessQueued decides one trace taken from the queue. A system error (the
// model down, timing out, answering garbage) is returned without deciding or
// exporting, so the queue can redeliver the trace and eventually
// dead-letter it. A trace the model rejects still gets the fallback, because
// retrying it cannot help.
func (e *Engine) ProcessQueued(ctx context.Context, t trace.Trace) (Outcome, error) {
	return e.run(ctx, t, true)
}

// Ready reports whether the scorer can be reached.
func (e *Engine) Ready(ctx context.Context) error {
	return e.deps.Scorer.Ready(ctx)
}

// Flush pushes any buffered exports out now.
func (e *Engine) Flush(ctx context.Context) error {
	return e.deps.Exporter.Flush(ctx)
}

func (e *Engine) run(ctx context.Context, t trace.Trace, deferSystemErrors bool) (Outcome, error) {
	start := time.Now()
	record, err := e.decide(ctx, t, start, deferSystemErrors)
	if err != nil {
		return Outcome{}, err
	}
	out := Outcome{Record: record, Exported: e.deps.Exporter.Export(ctx, record), Duration: time.Since(start)}
	e.deps.Metrics.TraceDecided(record.Decision)
	e.logDecision(out)
	return out, nil
}

// decide scores the trace and applies the policy, or falls back when the
// model gave no usable answer.
func (e *Engine) decide(ctx context.Context, t trace.Trace, start time.Time, deferSystemErrors bool) (Record, error) {
	score, err := e.deps.Scorer.Score(ctx, trace.BuildState(t))
	switch {
	case err == nil:
		e.deps.Metrics.TraceScored(score)
		return e.scored(t, score), nil
	case deferSystemErrors && isSystemError(err):
		return Record{}, err
	default:
		return e.fallback(ctx, t, err, time.Since(start)), nil
	}
}

func (e *Engine) scored(t trace.Trace, score Score) Record {
	calibrated := Calibrate(score.Raw, e.settings.Temperature)
	return Record{Trace: t, Decision: e.deps.Policy.Decide(score.Raw, calibrated), Model: score.Model, Latency: score.Latency}
}

func (e *Engine) fallback(ctx context.Context, t trace.Trace, cause error, elapsed time.Duration) Record {
	e.deps.Reporter.Report(ctx, fault.New(fault.KindOf(cause), fault.Pipeline, "score trace, falling back to a random decision", cause).
		WithTrace(t.TraceID))
	return Record{Trace: t, Decision: e.deps.Policy.Fallback(), Model: e.settings.Model, Latency: elapsed}
}

// isSystemError reports whether a scoring failure is the system's fault, so
// trying the same trace again later can help. Only a request the model
// refused, or one that was invalid, is not.
func isSystemError(err error) bool {
	switch fault.KindOf(err) {
	case fault.KindRejected, fault.KindInvalid:
		return false
	}
	return true
}

// logDecision writes one line per decision.
func (e *Engine) logDecision(out Outcome) {
	rec, d := out.Record, out.Record.Decision
	attrs := []any{
		"trace_id", rec.Trace.TraceID,
		"decision", d.Label(),
		"reason", d.Reason,
		"severity", d.Severity,
		"model", rec.Model,
		"latency_ms", Milliseconds(rec.Latency),
		"duration_ms", Milliseconds(out.Duration),
		"exported", out.Exported,
	}
	if d.Scored {
		attrs = append(attrs, "p_keep", float64(d.PKeep), "p_keep_raw", float64(d.PKeepRaw))
	}
	e.deps.Logger.Info("decision", attrs...)
}

// Label is the decision as a word: "keep" or "drop".
func (d Decision) Label() string {
	if d.Keep {
		return "keep"
	}
	return "drop"
}

// Milliseconds is a duration in fractional milliseconds, as the API and logs report it.
func Milliseconds(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

type noMetrics struct{}

func (noMetrics) TraceScored(Score)     {}
func (noMetrics) TraceDecided(Decision) {}
