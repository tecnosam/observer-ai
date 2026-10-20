package pipeline

import (
	"context"
	"time"

	"github.com/observer-ai/engine/pkg/trace"
)

// Scorer asks a decision model about one trace. Errors are *fault.Error: a
// rejected request is fault.KindRejected; a model that is down, slow or
// answering garbage is fault.KindUnavailable or fault.KindTimeout.
type Scorer interface {
	Score(ctx context.Context, state trace.State) (Score, error)
	Ready(ctx context.Context) error
}

// Policy turns a calibrated probability into a keep or drop decision.
type Policy interface {
	Decide(raw, calibrated Probability) Decision
	// Fallback decides for a trace the model could not score.
	Fallback() Decision
}

// Exporter ships kept traces. Export reports whether it handed the trace on.
type Exporter interface {
	Export(ctx context.Context, rec Record) bool
	Flush(ctx context.Context) error
}

// Publisher puts a validated trace on the work queue. A full queue is
// fault.KindFull; one that cannot be reached is fault.KindUnavailable.
type Publisher interface {
	Publish(ctx context.Context, t trace.Trace) error
	Ready() error
}

// Metrics records what the pipeline does, for dashboards and alerts.
type Metrics interface {
	// ScoreFinished records one call to a model, successful or not.
	ScoreFinished(ctx context.Context, r ScoreResult)
	// TraceDecided records a decision, kept or dropped.
	TraceDecided(ctx context.Context, rec Record)
}

// ScoreResult is one call to a model as Metrics sees it.
type ScoreResult struct {
	Agent    AgentLabel
	Model    ModelName
	Duration time.Duration // the whole call, retries included
	Err      error         // nil on success
}
