package pipeline

import (
	"context"

	"github.com/observer-ai/engine/internal/trace"
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

// Metrics counts what the pipeline does.
type Metrics interface {
	TraceScored(Score)
	TraceDecided(Decision)
}
