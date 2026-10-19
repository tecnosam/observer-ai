// Package metrics holds the engine's metrics recorders. Noop is the default;
// an OpenTelemetry recorder comes with the metrics phase.
package metrics

import (
	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/pipeline"
)

// Noop records nothing. It satisfies pipeline.Metrics and observe.ErrorCounter.
type Noop struct{}

func (Noop) TraceScored(pipeline.Score)                {}
func (Noop) TraceDecided(pipeline.Decision)            {}
func (Noop) ErrorReported(fault.Kind, fault.Component) {}
