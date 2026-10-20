package metrics

import (
	"context"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// Noop records nothing. It satisfies every metrics hook.
type Noop struct{}

func (Noop) RequestFinished(context.Context, Request)            {}
func (Noop) ScoreFinished(context.Context, pipeline.ScoreResult) {}
func (Noop) TraceDecided(context.Context, pipeline.Record)       {}
func (Noop) SendFinished(context.Context, Send)                  {}
func (Noop) ErrorReported(fault.Kind, fault.Component)           {}
