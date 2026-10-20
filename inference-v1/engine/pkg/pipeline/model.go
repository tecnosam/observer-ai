package pipeline

import (
	"context"
	"fmt"

	"github.com/observer-ai/engine/pkg/trace"
)

// Model is one fine-tuned decision model: the scorer that asks it, and how
// its raw answers are calibrated.
type Model struct {
	// Name labels the model in fallback decisions, when it reported nothing.
	Name        ModelName
	Scorer      Scorer
	Temperature Temperature
	// Agent is the label metrics and logs give the traces this model
	// scored; Route sets it.
	Agent AgentLabel
}

// AgentLabel is a trace's agent as metrics and logs show it: the agent's
// name when it has a model of its own, else AgentOther or AgentNone. Unlike
// the name a trace carries, which any client can set to anything, its
// values are bounded by the config, so it is safe as a metric attribute.
type AgentLabel string

const (
	AgentNone  AgentLabel = "none"  // the trace named no agent
	AgentOther AgentLabel = "other" // it named one with no model of its own
)

// ModelRouter picks the model that scores a trace. The engine uses one
// model per agent, each fine-tuned on that agent's traces.
type ModelRouter interface {
	// Route returns the model for agent, labelled; an unknown or empty
	// agent gets the default model.
	Route(agent trace.AgentName) Model
	// Ready reports whether the default model can be reached. An agent's
	// model being down affects only that agent's traces, so it does not
	// make the engine unready.
	Ready(ctx context.Context) error
}

// AgentRouter routes by agent name, with a default for every other agent.
type AgentRouter struct {
	fallback Model
	byAgent  map[trace.AgentName]Model
}

// NewAgentRouter routes the agents in byAgent to their models and every
// other trace to def. byAgent may be nil: one model for everything.
func NewAgentRouter(def Model, byAgent map[trace.AgentName]Model) *AgentRouter {
	return &AgentRouter{fallback: def, byAgent: byAgent}
}

// SingleModel scores every trace with m.
func SingleModel(m Model) *AgentRouter { return NewAgentRouter(m, nil) }

func (r *AgentRouter) Route(agent trace.AgentName) Model {
	if m, ok := r.byAgent[agent]; ok {
		m.Agent = AgentLabel(agent)
		return m
	}
	m := r.fallback
	m.Agent = AgentOther
	if agent == "" {
		m.Agent = AgentNone
	}
	return m
}

// Ready probes the default model only.
func (r *AgentRouter) Ready(ctx context.Context) error {
	if err := r.fallback.Scorer.Ready(ctx); err != nil {
		return fmt.Errorf("model %s (default): %w", r.fallback.Name, err)
	}
	return nil
}
