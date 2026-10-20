package scorer

import (
	"fmt"

	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

// AbstractScorerFactory builds the client for one model. A model server
// that does not speak /v1/systemone is a new factory.
type AbstractScorerFactory interface {
	Build(cfg Config) (pipeline.Scorer, error)
}

// HTTPFactory builds clients for servers speaking /v1/systemone: Kev, and
// TypeSafe's hosted Jev.
type HTTPFactory struct{}

func (HTTPFactory) Build(cfg Config) (pipeline.Scorer, error) { return New(cfg) }

// ModelConfig is one model to build: its client and its calibration.
type ModelConfig struct {
	Client      Config
	Temperature pipeline.Temperature
}

// Models is every model the engine routes between: the default, and one per
// agent that has its own.
type Models struct {
	Default ModelConfig
	ByAgent map[trace.AgentName]ModelConfig
}

// BuildRouter builds every model with f and routes each agent to its own.
func BuildRouter(f AbstractScorerFactory, ms Models) (*pipeline.AgentRouter, error) {
	def, err := buildModel(f, ms.Default)
	if err != nil {
		return nil, fmt.Errorf("default model: %w", err)
	}
	byAgent := make(map[trace.AgentName]pipeline.Model, len(ms.ByAgent))
	for agent, mc := range ms.ByAgent {
		if byAgent[agent], err = buildModel(f, mc); err != nil {
			return nil, fmt.Errorf("model for agent %s: %w", agent, err)
		}
	}
	return pipeline.NewAgentRouter(def, byAgent), nil
}

func buildModel(f AbstractScorerFactory, mc ModelConfig) (pipeline.Model, error) {
	s, err := f.Build(mc.Client)
	if err != nil {
		return pipeline.Model{}, err
	}
	return pipeline.Model{Name: pipeline.ModelName(mc.Client.Model), Scorer: s, Temperature: mc.Temperature}, nil
}
