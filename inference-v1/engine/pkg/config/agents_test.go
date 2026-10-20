package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/trace"
)

const supportManifest = `{
  "model": "support",
  "questions": {"answer_wrong": {"type": "noul", "instructions": "wrong?"}},
  "temperatures": {"answer_wrong": 1.4}
}`

// writeAgents writes a config with one agent model and its manifest next to it.
func writeAgents(t *testing.T, agents string) string {
	t.Helper()
	path := write(t, "scorer:\n  manifest: manifest.json\n  timeout: 2s\n"+agents, testManifest)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "support.json"), []byte(supportManifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// An agent's model is complete on its own: what it leaves out gets the
// engine's built-in defaults, never the scorer section's values.
func TestAgentModelTakesNothingFromScorer(t *testing.T) {
	cfg, err := Load(writeAgents(t, `
agents:
  models:
    support-bot:
      endpoint: http://kev-support:8010/v1/systemone
      model: kev-support
      manifest: support.json
`))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Agents.Models["support-bot"]
	if m.Endpoint != "http://kev-support:8010/v1/systemone" || m.Model != "kev-support" || m.Manifest != "support.json" {
		t.Errorf("own keys = %+v", m)
	}
	// scorer.timeout is 2s in this file; the agent's model keeps the built-in 10s.
	if def := DefaultModel(); m.Timeout != def.Timeout || m.MaxRetries != def.MaxRetries || m.DecisionQuestion != def.DecisionQuestion {
		t.Errorf("optional keys = %+v, want the built-in defaults", m)
	}
	if cfg.Scorer.Timeout.Std() != 2*time.Second {
		t.Errorf("scorer.timeout changed to %s", cfg.Scorer.Timeout.Std())
	}
	if got := cfg.AgentManifests["support-bot"]; got.Model != "support" || got.Temperature != 1.4 {
		t.Errorf("agent manifest = %+v", got)
	}

	ms := cfg.Models(nil)
	if a := ms.ByAgent[trace.AgentName("support-bot")]; a.Client.Model != "kev-support" || a.Temperature != 1.4 {
		t.Errorf("compiled agent model = %+v", a)
	}
	if ms.Default.Client.Model != "kev-latest" || ms.Default.Temperature != 0.86 {
		t.Errorf("compiled default model = %+v", ms.Default)
	}
}

// Environment overrides change the scorer section only, and an agent's
// model never picks them up.
func TestAgentModelIgnoresScorerEnvironment(t *testing.T) {
	t.Setenv(EnvScorerEndpoint, "http://host.docker.internal:8010/v1/systemone")
	cfg, err := Load(writeAgents(t, `
agents:
  models:
    support-bot: {endpoint: "http://kev-support:8010/v1/systemone", model: kev-support, manifest: support.json}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Agents.Models["support-bot"].Endpoint != "http://kev-support:8010/v1/systemone" {
		t.Errorf("agent endpoint = %q", cfg.Agents.Models["support-bot"].Endpoint)
	}
}

func TestAgentModelErrors(t *testing.T) {
	const ok = "      endpoint: http://kev:8010/v1/systemone\n      model: kev-a\n"
	cases := []struct {
		name, agents, want string
	}{
		{"no endpoint", "agents:\n  models:\n    a:\n      model: kev-a\n      manifest: support.json\n", "agents.models.a.endpoint is required"},
		{"no model", "agents:\n  models:\n    a:\n      endpoint: http://kev:8010/v1/systemone\n      manifest: support.json\n", "agents.models.a.model is required"},
		{"no manifest", "agents:\n  models:\n    a:\n" + ok, "agents.models.a.manifest is required"},
		{"unknown key", "agents:\n  models:\n    a:\n" + ok + "      modle: x\n", "field modle not found"},
		{"bad value", "agents:\n  models:\n    a:\n" + ok + "      manifest: support.json\n      timeout: 0s\n", "agents.models.a.timeout must be positive"},
		{"missing manifest file", "agents:\n  models:\n    a:\n" + ok + "      manifest: nope.json\n", "nope.json"},
		{"no attributes", "agents:\n  attributes: []\n  models:\n    a:\n" + ok + "      manifest: support.json\n", "agents.attributes must name at least one"},
		{"slow model", "agents:\n  models:\n    a:\n" + ok + "      manifest: support.json\n      timeout: 10m\n", "for agents.models.a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeAgents(t, tc.agents))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// Each model reads its own API key.
func TestAgentModelAPIKey(t *testing.T) {
	t.Setenv("SUPPORT_KEY", "s3cret")
	cfg, err := Load(writeAgents(t, `
agents:
  models:
    support-bot:
      kind: jev
      endpoint: https://api.typesafe.ai/v1/systemone
      model: jev-latest
      api_key_env: SUPPORT_KEY
      manifest: support.json
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Agents.Models["support-bot"].APIKey != "s3cret" || cfg.Scorer.APIKey != "" {
		t.Errorf("keys: agent %q, default %q", cfg.Agents.Models["support-bot"].APIKey, cfg.Scorer.APIKey)
	}
}

// In code, an agent's model is given whole, with its manifest as a value.
func TestStaticFactoryAgentModels(t *testing.T) {
	cfg := Default()
	support := DefaultModel()
	support.Endpoint, support.Model = "http://kev-support:8010/v1/systemone", "kev-support"
	cfg.Agents.Models = map[string]Scorer{"support-bot": support}
	questions := json.RawMessage(`{"answer_wrong": {"type": "noul", "instructions": "?"}}`)
	f := NewStaticFactory(cfg, &Manifest{Questions: questions})
	f.AgentManifests = map[string]*Manifest{"support-bot": {Questions: questions, Temperatures: map[string]float64{"answer_wrong": 2}}}

	got, err := NewCompiler(f, WithEnv(nil)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if got.AgentManifests["support-bot"].Temperature != 2 || got.Manifest.Temperature != 1 {
		t.Errorf("manifests: agent %+v, default %+v", got.AgentManifests["support-bot"], got.Manifest)
	}
}

func TestMetricsConfig(t *testing.T) {
	t.Setenv("METRICS_HEADERS", "Authorization=Bearer%20abc")
	cfg, err := Load(write(t, `
scorer:
  manifest: manifest.json
observability:
  metrics:
    endpoint: http://collector:4318/v1/metrics
    headers_env: METRICS_HEADERS
    interval: 30s
`, testManifest))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.MetricsConfig()
	if m.Endpoint != "http://collector:4318/v1/metrics" || m.Interval != 30*time.Second || m.Headers["Authorization"] != "Bearer abc" {
		t.Errorf("metrics = %+v", m)
	}

	if cfg := Default(); cfg.Observability.Metrics.Enabled() {
		t.Error("metrics are on by default")
	}
	_, err = Load(write(t, "scorer:\n  manifest: manifest.json\nobservability:\n  metrics:\n    endpoint: collector:4318\n", testManifest))
	if err == nil || !strings.Contains(err.Error(), "observability.metrics.endpoint") {
		t.Errorf("error = %v", err)
	}
}
