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

// An agent's model states only what differs; the rest comes from scorer.
func TestAgentModelInheritsScorer(t *testing.T) {
	cfg, err := Load(writeAgents(t, `
agents:
  models:
    support-bot:
      model: kev-support
      manifest: support.json
`))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Agents.Models["support-bot"]
	if m.Model != "kev-support" || m.Manifest != "support.json" {
		t.Errorf("own keys = %+v", m)
	}
	if m.Endpoint != cfg.Scorer.Endpoint || m.Timeout.Std() != 2*time.Second || m.DecisionQuestion != "answer_wrong" {
		t.Errorf("inherited keys = %+v, want the scorer section's", m)
	}
	if got := cfg.AgentManifests["support-bot"]; got.Model != "support" || got.Temperature != 1.4 {
		t.Errorf("agent manifest = %+v", got)
	}
	if cfg.Manifest.Temperature != 0.86 {
		t.Errorf("default manifest changed: %+v", cfg.Manifest)
	}

	// Compiled for the components: one model per agent, with its own temperature.
	ms := cfg.Models(nil)
	if a := ms.ByAgent[trace.AgentName("support-bot")]; a.Client.Model != "kev-support" || a.Temperature != 1.4 {
		t.Errorf("compiled agent model = %+v", a)
	}
	if ms.Default.Client.Model != "kev-latest" || ms.Default.Temperature != 0.86 {
		t.Errorf("compiled default model = %+v", ms.Default)
	}
}

// An agent's backoff list is its own copy, not the scorer's.
func TestAgentModelOverridesWholeLists(t *testing.T) {
	cfg, err := Load(writeAgents(t, `
agents:
  models:
    support-bot:
      manifest: support.json
      backoff: [1s]
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Agents.Models["support-bot"].Backoff; len(got) != 1 || got[0].Std() != time.Second {
		t.Errorf("agent backoff = %v", got)
	}
	if len(cfg.Scorer.Backoff) != 3 {
		t.Errorf("scorer backoff changed to %v", cfg.Scorer.Backoff)
	}
}

func TestAgentModelErrors(t *testing.T) {
	cases := []struct {
		name, agents, want string
	}{
		{"unknown key", "agents:\n  models:\n    a:\n      modle: x\n", "field modle not found"},
		{"bad value", "agents:\n  models:\n    a:\n      manifest: support.json\n      timeout: 0s\n", "agents.models.a.timeout must be positive"},
		{"missing manifest file", "agents:\n  models:\n    a:\n      manifest: nope.json\n", "nope.json"},
		{"no attributes", "agents:\n  attributes: []\n  models:\n    a:\n      manifest: support.json\n", "agents.attributes must name at least one"},
		{"slow model", "agents:\n  models:\n    a:\n      manifest: support.json\n      timeout: 10m\n", "for agents.models.a"},
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
	support := cfg.Scorer
	support.Model = "kev-support"
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
