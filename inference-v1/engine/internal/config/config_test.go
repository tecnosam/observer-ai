package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testManifest = `{
  "model": "m",
  "questions": {
    "answer_wrong": {"type": "noul", "instructions": "wrong?"},
    "quality": {"type": "score", "instructions": "how good?", "criteria": ["a", "b"]}
  },
  "temperatures": {"answer_wrong": 0.86},
  "extra": {"ignored": true}
}`

// write puts a config and manifest in a temp dir and returns the config path.
func write(t *testing.T, yaml, manifest string) string {
	t.Helper()
	dir := t.TempDir()
	if manifest != "" {
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "observer.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	want := Default()
	if cfg.Server != want.Server {
		t.Errorf("server = %+v, want %+v", cfg.Server, want.Server)
	}
	if cfg.Policy != want.Policy || cfg.Alert != want.Alert || cfg.Export != want.Export {
		t.Errorf("policy/alert/export defaults not applied: %+v", cfg)
	}
	if cfg.Scorer.Timeout.Std() != 10*time.Second || cfg.Scorer.MaxRetries != 3 || len(cfg.Scorer.Backoff) != 3 {
		t.Errorf("scorer defaults not applied: %+v", cfg.Scorer)
	}
	if cfg.Manifest.Temperature != 0.86 {
		t.Errorf("temperature = %v, want 0.86", cfg.Manifest.Temperature)
	}
	// Compacted, with the manifest's key order preserved.
	wantQ := `{"answer_wrong":{"type":"noul","instructions":"wrong?"},"quality":{"type":"score","instructions":"how good?","criteria":["a","b"]}}`
	if string(cfg.Manifest.Questions) != wantQ {
		t.Errorf("questions = %s\nwant %s", cfg.Manifest.Questions, wantQ)
	}
}

func TestLoadOverrides(t *testing.T) {
	yaml := `
server:
  addr: ":9999"
  workers: 2
  queue_size: 5
scorer:
  endpoint: http://kev:1/v1/systemone
  manifest: manifest.json
  timeout: 250ms
  max_retries: 0
  backoff: [10ms, 1s]
policy:
  keep_threshold: 0
  random_floor: 1
alert:
  severity_high: 0.9
export:
  phoenix_endpoint: http://phoenix:2/v1/traces
  project: p
`
	cfg, err := Load(write(t, yaml, testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Addr != ":9999" || cfg.Server.Workers != 2 || cfg.Server.QueueSize != 5 {
		t.Errorf("server = %+v", cfg.Server)
	}
	if cfg.Scorer.Timeout.Std() != 250*time.Millisecond || cfg.Scorer.MaxRetries != 0 {
		t.Errorf("scorer = %+v", cfg.Scorer)
	}
	if got := cfg.Scorer.Backoff; len(got) != 2 || got[0].Std() != 10*time.Millisecond || got[1].Std() != time.Second {
		t.Errorf("backoff = %v", got)
	}
	// Explicit zero must survive, not be replaced by the default.
	if cfg.Policy.KeepThreshold != 0 || cfg.Policy.RandomFloor != 1 {
		t.Errorf("policy = %+v", cfg.Policy)
	}
	if cfg.Export.Project != "p" {
		t.Errorf("export = %+v", cfg.Export)
	}
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv(EnvScorerEndpoint, "http://env-kev:8010/v1/systemone")
	t.Setenv(EnvPhoenixEndpoint, "http://env-phoenix:6006/v1/traces")
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\n  endpoint: http://file:1/x\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scorer.Endpoint != "http://env-kev:8010/v1/systemone" {
		t.Errorf("scorer endpoint = %q", cfg.Scorer.Endpoint)
	}
	if cfg.Export.PhoenixEndpoint != "http://env-phoenix:6006/v1/traces" {
		t.Errorf("phoenix endpoint = %q", cfg.Export.PhoenixEndpoint)
	}
}

func TestManifestDefaultTemperature(t *testing.T) {
	m := `{"questions": {"answer_wrong": {"type": "noul", "instructions": "x"}}}`
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\n", m))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Manifest.Temperature != 1.0 {
		t.Errorf("temperature = %v, want 1.0", cfg.Manifest.Temperature)
	}
}

func TestLoadErrors(t *testing.T) {
	base := "scorer:\n  manifest: manifest.json\n"
	tests := []struct {
		name     string
		yaml     string
		manifest string
		wantErr  string
	}{
		{"threshold above 1", base + "policy:\n  keep_threshold: 1.5\n", testManifest, "policy.keep_threshold must be in [0, 1]"},
		{"negative floor", base + "policy:\n  random_floor: -0.1\n", testManifest, "policy.random_floor must be in [0, 1]"},
		{"severity above 1", base + "alert:\n  severity_high: 2\n", testManifest, "alert.severity_high must be in [0, 1]"},
		{"bad duration", "scorer:\n  manifest: manifest.json\n  timeout: soon\n", testManifest, `invalid duration "soon"`},
		{"bad backoff", "scorer:\n  manifest: manifest.json\n  backoff: [1s, later]\n", testManifest, `invalid duration "later"`},
		{"zero timeout", "scorer:\n  manifest: manifest.json\n  timeout: 0s\n", testManifest, "scorer.timeout must be positive"},
		{"zero workers", base + "server:\n  workers: 0\n", testManifest, "server.workers must be at least 1"},
		{"bad endpoint", "scorer:\n  manifest: manifest.json\n  endpoint: localhost:8010\n", testManifest, "scorer.endpoint"},
		{"unknown key", base + "polcy:\n  keep_threshold: 0.5\n", testManifest, "polcy"},
		{"no manifest path", "server:\n  workers: 1\n", testManifest, "scorer.manifest is required"},
		{"manifest file missing", base, "", "manifest:"},
		{"question missing", "scorer:\n  manifest: manifest.json\n  decision_question: nope\n", testManifest, `decision question "nope" not found`},
		{"question not noul", "scorer:\n  manifest: manifest.json\n  decision_question: quality\n", testManifest, `has type "score", want "noul"`},
		{"bad temperature", base, `{"questions": {"answer_wrong": {"type": "noul"}}, "temperatures": {"answer_wrong": 0}}`, "must be positive"},
		{"manifest not JSON", base, "{", "manifest"},
		{"no questions", base, `{"model": "m"}`, "must be a non-empty object"},
		{"questions is a list", base, `{"questions": []}`, "must be a non-empty object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(write(t, tt.yaml, tt.manifest))
			if err == nil {
				t.Fatalf("expected an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestMissingConfigFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected an error")
	}
}
