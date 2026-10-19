package config

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// A program that embeds the engine configures it in code: no YAML file, no
// manifest file.
func TestStaticFactoryNeedsNoFiles(t *testing.T) {
	cfg := Default()
	cfg.Scorer.Endpoint = "http://kev.internal:8010/v1/systemone"
	cfg.Policy.KeepThreshold = 0.4
	manifest := &Manifest{
		Model:        "m",
		Questions:    json.RawMessage(`{"answer_wrong": {"type": "noul", "instructions": "wrong?"}}`),
		Temperatures: map[string]float64{"answer_wrong": 1.5},
	}

	got, err := NewCompiler(NewStaticFactory(cfg, manifest), WithEnv(nil)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if got.Scorer.Endpoint != cfg.Scorer.Endpoint || got.Policy.KeepThreshold != 0.4 {
		t.Errorf("settings changed on the way through: %+v", got.Scorer)
	}
	if got.Manifest.Temperature != 1.5 || string(got.Manifest.Questions) != `{"answer_wrong":{"type":"noul","instructions":"wrong?"}}` {
		t.Errorf("manifest = %+v", got.Manifest)
	}
	if string(manifest.Questions) == string(got.Manifest.Questions) {
		t.Error("compiling changed the caller's manifest value")
	}
}

func TestStaticFactoryIsValidatedLikeAFile(t *testing.T) {
	cfg := Default()
	cfg.Server.Workers = 0
	cfg.Scorer.Manifest = "/nowhere/manifest.json"
	_, err := NewCompiler(NewStaticFactory(cfg, nil), WithEnv(nil)).Compile()
	if err == nil || !strings.Contains(err.Error(), "server.workers must be at least 1") {
		t.Errorf("error = %v", err)
	}

	cfg.Server.Workers = 1
	_, err = NewCompiler(NewStaticFactory(cfg, nil), WithEnv(nil)).Compile()
	if err == nil || !strings.Contains(err.Error(), "/nowhere/manifest.json") {
		t.Errorf("with no manifest value the file is read: error = %v", err)
	}
}

// The environment is injected: tests and embedding programs decide what it holds.
func TestCompilerReadsTheInjectedEnvironment(t *testing.T) {
	env := map[string]string{EnvScorerEndpoint: "http://from-env:8010/v1/systemone", "MY_KEY": "secret"}
	cfg := Default()
	cfg.Scorer.APIKeyEnv = "MY_KEY"
	manifest := &Manifest{Questions: json.RawMessage(`{"answer_wrong": {"type": "noul", "instructions": "?"}}`)}

	got, err := NewCompiler(NewStaticFactory(cfg, manifest), WithEnv(func(k string) string { return env[k] })).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if got.Scorer.Endpoint != env[EnvScorerEndpoint] || got.Scorer.APIKey != "secret" {
		t.Errorf("endpoint %q, key %q", got.Scorer.Endpoint, got.Scorer.APIKey)
	}

	got, err = NewCompiler(NewStaticFactory(Default(), manifest), WithEnv(nil)).Compile()
	if err != nil || got.Scorer.Endpoint != Default().Scorer.Endpoint {
		t.Errorf("WithEnv(nil) must ignore the environment: endpoint %q, error %v", got.Scorer.Endpoint, err)
	}
}

// Any AbstractLoaderFactory works: settings could come from a database, flags or a remote store.
type fakeFactory struct{ loadErr error }

func (f fakeFactory) ConfigLoader() AbstractConfigLoader { return fakeConfigLoader{f.loadErr} }
func (f fakeFactory) ManifestLoader(Config) AbstractManifestLoader {
	return &staticManifestLoader{manifest: Manifest{Questions: json.RawMessage(`{"answer_wrong": {"type": "noul", "instructions": "?"}}`)}}
}
func (f fakeFactory) Describe() string { return "fake store" }

type fakeConfigLoader struct{ err error }

func (l fakeConfigLoader) LoadConfig() (Config, error) { return Default(), l.err }

func TestCompilerAcceptsAnyFactory(t *testing.T) {
	if _, err := NewCompiler(fakeFactory{}, WithEnv(nil)).Compile(); err != nil {
		t.Fatal(err)
	}
	broken := errors.New("store unreachable")
	if _, err := NewCompiler(fakeFactory{loadErr: broken}, WithEnv(nil)).Compile(); !errors.Is(err, broken) {
		t.Errorf("error = %v, want the loader's error", err)
	}
}
