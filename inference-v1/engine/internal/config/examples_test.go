package config

import (
	"path/filepath"
	"testing"
)

const examplesDir = "../../../examples"

// Every example config in inference-v1/examples must load and validate.
// OBSERVER_MANIFEST points them all at the test manifest, because the
// fine-tuning runs most of them name are not in the repository.
func TestExampleConfigsLoad(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(examplesDir, "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no example configs in %s", examplesDir)
	}
	t.Setenv(EnvManifest, mustAbs(t, "../../testdata/manifest.json"))
	t.Setenv(DefaultJevAPIKeyEnv, "test-key")
	t.Setenv("LANGFUSE_OTLP_HEADERS", "Authorization=Basic%20dGVzdDp0ZXN0")
	t.Setenv("PHOENIX_OTLP_HEADERS", "api_key=test")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := Load(path); err != nil {
				t.Error(err)
			}
		})
	}
}

// The Jev example's manifest is in the repository, so check it for real.
func TestJevExampleManifest(t *testing.T) {
	if _, err := LoadManifest(filepath.Join(examplesDir, "manifests", "jev.json"), "answer_wrong"); err != nil {
		t.Fatal(err)
	}
}

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}
