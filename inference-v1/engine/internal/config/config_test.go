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
	if q := cfg.Queue; q.URL != "nats://localhost:4222" || q.Stream != "OBSERVER_TRACES" || q.MaxDeliver != 5 ||
		q.AckWait.Std() != 2*time.Minute || len(q.RedeliveryBackoff) != 4 || q.DLQStream != "OBSERVER_TRACES_DLQ" {
		t.Errorf("queue defaults not applied: %+v", q)
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
queue:
  url: nats://nats:4222
  stream: T
  subject: t.in
  consumer: c
  max_messages: 7
  ack_wait: 45s
  max_deliver: 2
  redelivery_backoff: [1s]
  dlq_stream: T_DLQ
  dlq_subject: t.dlq
  dlq_max_age: 1h
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
	if cfg.Server.Addr != ":9999" || cfg.Server.Workers != 2 {
		t.Errorf("server = %+v", cfg.Server)
	}
	q := cfg.Queue
	if q.URL != "nats://nats:4222" || q.Stream != "T" || q.Subject != "t.in" || q.Consumer != "c" ||
		q.MaxMessages != 7 || q.AckWait.Std() != 45*time.Second || q.MaxDeliver != 2 ||
		len(q.RedeliveryBackoff) != 1 || q.DLQStream != "T_DLQ" || q.DLQSubject != "t.dlq" || q.DLQMaxAge.Std() != time.Hour {
		t.Errorf("queue = %+v", q)
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
	t.Setenv(EnvNATSURL, "nats://env-nats:4222")
	path := write(t, "scorer:\n  manifest: does-not-exist.json\n  endpoint: http://file:1/x\n", testManifest)
	t.Setenv(EnvManifest, filepath.Join(filepath.Dir(path), "manifest.json"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue.URL != "nats://env-nats:4222" {
		t.Errorf("nats url = %q", cfg.Queue.URL)
	}
	if cfg.Manifest.Temperature != 0.86 {
		t.Errorf("manifest from %s was not loaded", EnvManifest)
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
		{"removed queue_size", base + "server:\n  queue_size: 10\n", testManifest, "queue_size"},
		{"bad nats url", base + "queue:\n  url: http://nats:4222\n", testManifest, "queue.url"},
		{"stream name with dot", base + "queue:\n  stream: a.b\n", testManifest, "queue.stream must be a non-empty name"},
		{"wildcard subject", base + "queue:\n  subject: traces.>\n", testManifest, "queue.subject must be a non-empty subject"},
		{"dlq same stream", base + "queue:\n  dlq_stream: OBSERVER_TRACES\n", testManifest, "queue.dlq_stream must differ"},
		{"dlq same subject", base + "queue:\n  dlq_subject: observer.traces\n", testManifest, "queue.dlq_subject must differ"},
		{"zero max_deliver", base + "queue:\n  max_deliver: 0\n", testManifest, "queue.max_deliver must be at least 1"},
		{"zero max_messages", base + "queue:\n  max_messages: 0\n", testManifest, "queue.max_messages must be at least 1"},
		{"otlp same ports", base + "otlp:\n  grpc_addr: \":4317\"\n  http_addr: \":4317\"\n", testManifest, "otlp.grpc_addr and otlp.http_addr must differ"},
		{"otlp port clashes with api", base + "otlp:\n  http_addr: \":8080\"\n", testManifest, "otlp.http_addr must differ from server.addr"},
		{"otlp max_wait below idle", base + "otlp:\n  idle_timeout: 1m\n  max_wait: 30s\n", testManifest, "otlp.max_wait (30s) must be at least otlp.idle_timeout (1m0s)"},
		{"otlp spans expire too soon", base + "otlp:\n  span_max_age: 1m\n", testManifest, "otlp.span_max_age (1m0s) must exceed otlp.max_wait (2m0s)"},
		{"otlp zero assemblers", base + "otlp:\n  assemblers: 0\n", testManifest, "otlp.assemblers must be at least 1"},
		{"otlp stream reused", base + "otlp:\n  span_stream: OBSERVER_TRACES\n", testManifest, "otlp.span_stream and queue.stream must differ"},
		{"otlp streams equal", base + "otlp:\n  pending_stream: OBSERVER_SPANS\n", testManifest, "otlp.pending_stream and otlp.span_stream must differ"},
		{"otlp subject is a prefix", base + "otlp:\n  span_subject: observer.traces\n", testManifest, `otlp.span_subject ("observer.traces") must not equal or be a prefix of`},
		{"ack_wait too short", base + "queue:\n  ack_wait: 30s\n", testManifest, "queue.ack_wait (30s) must exceed the longest possible scoring time (47.5s"},
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

func TestScorerWorstCase(t *testing.T) {
	d := Default().Scorer // 4 attempts x 10s + 500ms + 2s + 5s
	if got := d.WorstCase(); got != 47500*time.Millisecond {
		t.Errorf("WorstCase = %v, want 47.5s", got)
	}
	d.MaxRetries = 5 // the last backoff entry repeats: + 5s + 5s, + 2 attempts
	if got := d.WorstCase(); got != 77500*time.Millisecond {
		t.Errorf("WorstCase with 5 retries = %v, want 1m17.5s", got)
	}
	d.MaxRetries = 0
	if got := d.WorstCase(); got != 10*time.Second {
		t.Errorf("WorstCase with no retries = %v, want 10s", got)
	}
}

func TestOTLPDefaultsAndOverrides(t *testing.T) {
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	o := cfg.OTLP
	if o.GRPCAddr != ":4317" || o.HTTPAddr != ":4318" || o.IdleTimeout.Std() != 10*time.Second ||
		o.MaxWait.Std() != 2*time.Minute || o.Assemblers != 4 || o.SpanStream != "OBSERVER_SPANS" ||
		o.PendingStream != "OBSERVER_PENDING" || o.SpanMaxAge.Std() != time.Hour || !o.Enabled() {
		t.Errorf("otlp defaults = %+v", o)
	}

	t.Setenv(EnvOTLPGRPCAddr, ":14317")
	t.Setenv(EnvOTLPHTTPAddr, ":14318")
	cfg, err = Load(write(t, "scorer:\n  manifest: manifest.json\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OTLP.GRPCAddr != ":14317" || cfg.OTLP.HTTPAddr != ":14318" {
		t.Errorf("otlp addresses = %q, %q; want the env overrides", cfg.OTLP.GRPCAddr, cfg.OTLP.HTTPAddr)
	}
}

// Empty addresses switch OTLP off, and then its other settings are not checked.
func TestOTLPDisabled(t *testing.T) {
	yaml := "scorer:\n  manifest: manifest.json\notlp:\n  grpc_addr: \"\"\n  http_addr: \"\"\n  assemblers: 0\n"
	cfg, err := Load(write(t, yaml, testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OTLP.Enabled() {
		t.Error("OTLP reported enabled with both addresses empty")
	}
}
