package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var exporterManifest = &Manifest{Questions: json.RawMessage(`{"answer_wrong": {"type": "noul", "instructions": "?"}}`)}

func compileExporters(t *testing.T, exporters []Exporter, env map[string]string) (*Config, error) {
	t.Helper()
	cfg := Default()
	cfg.Exporters = exporters
	return NewCompiler(NewStaticFactory(cfg, exporterManifest), WithEnv(func(k string) string { return env[k] })).Compile()
}

// Inline headers and the ones in headers_env are merged; the variable wins.
func TestExportHeadersMerge(t *testing.T) {
	exporters := []Exporter{{
		Name:       "langfuse",
		Endpoint:   "https://cloud.langfuse.com/api/public/otel/v1/traces",
		Headers:    map[string]string{"x-team": "agents", "Authorization": "overridden"},
		HeadersEnv: "LANGFUSE_OTLP_HEADERS",
	}}
	env := map[string]string{"LANGFUSE_OTLP_HEADERS": "Authorization=Basic%20cGs6c2s=, x-extra = 1"}
	cfg, err := compileExporters(t, exporters, env)
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.Exporters[0].ResolvedHeaders
	want := map[string]string{"x-team": "agents", "Authorization": "Basic cGs6c2s=", "x-extra": "1"}
	if len(got) != len(want) {
		t.Fatalf("headers = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("header %s = %q, want %q", k, got[k], v)
		}
	}
	if len(cfg.Exporters[0].Headers) != 2 {
		t.Error("resolving changed the inline headers")
	}
}

func TestExportHeadersErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"unset", nil, "$OTLP_HEADERS is not set"},
		{"no equals", map[string]string{"OTLP_HEADERS": "Authorization"}, `expected "key=value"`},
		{"bad escape", map[string]string{"OTLP_HEADERS": "Authorization=%zz"}, `header "Authorization"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exporters := []Exporter{{Name: "b", Endpoint: "http://b:4318/v1/traces", HeadersEnv: "OTLP_HEADERS"}}
			_, err := compileExporters(t, exporters, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "exporters[0] (b)") {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "%zz") {
				t.Error("the error repeats the header value")
			}
		})
	}
}

func TestExportersValidation(t *testing.T) {
	cases := []struct {
		name      string
		exporters []Exporter
		want      string
	}{
		{"duplicate name", []Exporter{{Name: "a", Endpoint: "http://a/v1/traces"}, {Name: "a", Endpoint: "http://b/v1/traces"}}, `"a" is used twice`},
		{"no name", []Exporter{{Endpoint: "http://a/v1/traces"}}, `exporters[0].name "" must be letters`},
		{"unsafe name", []Exporter{{Name: "lang fuse", Endpoint: "http://a/v1/traces"}}, `"lang fuse" must be letters`},
		{"same stream", []Exporter{{Name: "lang-fuse", Endpoint: "http://a/v1/traces"}, {Name: "LANG_FUSE", Endpoint: "http://b/v1/traces"}}, `"LANG_FUSE" is used twice`},
		{"negative max_deliver", []Exporter{{Name: "a", Endpoint: "http://a/v1/traces", MaxDeliver: -1}}, "must not be negative"},
		{"negative backoff", []Exporter{{Name: "a", Endpoint: "http://a/v1/traces", Backoff: []Duration{-1}}}, "backoff[0] must not be negative"},
		{"bad endpoint", []Exporter{{Name: "a", Endpoint: "phoenix:6006"}}, "exporters[0].endpoint"},
		{"negative queue", []Exporter{{Name: "a", Endpoint: "http://a/v1/traces", QueueSize: -1}}, "must not be negative"},
		{"empty header name", []Exporter{{Name: "a", Endpoint: "http://a/v1/traces", Headers: map[string]string{" ": "x"}}}, "header name must not be empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileExporters(t, tc.exporters, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// An empty list is allowed: the engine decides but sends traces nowhere.
func TestNoExporters(t *testing.T) {
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\nexporters: []\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Exporters) != 0 || len(cfg.ExportBackends()) != 0 {
		t.Errorf("exporters = %+v", cfg.Exporters)
	}
}

// OBSERVER_EXPORT_ENDPOINT moves the first exporter only.
func TestExportEndpointEnvMovesFirstExporter(t *testing.T) {
	exporters := []Exporter{{Name: "a", Endpoint: "http://a/v1/traces"}, {Name: "b", Endpoint: "http://b/v1/traces"}}
	cfg, err := compileExporters(t, exporters, map[string]string{EnvExportEndpoint: "http://env/v1/traces"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Exporters[0].Endpoint != "http://env/v1/traces" || cfg.Exporters[1].Endpoint != "http://b/v1/traces" {
		t.Errorf("exporters = %+v", cfg.Exporters)
	}
	backends := cfg.ExportBackends()
	if len(backends) != 2 || backends[0].Name != "a" || backends[0].Endpoint != "http://env/v1/traces" {
		t.Errorf("backends = %+v", backends)
	}
}

// Exporter retries reach the export package; an empty backoff leaves its default.
func TestExportBackendRetries(t *testing.T) {
	exporters := []Exporter{
		{Name: "a", Endpoint: "http://a/v1/traces", MaxDeliver: 5, Backoff: []Duration{Duration(time.Second)}},
		{Name: "b", Endpoint: "http://b/v1/traces"},
	}
	cfg, err := compileExporters(t, exporters, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.ExportBackends()
	if b[0].MaxDeliver != 5 || len(b[0].Backoff) != 1 || b[0].Backoff[0] != time.Second {
		t.Errorf("a = %+v", b[0])
	}
	if b[1].Backoff != nil {
		t.Errorf("b backoff = %v, want nil for the default", b[1].Backoff)
	}
}

// queue.url chooses the broker: empty is in process, set is NATS and must
// be a NATS URL.
func TestQueueURLChoosesBroker(t *testing.T) {
	cfg, err := Load(write(t, "scorer:\n  manifest: manifest.json\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue.NATS() || cfg.BrokerConfig(nil).URL != "" {
		t.Errorf("default queue.url = %q, want in process", cfg.Queue.URL)
	}

	cfg, err = Load(write(t, "scorer:\n  manifest: manifest.json\nqueue:\n  url: nats://nats:4222\n", testManifest))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Queue.NATS() || cfg.BrokerConfig(nil).URL != "nats://nats:4222" {
		t.Errorf("queue = %+v, want NATS", cfg.Queue)
	}
	if s := cfg.ExportStreams(); s.Stream != "OBSERVER_EXPORTS" || s.Consumer != "observer-workers-export" || s.DLQSubject != "observer.exports_dlq" {
		t.Errorf("export streams = %+v", s)
	}

	_, err = Load(write(t, "scorer:\n  manifest: manifest.json\nqueue:\n  url: http://nats:4222\n", testManifest))
	if err == nil || !strings.Contains(err.Error(), "leave it empty to queue in process") {
		t.Errorf("error = %v", err)
	}
}
