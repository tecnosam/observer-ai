package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeNATS writes a config using NATS with the given queue keys, and a
// creds file next to it.
func writeNATS(t *testing.T, queue string) string {
	t.Helper()
	path := write(t, "scorer:\n  manifest: manifest.json\nqueue:\n  url: nats://nats:4222\n"+queue, testManifest)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "engine.creds"), []byte("creds"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNATSAuthAndTLS(t *testing.T) {
	t.Setenv("NATS_PASSWORD", "pw")
	cfg, err := Load(writeNATS(t, `
  replicas: 3
  auth:
    user: observer
    password_env: NATS_PASSWORD
  tls:
    ca_file: engine.creds
`))
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.BrokerConfig(nil)
	if b.Auth.User != "observer" || b.Auth.Password != "pw" {
		t.Errorf("auth = %+v", b.Auth)
	}
	if !filepath.IsAbs(b.TLS.CAFile) || filepath.Base(b.TLS.CAFile) != "engine.creds" {
		t.Errorf("ca_file = %q, want resolved against the config's directory", b.TLS.CAFile)
	}
	if cfg.QueueConfig(nil).Replicas != 3 || cfg.ExportStreams().Replicas != 3 {
		t.Error("replicas did not reach the queue and the export streams")
	}
}

func TestNATSConfigErrors(t *testing.T) {
	t.Setenv("NATS_TOKEN", "x")
	cases := []struct{ name, queue, want string }{
		{"replicas", "  replicas: 7\n", "queue.replicas must be between 1 and 5"},
		{"two methods", "  auth:\n    credentials_file: engine.creds\n    token_env: NATS_TOKEN\n", "set one way to authenticate, got credentials_file, token_env"},
		{"user alone", "  auth:\n    user: observer\n", "user and queue.auth.password_env go together"},
		{"secret unset", "  auth:\n    token_env: NO_SUCH_VAR\n", "queue.auth.token_env: $NO_SUCH_VAR is not set"},
		{"missing file", "  auth:\n    credentials_file: nope.creds\n", "queue.auth.credentials_file"},
		{"cert alone", "  tls:\n    cert_file: engine.creds\n", "cert_file and queue.tls.key_file go together"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeNATS(t, tc.queue))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// A secret never shows in an error.
func TestNATSSecretNotInErrors(t *testing.T) {
	t.Setenv("NATS_TOKEN", "t0ken-value")
	_, err := Load(writeNATS(t, "  replicas: 9\n  auth:\n    token_env: NATS_TOKEN\n"))
	if err == nil || strings.Contains(err.Error(), "t0ken-value") {
		t.Errorf("error = %v", err)
	}
}

// Without NATS the connection settings are not checked.
func TestNATSSettingsIgnoredInMemory(t *testing.T) {
	_, err := Load(write(t, "scorer:\n  manifest: manifest.json\nqueue:\n  auth:\n    user: observer\n", testManifest))
	if err != nil {
		t.Errorf("error = %v", err)
	}
}
