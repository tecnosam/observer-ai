package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/observer-ai/engine/pkg/queue"
)

// tokenServer starts a JetStream server that requires token.
func tokenServer(t *testing.T, token string) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: server.RANDOM_PORT, JetStream: true, StoreDir: t.TempDir(),
		NoLog: true, NoSigs: true, Authorization: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func ingest(t *testing.T, cfg Config, q queue.Config) error {
	t.Helper()
	f, err := New(cfg)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := f.Ingest(context.Background(), q)
	if err == nil {
		b.Close()
	}
	return err
}

func TestNATSTokenAuth(t *testing.T) {
	srv := tokenServer(t, "s3cret")
	q := queueConfig()
	q.ConnectTimeout = time.Second

	if err := ingest(t, Config{URL: srv.ClientURL(), Auth: NATSAuth{Token: "s3cret"}, Logger: quiet}, q); err != nil {
		t.Errorf("right token: %v", err)
	}
	if err := ingest(t, Config{URL: srv.ClientURL(), Auth: NATSAuth{Token: "wrong"}, Logger: quiet}, q); err == nil {
		t.Error("wrong token connected")
	}
}

// Replicas reach the streams: a single server refuses more than one.
func TestNATSReplicasAreApplied(t *testing.T) {
	srv := tokenServer(t, "s3cret")
	q := queueConfig()
	q.ConnectTimeout = time.Second
	q.Replicas = 3
	err := ingest(t, Config{URL: srv.ClientURL(), Auth: NATSAuth{Token: "s3cret"}, Logger: quiet}, q)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "replica") {
		t.Errorf("error = %v, want the server refusing 3 replicas", err)
	}
}

func TestNATSOptionsFailOnABadSeedFile(t *testing.T) {
	if _, err := New(Config{URL: "nats://127.0.0.1:1", Auth: NATSAuth{NkeySeedFile: "/nonexistent"}}); err == nil {
		t.Error("a missing seed file was accepted")
	}
}
