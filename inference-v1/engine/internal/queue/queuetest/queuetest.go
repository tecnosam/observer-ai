// Package queuetest runs an in-process NATS JetStream server for tests. It
// listens on a random loopback port and stores data in a temp directory, so
// tests need no network and no Docker.
package queuetest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// StartServer starts a JetStream-enabled NATS server and returns it. Its
// client URL is srv.ClientURL(). The server stops when the test ends.
func StartServer(t testing.TB) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("start NATS server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv
}
