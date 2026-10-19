// Package natsconn holds the one NATS connection the engine shares between
// its JetStream users: the trace queue, span assembly and the export queues.
package natsconn

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/observe"
)

const (
	connectionName = "observer-engine"
	reconnectWait  = time.Second
)

// Config is what a connection is opened with.
type Config struct {
	URL      string
	Logger   *slog.Logger     // defaults to slog.Default()
	Reporter observe.Reporter // receives disconnects; defaults to observe.Discard
}

// Conn is an open NATS connection and its JetStream context.
type Conn struct {
	NC *nats.Conn
	JS jetstream.JetStream
}

// Dial connects to NATS. It returns at once even when NATS is not up yet:
// the client keeps reconnecting for as long as the process runs, and the
// callers wait for JetStream when they set up their streams.
func Dial(cfg Config) (*Conn, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	nc, err := nats.Connect(cfg.URL,
		nats.Name(connectionName),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(reconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				cfg.Reporter.Report(context.Background(), fault.Unavailable(fault.Queue, "nats connection", err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			cfg.Logger.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats: connect to %s: %w", cfg.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: %w", err)
	}
	return &Conn{NC: nc, JS: js}, nil
}

// Ready reports whether the connection is up.
func (c *Conn) Ready() error {
	if !c.NC.IsConnected() {
		return fmt.Errorf("nats is %s", strings.ToLower(c.NC.Status().String()))
	}
	return nil
}

// MaxPayload is the largest message the server takes, or 0 when unknown.
func (c *Conn) MaxPayload() int {
	return int(c.NC.MaxPayload())
}

// Close closes the connection. Stop everything that uses it first.
func (c *Conn) Close() {
	c.NC.Close()
}

// Retry runs setup until it succeeds or timeout passes, so the engine can
// start alongside NATS.
func Retry(ctx context.Context, timeout, wait time.Duration, setup func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := setup(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}
