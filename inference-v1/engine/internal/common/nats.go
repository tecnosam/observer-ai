package common

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
)

const (
	connectionName = "observer-engine"
	reconnectWait  = time.Second
	pullErrorPause = time.Second
	maxHeaderValue = 1024
)

// NATSConfig is what a connection is opened with.
type NATSConfig struct {
	URL      string
	Logger   *slog.Logger     // defaults to slog.Default()
	Reporter observe.Reporter // receives disconnects; defaults to observe.Discard
}

// Conn is the one NATS connection the engine's JetStream users share: the
// trace queue, span assembly and the export queues.
type Conn struct {
	NC *nats.Conn
	JS jetstream.JetStream
}

// DialNATS connects to NATS. It returns at once even when NATS is not up
// yet: the client keeps reconnecting for as long as the process runs, and
// callers wait for JetStream when they set up their streams.
func DialNATS(cfg NATSConfig) (*Conn, error) {
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

// EnsureStreams creates or updates every stream, in order.
func (c *Conn) EnsureStreams(ctx context.Context, streams ...jetstream.StreamConfig) error {
	for _, s := range streams {
		if _, err := c.JS.CreateOrUpdateStream(ctx, s); err != nil {
			return fmt.Errorf("stream %s: %w", s.Name, err)
		}
	}
	return nil
}

// RetryUntil runs setup until it succeeds or timeout passes, waiting wait
// between tries, so the engine can start alongside NATS.
func RetryUntil(ctx context.Context, timeout, wait time.Duration, setup func(context.Context) error) error {
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

// Pull hands handle one message at a time from cons until p stops. A poll
// that finds nothing is not an error; any other failure goes to onError
// and the loop pauses a second before trying again.
func Pull(p *Pool, cons jetstream.Consumer, pollWait time.Duration, handle func(jetstream.Msg), onError func(error)) {
	for !p.Stopping() {
		// One message per pull: nothing waits out its ack deadline in a buffer.
		msg, err := cons.Next(jetstream.FetchMaxWait(pollWait))
		if err == nil {
			handle(msg)
			continue
		}
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) || p.Stopping() {
			continue
		}
		onError(err)
		select {
		case <-p.Stopped():
		case <-time.After(pullErrorPause):
		}
	}
}

// IsStreamFull recognises the server refusing a publish because the stream
// is at MaxMsgs. JetStream reports it as a store failure whose description
// carries the reason; there is no dedicated error code.
func IsStreamFull(err error) bool {
	return apiErrorSays(err, "maximum messages exceeded")
}

// IsSubjectFull recognises a publish refused because its subject already
// holds MaxMsgsPerSubject messages.
func IsSubjectFull(err error) bool {
	return apiErrorSays(err, "maximum messages per subject exceeded")
}

func apiErrorSays(err error, text string) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && strings.Contains(apiErr.Description, text)
}

// HeaderValue makes s safe to carry in a NATS header: one line, bounded.
func HeaderValue(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxHeaderValue {
		s = s[:maxHeaderValue] + "..."
	}
	return s
}
