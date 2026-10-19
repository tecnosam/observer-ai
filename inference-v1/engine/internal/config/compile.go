package config

import (
	"log/slog"
	"time"

	"github.com/observer-ai/engine/internal/broker"
	"github.com/observer-ai/engine/internal/export"
	"github.com/observer-ai/engine/internal/observe"
	"github.com/observer-ai/engine/internal/pipeline"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/scorer"
)

// The compiled Config in the shape each component takes. No component
// imports this package; main hands each one its part.

// ScorerConfig is the Kev (or Jev) client's settings.
func (c *Config) ScorerConfig(reporter observe.Reporter) scorer.Config {
	return scorer.Config{
		Endpoint:         c.Scorer.Endpoint,
		Model:            c.Scorer.Model,
		Questions:        c.Manifest.Questions,
		DecisionQuestion: c.Scorer.DecisionQuestion,
		APIKey:           c.Scorer.APIKey,
		Timeout:          c.Scorer.Timeout.Std(),
		MaxRetries:       c.Scorer.MaxRetries,
		Backoff:          durations(c.Scorer.Backoff),
		Reporter:         reporter,
	}
}

// QueueConfig is the work queue's settings, span storage included when OTLP is on.
func (c *Config) QueueConfig(logger *slog.Logger) queue.Config {
	return queue.Config{
		URL:               c.Queue.URL,
		ConnectTimeout:    c.Queue.ConnectTimeout.Std(),
		Stream:            c.Queue.Stream,
		Subject:           c.Queue.Subject,
		Consumer:          c.Queue.Consumer,
		MaxMessages:       c.Queue.MaxMessages,
		AckWait:           c.Queue.AckWait.Std(),
		MaxDeliver:        c.Queue.MaxDeliver,
		RedeliveryBackoff: durations(c.Queue.RedeliveryBackoff),
		DLQStream:         c.Queue.DLQStream,
		DLQSubject:        c.Queue.DLQSubject,
		DLQMaxAge:         c.Queue.DLQMaxAge.Std(),
		Spans:             c.OTLP.spanConfig(),
		Logger:            logger,
	}
}

// PolicyConfig is the keep/drop policy's thresholds.
func (c *Config) PolicyConfig() policy.Config {
	return policy.Config{
		KeepThreshold: pipeline.Probability(c.Policy.KeepThreshold),
		RandomFloor:   pipeline.Probability(c.Policy.RandomFloor),
		SeverityHigh:  pipeline.Probability(c.Alert.SeverityHigh),
	}
}

// ExportBackends is every configured trace back end, headers resolved.
func (c *Config) ExportBackends() []export.BackendConfig {
	out := make([]export.BackendConfig, len(c.Exporters))
	for i, e := range c.Exporters {
		out[i] = export.BackendConfig{
			Name:               e.Name,
			Endpoint:           e.Endpoint,
			Headers:            e.ResolvedHeaders,
			ResourceAttributes: e.ResourceAttributes,
			QueueSize:          e.QueueSize,
			Concurrency:        e.Concurrency,
			MaxDeliver:         e.MaxDeliver,
			Backoff:            exportBackoff(e.Backoff),
		}
	}
	return out
}

// exportBackoff is nil when none is set, so the exporter's default applies.
func exportBackoff(in []Duration) []time.Duration {
	if len(in) == 0 {
		return nil
	}
	return durations(in)
}

// BrokerConfig chooses where the queues live: NATS when queue.url is set.
func (c *Config) BrokerConfig(logger *slog.Logger) broker.Config {
	return broker.Config{URL: c.Queue.URL, Logger: logger}
}

// ExportStreams names the export queues' JetStream objects.
func (c *Config) ExportStreams() export.NATSStreams {
	return export.NATSStreams{
		Stream:     c.Queue.ExportStream,
		Subject:    c.Queue.ExportSubject,
		Consumer:   c.Queue.Consumer + "-export",
		DLQStream:  c.Queue.ExportDLQStream,
		DLQSubject: c.Queue.ExportDLQSubject,
		DLQMaxAge:  c.Queue.DLQMaxAge.Std(),
	}
}

// ErrorSinks is the error pipeline's sinks.
func (c *Config) ErrorSinks() []observe.SinkConfig {
	out := make([]observe.SinkConfig, len(c.Observability.ErrorSinks))
	for i, s := range c.Observability.ErrorSinks {
		out[i] = observe.SinkConfig{Kind: observe.SinkKind(s.Kind), Path: s.Path}
	}
	return out
}

// spanConfig maps the OTLP settings onto the queue. With OTLP off it is zero,
// and the queue creates no span streams.
func (o OTLP) spanConfig() queue.SpanConfig {
	if !o.Enabled() {
		return queue.SpanConfig{}
	}
	return queue.SpanConfig{
		Stream:         o.SpanStream,
		Subject:        o.SpanSubject,
		MaxAge:         o.SpanMaxAge.Std(),
		PendingStream:  o.PendingStream,
		PendingSubject: o.PendingSubject,
		IdleTimeout:    o.IdleTimeout.Std(),
		MaxWait:        o.MaxWait.Std(),
	}
}
