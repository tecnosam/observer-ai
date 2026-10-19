package config

import (
	"log/slog"

	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/scorer"
)

// The compiled Config in the shape each component takes. No component
// imports this package; main hands each one its part.

// ScorerConfig is the Kev (or Jev) client's settings.
func (c *Config) ScorerConfig(logger *slog.Logger) scorer.Config {
	return scorer.Config{
		Endpoint:         c.Scorer.Endpoint,
		Model:            c.Scorer.Model,
		Questions:        c.Manifest.Questions,
		DecisionQuestion: c.Scorer.DecisionQuestion,
		APIKey:           c.Scorer.APIKey,
		Timeout:          c.Scorer.Timeout.Std(),
		MaxRetries:       c.Scorer.MaxRetries,
		Backoff:          durations(c.Scorer.Backoff),
		Logger:           logger,
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
		KeepThreshold: c.Policy.KeepThreshold,
		RandomFloor:   c.Policy.RandomFloor,
		SeverityHigh:  c.Alert.SeverityHigh,
	}
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
