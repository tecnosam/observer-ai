package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// validate checks every value and returns all the problems at once.
func (c *Config) validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Server.Addr == "" {
		add("server.addr must not be empty")
	}
	if c.Server.Workers < 1 {
		add("server.workers must be at least 1, got %d", c.Server.Workers)
	}
	if c.Server.ShutdownTimeout <= 0 {
		add("server.shutdown_timeout must be positive, got %s", c.Server.ShutdownTimeout.Std())
	}

	if c.Scorer.Kind != ScorerKev && c.Scorer.Kind != ScorerJev {
		add("scorer.kind must be %q or %q, got %q", ScorerKev, ScorerJev, c.Scorer.Kind)
	}
	if c.Scorer.APIKeyEnv != "" && c.Scorer.APIKey == "" {
		add("scorer.api_key_env: $%s is not set; it must hold the scorer's API key", c.Scorer.APIKeyEnv)
	}
	if err := checkURL(c.Scorer.Endpoint); err != nil {
		add("scorer.endpoint: %w", err)
	}
	if c.Scorer.Model == "" {
		add("scorer.model must not be empty")
	}
	if c.Scorer.DecisionQuestion == "" {
		add("scorer.decision_question must not be empty")
	}
	if c.Scorer.Timeout <= 0 {
		add("scorer.timeout must be positive, got %s", c.Scorer.Timeout.Std())
	}
	if c.Scorer.MaxRetries < 0 {
		add("scorer.max_retries must not be negative, got %d", c.Scorer.MaxRetries)
	}
	for i, b := range c.Scorer.Backoff {
		if b < 0 {
			add("scorer.backoff[%d] must not be negative, got %s", i, b.Std())
		}
	}

	c.validateQueue(add)
	c.validateOTLP(add)
	c.validateObservability(add)

	checkUnit := func(name string, v float64) {
		if !(v >= 0 && v <= 1) { // also rejects NaN
			add("%s must be in [0, 1], got %v", name, v)
		}
	}
	checkUnit("policy.keep_threshold", c.Policy.KeepThreshold)
	checkUnit("policy.random_floor", c.Policy.RandomFloor)
	checkUnit("alert.severity_high", c.Alert.SeverityHigh)

	c.validateExporters(add)
	return errors.Join(errs...)
}

func (c *Config) validateQueue(add func(format string, args ...any)) {
	q := c.Queue
	if u, err := url.Parse(q.URL); q.NATS() && (err != nil || u.Host == "" || (u.Scheme != "nats" && u.Scheme != "tls")) {
		add("queue.url: %q is not a nats:// or tls:// URL (leave it empty to queue in process)", q.URL)
	}
	if q.ConnectTimeout <= 0 {
		add("queue.connect_timeout must be positive, got %s", q.ConnectTimeout.Std())
	}
	for name, v := range map[string]string{
		"queue.stream": q.Stream, "queue.consumer": q.Consumer, "queue.dlq_stream": q.DLQStream,
		"queue.export_stream": q.ExportStream, "queue.export_dlq_stream": q.ExportDLQStream,
	} {
		if v == "" || strings.ContainsAny(v, " .*>/\\") {
			add("%s must be a non-empty name without spaces, dots or wildcards, got %q", name, v)
		}
	}
	for name, v := range map[string]string{
		"queue.subject": q.Subject, "queue.dlq_subject": q.DLQSubject,
		"queue.export_subject": q.ExportSubject, "queue.export_dlq_subject": q.ExportDLQSubject,
	} {
		if v == "" || strings.ContainsAny(v, " *>") {
			add("%s must be a non-empty subject without spaces or wildcards, got %q", name, v)
		}
	}
	if q.Stream == q.DLQStream {
		add("queue.dlq_stream must differ from queue.stream")
	}
	if q.Subject == q.DLQSubject {
		add("queue.dlq_subject must differ from queue.subject")
	}
	if strings.HasPrefix(q.ExportDLQSubject, q.ExportSubject+".") {
		add("queue.export_dlq_subject must not be under queue.export_subject, where each exporter has its own subject")
	}
	if q.MaxMessages < 1 {
		add("queue.max_messages must be at least 1, got %d", q.MaxMessages)
	}
	if q.MaxDeliver < 1 {
		add("queue.max_deliver must be at least 1, got %d", q.MaxDeliver)
	}
	for i, b := range q.RedeliveryBackoff {
		if b < 0 {
			add("queue.redelivery_backoff[%d] must not be negative, got %s", i, b.Std())
		}
	}
	if q.DLQMaxAge <= 0 {
		add("queue.dlq_max_age must be positive, got %s", q.DLQMaxAge.Std())
	}
	// A worker must be able to finish every Kev attempt before the queue
	// gives the trace to someone else, or traces get scored twice.
	if worst := c.Scorer.WorstCase(); q.AckWait.Std() <= worst {
		add("queue.ack_wait (%s) must exceed the longest possible scoring time (%s: every attempt timing out plus backoff)",
			q.AckWait.Std(), worst)
	}
}

func (c *Config) validateOTLP(add func(format string, args ...any)) {
	o := c.OTLP
	if !o.Enabled() {
		return
	}
	if o.GRPCAddr != "" && o.GRPCAddr == o.HTTPAddr {
		add("otlp.grpc_addr and otlp.http_addr must differ, both are %q", o.GRPCAddr)
	}
	for name, addr := range map[string]string{"otlp.grpc_addr": o.GRPCAddr, "otlp.http_addr": o.HTTPAddr} {
		if addr != "" && addr == c.Server.Addr {
			add("%s must differ from server.addr, both are %q", name, addr)
		}
	}
	if o.IdleTimeout <= 0 {
		add("otlp.idle_timeout must be positive, got %s", o.IdleTimeout.Std())
	}
	if o.MaxWait < o.IdleTimeout {
		add("otlp.max_wait (%s) must be at least otlp.idle_timeout (%s)", o.MaxWait.Std(), o.IdleTimeout.Std())
	}
	// Spans must outlive the wait for their trace to complete.
	if o.SpanMaxAge <= o.MaxWait {
		add("otlp.span_max_age (%s) must exceed otlp.max_wait (%s)", o.SpanMaxAge.Std(), o.MaxWait.Std())
	}
	if o.Assemblers < 1 {
		add("otlp.assemblers must be at least 1, got %d", o.Assemblers)
	}

	streams := map[string]string{
		"otlp.span_stream": o.SpanStream, "otlp.pending_stream": o.PendingStream,
		"queue.stream": c.Queue.Stream, "queue.dlq_stream": c.Queue.DLQStream,
	}
	subjects := map[string]string{
		"otlp.span_subject": o.SpanSubject, "otlp.pending_subject": o.PendingSubject,
		"queue.subject": c.Queue.Subject, "queue.dlq_subject": c.Queue.DLQSubject,
	}
	for _, name := range []string{"otlp.span_stream", "otlp.pending_stream"} {
		if v := streams[name]; v == "" || strings.ContainsAny(v, " .*>/\\") {
			add("%s must be a non-empty name without spaces, dots or wildcards, got %q", name, v)
		}
		for other, v := range streams {
			if other != name && v == streams[name] && name < other {
				add("%s and %s must differ, both are %q", name, other, v)
			}
		}
	}
	for _, name := range []string{"otlp.span_subject", "otlp.pending_subject"} {
		if v := subjects[name]; v == "" || strings.ContainsAny(v, " *>") {
			add("%s must be a non-empty subject without spaces or wildcards, got %q", name, v)
		}
		// Span and marker subjects get a ".<trace id>" suffix, so neither
		// may sit under or over another subject in use.
		for other, v := range subjects {
			if other == name || v == "" || subjects[name] == "" {
				continue
			}
			if v == subjects[name] || strings.HasPrefix(v, subjects[name]+".") {
				add("%s (%q) must not equal or be a prefix of %s (%q)", name, subjects[name], other, v)
			}
		}
	}
}

// WorstCase is the longest a single scoring call can take: every attempt
// running to its timeout, plus the full backoff between attempts.
func (s Scorer) WorstCase() time.Duration {
	total := time.Duration(s.MaxRetries+1) * s.Timeout.Std()
	for i := 0; i < s.MaxRetries; i++ {
		switch n := len(s.Backoff); {
		case n > 0:
			total += s.Backoff[min(i, n-1)].Std()
		default:
			total += min(500*time.Millisecond<<min(i, 6), 30*time.Second)
		}
	}
	return total
}

func (c *Config) validateObservability(add func(format string, args ...any)) {
	for i, sink := range c.Observability.ErrorSinks {
		switch sink.Kind {
		case SinkStdout:
		case SinkFile:
			if sink.Path == "" {
				add("observability.error_sinks[%d]: a file sink needs a path", i)
			}
		default:
			add("observability.error_sinks[%d].kind must be %q or %q, got %q", i, SinkStdout, SinkFile, sink.Kind)
		}
	}
}

// exporterName keeps a name safe as a NATS subject token and stream suffix.
var exporterName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// exporterStreamName is the part of the export stream name an exporter adds.
func exporterStreamName(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func (c *Config) validateExporters(add func(format string, args ...any)) {
	seen := map[string]bool{}
	for i, e := range c.Exporters {
		stream := exporterStreamName(e.Name)
		switch {
		case !exporterName.MatchString(e.Name):
			add("exporters[%d].name %q must be letters, digits, '-' and '_' only", i, e.Name)
		case seen[stream]:
			add("exporters[%d].name %q is used twice (names differing only in case or '-'/'_' count as the same)", i, e.Name)
		}
		seen[stream] = true
		if err := checkURL(e.Endpoint); err != nil {
			add("exporters[%d].endpoint: %w", i, err)
		}
		if e.QueueSize < 0 || e.Concurrency < 0 || e.MaxDeliver < 0 {
			add("exporters[%d]: queue_size, concurrency and max_deliver must not be negative", i)
		}
		for j, d := range e.Backoff {
			if d < 0 {
				add("exporters[%d].backoff[%d] must not be negative", i, j)
			}
		}
		for k := range e.ResolvedHeaders {
			if strings.TrimSpace(k) == "" {
				add("exporters[%d].headers: a header name must not be empty", i)
			}
		}
	}
}
