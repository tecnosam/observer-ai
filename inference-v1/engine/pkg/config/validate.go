package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// validate checks every value and returns all the problems at once.
func (c *Config) validate() error {
	p := &problems{}
	c.validateServer(p)
	validateScorer(p, "scorer", c.Scorer)
	c.validateAgents(p)
	c.validateQueue(p)
	c.validateOTLP(p)
	c.validateObservability(p)
	c.validatePolicy(p)
	c.validateExporters(p)
	return p.err()
}

func (c *Config) validateServer(p *problems) {
	p.nonEmpty("server.addr", c.Server.Addr)
	p.atLeast("server.workers", c.Server.Workers, 1)
	p.positive("server.shutdown_timeout", c.Server.ShutdownTimeout)
}

// validateScorer checks one scorer section; key prefixes every message.
func validateScorer(p *problems, key string, s Scorer) {
	if s.Kind != ScorerKev && s.Kind != ScorerJev {
		p.add("%s.kind must be %q or %q, got %q", key, ScorerKev, ScorerJev, s.Kind)
	}
	if s.APIKeyEnv != "" && s.APIKey == "" {
		p.add("%s.api_key_env: $%s is not set; it must hold the scorer's API key", key, s.APIKeyEnv)
	}
	p.url(key+".endpoint", s.Endpoint)
	p.nonEmpty(key+".model", s.Model)
	p.nonEmpty(key+".decision_question", s.DecisionQuestion)
	p.positive(key+".timeout", s.Timeout)
	p.atLeast(key+".max_retries", s.MaxRetries, 0)
	p.backoff(key+".backoff", s.Backoff)
}

func (c *Config) validateAgents(p *problems) {
	for i, a := range c.Agents.Attributes {
		p.nonEmpty(fmt.Sprintf("agents.attributes[%d]", i), a)
	}
	if len(c.Agents.Models) > 0 && len(c.Agents.Attributes) == 0 {
		p.add("agents.attributes must name at least one attribute when agents.models is set")
	}
	for agent, model := range c.Agents.Models {
		if strings.TrimSpace(agent) == "" {
			p.add("agents.models: an agent name must not be empty")
		}
		key := modelKey(agent)
		// manifest is required too, but may come as a value in code; the
		// manifest loader checks it.
		for field, v := range map[string]string{"endpoint": model.Endpoint, "model": model.Model} {
			if v == "" {
				p.add("%s.%s is required: an agent's model takes nothing from the scorer section", key, field)
			}
		}
		if model.Endpoint != "" && model.Model != "" {
			validateScorer(p, key, model)
		}
	}
}

func (c *Config) validateQueue(p *problems) {
	q := c.Queue
	if u, err := url.Parse(q.URL); q.NATS() && (err != nil || u.Host == "" || (u.Scheme != "nats" && u.Scheme != "tls")) {
		p.add("queue.url: %q is not a nats:// or tls:// URL (leave it empty to queue in process)", q.URL)
	}
	p.positive("queue.connect_timeout", q.ConnectTimeout)
	p.atLeast("queue.max_messages", int(q.MaxMessages), 1)
	p.atLeast("queue.max_deliver", q.MaxDeliver, 1)
	p.backoff("queue.redelivery_backoff", q.RedeliveryBackoff)
	p.positive("queue.dlq_max_age", q.DLQMaxAge)
	if q.Replicas < 1 || q.Replicas > 5 {
		p.add("queue.replicas must be between 1 and 5, got %d", q.Replicas)
	}
	c.validateQueueNames(p)
	c.validateAckWait(p)
	if q.NATS() {
		validateNATSAuth(p, q.Auth)
		validateNATSTLS(p, q.TLS)
	}
}

func validateNATSAuth(p *problems, a NATSAuth) {
	if set := a.methods(); len(set) > 1 {
		p.add("queue.auth: set one way to authenticate, got %s", strings.Join(set, ", "))
	}
	if (a.User == "") != (a.PasswordEnv == "") {
		p.add("queue.auth.user and queue.auth.password_env go together")
	}
	p.readable("queue.auth.credentials_file", a.CredentialsFile)
	p.readable("queue.auth.nkey_seed_file", a.NkeySeedFile)
}

func validateNATSTLS(p *problems, t NATSTLS) {
	if (t.CertFile == "") != (t.KeyFile == "") {
		p.add("queue.tls.cert_file and queue.tls.key_file go together")
	}
	p.readable("queue.tls.ca_file", t.CAFile)
	p.readable("queue.tls.cert_file", t.CertFile)
	p.readable("queue.tls.key_file", t.KeyFile)
}

func (c *Config) validateQueueNames(p *problems) {
	q := c.Queue
	p.streamNames(map[string]string{
		"queue.stream": q.Stream, "queue.consumer": q.Consumer, "queue.dlq_stream": q.DLQStream,
		"queue.export_stream": q.ExportStream, "queue.export_dlq_stream": q.ExportDLQStream,
	})
	p.subjects(map[string]string{
		"queue.subject": q.Subject, "queue.dlq_subject": q.DLQSubject,
		"queue.export_subject": q.ExportSubject, "queue.export_dlq_subject": q.ExportDLQSubject,
	})
	if q.Stream == q.DLQStream {
		p.add("queue.dlq_stream must differ from queue.stream")
	}
	if q.Subject == q.DLQSubject {
		p.add("queue.dlq_subject must differ from queue.subject")
	}
	if strings.HasPrefix(q.ExportDLQSubject, q.ExportSubject+".") {
		p.add("queue.export_dlq_subject must not be under queue.export_subject, where each exporter has its own subject")
	}
}

// validateAckWait makes sure a worker can finish every scoring attempt, on
// the slowest model, before the queue gives the trace to someone else, or
// traces get scored twice.
func (c *Config) validateAckWait(p *problems) {
	worst, key := c.Scorer.WorstCase(), "scorer"
	for agent, model := range c.Agents.Models {
		if w := model.WorstCase(); w > worst {
			worst, key = w, modelKey(agent)
		}
	}
	if c.Queue.AckWait.Std() <= worst {
		p.add("queue.ack_wait (%s) must exceed the longest possible scoring time (%s for %s: every attempt timing out plus backoff)",
			c.Queue.AckWait.Std(), worst, key)
	}
}

func (c *Config) validateOTLP(p *problems) {
	o := c.OTLP
	if !o.Enabled() {
		return
	}
	c.validateOTLPAddrs(p)
	p.positive("otlp.idle_timeout", o.IdleTimeout)
	if o.MaxWait < o.IdleTimeout {
		p.add("otlp.max_wait (%s) must be at least otlp.idle_timeout (%s)", o.MaxWait.Std(), o.IdleTimeout.Std())
	}
	// Spans must outlive the wait for their trace to complete.
	if o.SpanMaxAge <= o.MaxWait {
		p.add("otlp.span_max_age (%s) must exceed otlp.max_wait (%s)", o.SpanMaxAge.Std(), o.MaxWait.Std())
	}
	p.atLeast("otlp.assemblers", o.Assemblers, 1)
	c.validateOTLPStreams(p)
	c.validateOTLPSubjects(p)
}

func (c *Config) validateOTLPAddrs(p *problems) {
	o := c.OTLP
	if o.GRPCAddr != "" && o.GRPCAddr == o.HTTPAddr {
		p.add("otlp.grpc_addr and otlp.http_addr must differ, both are %q", o.GRPCAddr)
	}
	for name, addr := range map[string]string{"otlp.grpc_addr": o.GRPCAddr, "otlp.http_addr": o.HTTPAddr} {
		if addr != "" && addr == c.Server.Addr {
			p.add("%s must differ from server.addr, both are %q", name, addr)
		}
	}
}

// validateOTLPStreams checks the span streams' names, and that they share
// none with the queue's streams.
func (c *Config) validateOTLPStreams(p *problems) {
	o := c.OTLP
	streams := map[string]string{
		"otlp.span_stream": o.SpanStream, "otlp.pending_stream": o.PendingStream,
		"queue.stream": c.Queue.Stream, "queue.dlq_stream": c.Queue.DLQStream,
	}
	p.streamNames(map[string]string{"otlp.span_stream": o.SpanStream, "otlp.pending_stream": o.PendingStream})
	for _, name := range []string{"otlp.span_stream", "otlp.pending_stream"} {
		for other, v := range streams {
			if other != name && v == streams[name] && name < other {
				p.add("%s and %s must differ, both are %q", name, other, v)
			}
		}
	}
}

// validateOTLPSubjects checks the span subjects. Span and marker subjects
// get a ".<trace id>" suffix, so neither may sit under or over another
// subject in use.
func (c *Config) validateOTLPSubjects(p *problems) {
	o := c.OTLP
	subjects := map[string]string{
		"otlp.span_subject": o.SpanSubject, "otlp.pending_subject": o.PendingSubject,
		"queue.subject": c.Queue.Subject, "queue.dlq_subject": c.Queue.DLQSubject,
	}
	p.subjects(map[string]string{"otlp.span_subject": o.SpanSubject, "otlp.pending_subject": o.PendingSubject})
	for _, name := range []string{"otlp.span_subject", "otlp.pending_subject"} {
		mine := subjects[name]
		for other, v := range subjects {
			if other == name || v == "" || mine == "" {
				continue
			}
			if v == mine || strings.HasPrefix(v, mine+".") {
				p.add("%s (%q) must not equal or be a prefix of %s (%q)", name, mine, other, v)
			}
		}
	}
}

func (c *Config) validateObservability(p *problems) {
	c.validateErrorSinks(p)
	m := c.Observability.Metrics
	if !m.Enabled() {
		return
	}
	p.url("observability.metrics.endpoint", m.Endpoint)
	p.positive("observability.metrics.interval", m.Interval)
	p.headerNames("observability.metrics.headers", m.ResolvedHeaders)
}

func (c *Config) validateErrorSinks(p *problems) {
	for i, sink := range c.Observability.ErrorSinks {
		switch sink.Kind {
		case SinkStdout:
		case SinkFile:
			if sink.Path == "" {
				p.add("observability.error_sinks[%d]: a file sink needs a path", i)
			}
		default:
			p.add("observability.error_sinks[%d].kind must be %q or %q, got %q", i, SinkStdout, SinkFile, sink.Kind)
		}
	}
}

func (c *Config) validatePolicy(p *problems) {
	p.unit("policy.keep_threshold", c.Policy.KeepThreshold)
	p.unit("policy.random_floor", c.Policy.RandomFloor)
	p.unit("alert.severity_high", c.Alert.SeverityHigh)
}

// exporterName keeps a name safe as a NATS subject token and stream suffix.
var exporterName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// exporterStreamName is the part of the export stream name an exporter adds.
func exporterStreamName(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func (c *Config) validateExporters(p *problems) {
	seen := map[string]bool{}
	for i, e := range c.Exporters {
		key := fmt.Sprintf("exporters[%d]", i)
		stream := exporterStreamName(e.Name)
		switch {
		case !exporterName.MatchString(e.Name):
			p.add("%s.name %q must be letters, digits, '-' and '_' only", key, e.Name)
		case seen[stream]:
			p.add("%s.name %q is used twice (names differing only in case or '-'/'_' count as the same)", key, e.Name)
		}
		seen[stream] = true
		validateExporter(p, key, e)
	}
}

func validateExporter(p *problems, key string, e Exporter) {
	p.url(key+".endpoint", e.Endpoint)
	p.atLeast(key+".queue_size", e.QueueSize, 0)
	p.atLeast(key+".concurrency", e.Concurrency, 0)
	p.atLeast(key+".max_deliver", e.MaxDeliver, 0)
	p.backoff(key+".backoff", e.Backoff)
	p.headerNames(key+".headers", e.ResolvedHeaders)
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

// problems collects every validation failure, so all are reported at once.
// Its helpers each check one common kind of rule.
type problems struct{ errs []error }

func (p *problems) add(format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf(format, args...))
}

func (p *problems) err() error { return errors.Join(p.errs...) }

func (p *problems) nonEmpty(key, v string) {
	if v == "" {
		p.add("%s must not be empty", key)
	}
}

func (p *problems) positive(key string, d Duration) {
	if d <= 0 {
		p.add("%s must be positive, got %s", key, d.Std())
	}
}

func (p *problems) atLeast(key string, v, least int) {
	if v >= least {
		return
	}
	if least == 0 {
		p.add("%s must not be negative, got %d", key, v)
		return
	}
	p.add("%s must be at least %d, got %d", key, least, v)
}

// unit checks v is a probability; NaN fails too.
func (p *problems) unit(key string, v float64) {
	if !(v >= 0 && v <= 1) {
		p.add("%s must be in [0, 1], got %v", key, v)
	}
}

func (p *problems) url(key, v string) {
	if err := checkURL(v); err != nil {
		p.add("%s: %w", key, err)
	}
}

func (p *problems) backoff(key string, ds []Duration) {
	for i, d := range ds {
		if d < 0 {
			p.add("%s[%d] must not be negative, got %s", key, i, d.Std())
		}
	}
}

func (p *problems) headerNames(key string, headers map[string]string) {
	for k := range headers {
		if strings.TrimSpace(k) == "" {
			p.add("%s: a header name must not be empty", key)
		}
	}
}

// readable checks that a configured file can be opened, so a wrong path
// fails at start-up rather than at the first reconnect.
func (p *problems) readable(key, path string) {
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		p.add("%s: %w", key, err)
		return
	}
	f.Close()
}

// streamNames checks NATS stream (and consumer) names.
func (p *problems) streamNames(names map[string]string) {
	for key, v := range names {
		if v == "" || strings.ContainsAny(v, " .*>/\\") {
			p.add("%s must be a non-empty name without spaces, dots or wildcards, got %q", key, v)
		}
	}
}

// subjects checks NATS subjects: dots allowed, wildcards not.
func (p *problems) subjects(subjects map[string]string) {
	for key, v := range subjects {
		if v == "" || strings.ContainsAny(v, " *>") {
			p.add("%s must be a non-empty subject without spaces or wildcards, got %q", key, v)
		}
	}
}
