// Package config loads and validates observer.yaml and the model manifest.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	EnvScorerEndpoint  = "OBSERVER_SCORER_ENDPOINT"
	EnvPhoenixEndpoint = "OBSERVER_PHOENIX_ENDPOINT"
	EnvNATSURL         = "OBSERVER_NATS_URL"
	EnvManifest        = "OBSERVER_MANIFEST"
	EnvOTLPGRPCAddr    = "OBSERVER_OTLP_GRPC_ADDR"
	EnvOTLPHTTPAddr    = "OBSERVER_OTLP_HTTP_ADDR"

	questionTypeNoul = "noul"
)

// Duration is a time.Duration that parses from YAML strings like "500ms".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"500ms\"", n.Line)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", n.Line, s)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

type Config struct {
	Server Server `yaml:"server"`
	Queue  Queue  `yaml:"queue"`
	OTLP   OTLP   `yaml:"otlp"`
	Scorer Scorer `yaml:"scorer"`
	Policy Policy `yaml:"policy"`
	Alert  Alert  `yaml:"alert"`
	Export Export `yaml:"export"`

	// Manifest is loaded from Scorer.Manifest; it is not part of the YAML.
	Manifest Manifest `yaml:"-"`
}

type Server struct {
	Addr            string   `yaml:"addr"`
	Workers         int      `yaml:"workers"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

// OTLP configures the OpenTelemetry receivers and how the spans they take in
// are assembled into traces.
type OTLP struct {
	// GRPCAddr and HTTPAddr are the listen addresses. An empty address
	// turns that protocol off; both empty turns OTLP ingestion off.
	GRPCAddr string `yaml:"grpc_addr"`
	HTTPAddr string `yaml:"http_addr"`

	// IdleTimeout is how long a trace must receive no spans before it is
	// assembled and scored.
	IdleTimeout Duration `yaml:"idle_timeout"`
	// MaxWait assembles a trace this long after its first span even if
	// spans are still arriving.
	MaxWait    Duration `yaml:"max_wait"`
	Assemblers int      `yaml:"assemblers"`

	SpanStream     string   `yaml:"span_stream"`
	SpanSubject    string   `yaml:"span_subject"`
	SpanMaxAge     Duration `yaml:"span_max_age"`
	PendingStream  string   `yaml:"pending_stream"`
	PendingSubject string   `yaml:"pending_subject"`
}

// Enabled reports whether any OTLP receiver is switched on.
func (o OTLP) Enabled() bool { return o.GRPCAddr != "" || o.HTTPAddr != "" }

// Queue configures the NATS JetStream work queue between the API and the
// workers, and its dead-letter stream.
type Queue struct {
	URL            string   `yaml:"url"`
	ConnectTimeout Duration `yaml:"connect_timeout"`

	Stream   string `yaml:"stream"`
	Subject  string `yaml:"subject"`
	Consumer string `yaml:"consumer"`
	// MaxMessages bounds the stream; publishes beyond it are refused.
	MaxMessages int64 `yaml:"max_messages"`

	// AckWait is how long a worker may hold a trace before it is redelivered.
	AckWait Duration `yaml:"ack_wait"`
	// MaxDeliver is the number of delivery attempts before a trace that
	// keeps hitting system errors is dead-lettered.
	MaxDeliver int `yaml:"max_deliver"`
	// RedeliveryBackoff[i] is the delay before delivery i+2; the last
	// entry repeats.
	RedeliveryBackoff []Duration `yaml:"redelivery_backoff"`

	DLQStream  string   `yaml:"dlq_stream"`
	DLQSubject string   `yaml:"dlq_subject"`
	DLQMaxAge  Duration `yaml:"dlq_max_age"`
}

type Scorer struct {
	Endpoint         string     `yaml:"endpoint"`
	Model            string     `yaml:"model"`
	Manifest         string     `yaml:"manifest"`
	DecisionQuestion string     `yaml:"decision_question"`
	Timeout          Duration   `yaml:"timeout"`
	MaxRetries       int        `yaml:"max_retries"`
	Backoff          []Duration `yaml:"backoff"`
}

type Policy struct {
	KeepThreshold float64 `yaml:"keep_threshold"`
	RandomFloor   float64 `yaml:"random_floor"`
}

type Alert struct {
	SeverityHigh float64 `yaml:"severity_high"`
}

type Export struct {
	PhoenixEndpoint string `yaml:"phoenix_endpoint"`
	Project         string `yaml:"project"`
}

// Manifest is the subset of the fine-tuning notebook's manifest.json that the
// engine uses.
type Manifest struct {
	Model string `json:"model"`
	// Questions is the manifest's questions object, compacted but otherwise
	// untouched, so it reaches Kev with the same keys in the same order the
	// model was trained on.
	Questions    json.RawMessage    `json:"questions"`
	Temperatures map[string]float64 `json:"temperatures"`

	// Temperature is the calibration temperature for the decision question
	// (1.0 when the manifest has none).
	Temperature float64 `json:"-"`
}

// Default returns the configuration used for any key the YAML omits.
func Default() Config {
	return Config{
		Server: Server{
			Addr:            ":8080",
			Workers:         8,
			ShutdownTimeout: Duration(30 * time.Second),
		},
		Queue: Queue{
			URL:            "nats://localhost:4222",
			ConnectTimeout: Duration(30 * time.Second),
			Stream:         "OBSERVER_TRACES",
			Subject:        "observer.traces",
			Consumer:       "observer-workers",
			MaxMessages:    100000,
			AckWait:        Duration(2 * time.Minute),
			MaxDeliver:     5,
			RedeliveryBackoff: []Duration{
				Duration(5 * time.Second),
				Duration(30 * time.Second),
				Duration(2 * time.Minute),
				Duration(5 * time.Minute),
			},
			DLQStream:  "OBSERVER_TRACES_DLQ",
			DLQSubject: "observer.traces.dlq",
			DLQMaxAge:  Duration(7 * 24 * time.Hour),
		},
		OTLP: OTLP{
			GRPCAddr:       ":4317",
			HTTPAddr:       ":4318",
			IdleTimeout:    Duration(10 * time.Second),
			MaxWait:        Duration(2 * time.Minute),
			Assemblers:     4,
			SpanStream:     "OBSERVER_SPANS",
			SpanSubject:    "observer.spans",
			SpanMaxAge:     Duration(time.Hour),
			PendingStream:  "OBSERVER_PENDING",
			PendingSubject: "observer.pending",
		},
		Scorer: Scorer{
			Endpoint:         "http://localhost:8010/v1/systemone",
			Model:            "kev-latest",
			DecisionQuestion: "answer_wrong",
			Timeout:          Duration(10 * time.Second),
			MaxRetries:       3,
			Backoff: []Duration{
				Duration(500 * time.Millisecond),
				Duration(2 * time.Second),
				Duration(5 * time.Second),
			},
		},
		Policy: Policy{KeepThreshold: 0.5, RandomFloor: 0.02},
		Alert:  Alert{SeverityHigh: 0.8},
		Export: Export{
			PhoenixEndpoint: "http://localhost:6006/v1/traces",
			Project:         "observer-ai",
		},
	}
}

// Load reads the YAML config at path, applies defaults and env overrides,
// loads the manifest (relative paths resolve against the config file's
// directory) and validates everything.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}

	if v := os.Getenv(EnvScorerEndpoint); v != "" {
		cfg.Scorer.Endpoint = v
	}
	if v := os.Getenv(EnvPhoenixEndpoint); v != "" {
		cfg.Export.PhoenixEndpoint = v
	}
	if v := os.Getenv(EnvNATSURL); v != "" {
		cfg.Queue.URL = v
	}
	if v := os.Getenv(EnvManifest); v != "" {
		cfg.Scorer.Manifest = v
	}
	if v := os.Getenv(EnvOTLPGRPCAddr); v != "" {
		cfg.OTLP.GRPCAddr = v
	}
	if v := os.Getenv(EnvOTLPHTTPAddr); v != "" {
		cfg.OTLP.HTTPAddr = v
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}

	manifestPath := cfg.Scorer.Manifest
	if !filepath.IsAbs(manifestPath) {
		manifestPath = filepath.Join(filepath.Dir(path), manifestPath)
	}
	m, err := LoadManifest(manifestPath, cfg.Scorer.DecisionQuestion)
	if err != nil {
		return nil, err
	}
	cfg.Manifest = *m
	return &cfg, nil
}

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

	if err := checkURL(c.Scorer.Endpoint); err != nil {
		add("scorer.endpoint: %w", err)
	}
	if c.Scorer.Model == "" {
		add("scorer.model must not be empty")
	}
	if c.Scorer.Manifest == "" {
		add("scorer.manifest is required")
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

	checkUnit := func(name string, v float64) {
		if !(v >= 0 && v <= 1) { // also rejects NaN
			add("%s must be in [0, 1], got %v", name, v)
		}
	}
	checkUnit("policy.keep_threshold", c.Policy.KeepThreshold)
	checkUnit("policy.random_floor", c.Policy.RandomFloor)
	checkUnit("alert.severity_high", c.Alert.SeverityHigh)

	if err := checkURL(c.Export.PhoenixEndpoint); err != nil {
		add("export.phoenix_endpoint: %w", err)
	}
	if c.Export.Project == "" {
		add("export.project must not be empty")
	}
	return errors.Join(errs...)
}

func (c *Config) validateQueue(add func(format string, args ...any)) {
	q := c.Queue
	if u, err := url.Parse(q.URL); err != nil || u.Host == "" || (u.Scheme != "nats" && u.Scheme != "tls") {
		add("queue.url: %q is not a nats:// or tls:// URL", q.URL)
	}
	if q.ConnectTimeout <= 0 {
		add("queue.connect_timeout must be positive, got %s", q.ConnectTimeout.Std())
	}
	for name, v := range map[string]string{
		"queue.stream": q.Stream, "queue.consumer": q.Consumer, "queue.dlq_stream": q.DLQStream,
	} {
		if v == "" || strings.ContainsAny(v, " .*>/\\") {
			add("%s must be a non-empty name without spaces, dots or wildcards, got %q", name, v)
		}
	}
	for name, v := range map[string]string{"queue.subject": q.Subject, "queue.dlq_subject": q.DLQSubject} {
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

func checkURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an http(s) URL", s)
	}
	return nil
}

// LoadManifest reads manifest.json and checks that decisionQuestion exists
// and is a noul question.
func LoadManifest(path, decisionQuestion string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	var questions map[string]json.RawMessage
	if err := json.Unmarshal(m.Questions, &questions); err != nil || len(questions) == 0 {
		return nil, fmt.Errorf("manifest %s: \"questions\" must be a non-empty object", path)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, m.Questions); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	m.Questions = compact.Bytes()

	q, ok := questions[decisionQuestion]
	if !ok {
		return nil, fmt.Errorf("manifest %s: decision question %q not found in questions", path, decisionQuestion)
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(q, &head); err != nil {
		return nil, fmt.Errorf("manifest %s: question %q: %w", path, decisionQuestion, err)
	}
	if head.Type != questionTypeNoul {
		return nil, fmt.Errorf("manifest %s: decision question %q has type %q, want %q",
			path, decisionQuestion, head.Type, questionTypeNoul)
	}

	m.Temperature = 1.0
	if t, ok := m.Temperatures[decisionQuestion]; ok {
		if !(t > 0) {
			return nil, fmt.Errorf("manifest %s: temperature for %q must be positive, got %v", path, decisionQuestion, t)
		}
		m.Temperature = t
	}
	return &m, nil
}
