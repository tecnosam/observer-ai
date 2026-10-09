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
	"time"

	"gopkg.in/yaml.v3"
)

const (
	EnvScorerEndpoint  = "OBSERVER_SCORER_ENDPOINT"
	EnvPhoenixEndpoint = "OBSERVER_PHOENIX_ENDPOINT"

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
	QueueSize       int      `yaml:"queue_size"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
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
			QueueSize:       1000,
			ShutdownTimeout: Duration(30 * time.Second),
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
	if c.Server.QueueSize < 1 {
		add("server.queue_size must be at least 1, got %d", c.Server.QueueSize)
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
