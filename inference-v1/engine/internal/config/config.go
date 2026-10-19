// Package config turns the engine's settings into a validated Config, and
// that Config into the settings each component takes.
//
//	config.go      the Config types and their defaults
//	loader.go      where settings come from: AbstractLoaderFactory and its implementations
//	compiler.go    Compiler: loaded settings to a validated Config (Load is the YAML shortcut)
//	validate.go    the rules a Config must meet
//	manifest.go    the model manifest
//	compile.go     a Config to each component's own settings
//	utils.go       small helpers the rest share
package config

import (
	"encoding/json"
	"fmt"
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

	// Scorer kinds. Both speak the /v1/systemone protocol; jev is TypeSafe's
	// hosted model and needs an API key.
	ScorerKev = "kev"
	ScorerJev = "jev"

	// DefaultJevAPIKeyEnv holds Jev's API key when scorer.api_key_env is unset.
	DefaultJevAPIKeyEnv = "JEV_API_KEY"
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
	// Kind is kev (a Kev server, no key by default) or jev (TypeSafe's
	// hosted Jev, which needs an API key).
	Kind     string `yaml:"kind"`
	Endpoint string `yaml:"endpoint"`
	Model    string `yaml:"model"`
	// APIKeyEnv names the environment variable holding the scorer's API key,
	// sent as a bearer token. Defaults to JEV_API_KEY for jev; unset means no
	// key for kev. The key itself never goes in the file.
	APIKeyEnv string `yaml:"api_key_env"`
	// APIKey is read from APIKeyEnv at load time; it is not part of the YAML.
	APIKey string `yaml:"-"`

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
			Kind:             ScorerKev,
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
