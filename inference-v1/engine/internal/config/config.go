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
	EnvScorerEndpoint = "OBSERVER_SCORER_ENDPOINT"
	EnvExportEndpoint = "OBSERVER_EXPORT_ENDPOINT" // the first exporter's endpoint
	EnvNATSURL        = "OBSERVER_NATS_URL"
	EnvManifest       = "OBSERVER_MANIFEST"
	EnvOTLPGRPCAddr   = "OBSERVER_OTLP_GRPC_ADDR"
	EnvOTLPHTTPAddr   = "OBSERVER_OTLP_HTTP_ADDR"

	questionTypeNoul = "noul"

	// Scorer kinds. Both speak the /v1/systemone protocol; jev is TypeSafe's
	// hosted model and needs an API key.
	ScorerKev = "kev"
	ScorerJev = "jev"

	// DefaultJevAPIKeyEnv holds Jev's API key when scorer.api_key_env is unset.
	DefaultJevAPIKeyEnv = "JEV_API_KEY"

	// Error sink kinds.
	SinkStdout = "stdout"
	SinkFile   = "file"
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
	Server    Server     `yaml:"server"`
	Queue     Queue      `yaml:"queue"`
	OTLP      OTLP       `yaml:"otlp"`
	Scorer    Scorer     `yaml:"scorer"`
	Policy    Policy     `yaml:"policy"`
	Alert     Alert      `yaml:"alert"`
	Exporters []Exporter `yaml:"exporters"`

	Observability Observability `yaml:"observability"`

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

// Queue configures the work queue between the receivers and the workers,
// with its dead-letter stream, and the export queues. With URL set they all
// live in NATS JetStream; with URL empty, in process, where only
// max_messages, max_deliver and redelivery_backoff apply.
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

	// ExportStream prefixes each exporter's work-queue stream,
	// <export_stream>_<NAME>, on subject <export_subject>.<name>. Traces an
	// exporter gave up on go to ExportDLQStream on ExportDLQSubject.
	ExportStream     string `yaml:"export_stream"`
	ExportSubject    string `yaml:"export_subject"`
	ExportDLQStream  string `yaml:"export_dlq_stream"`
	ExportDLQSubject string `yaml:"export_dlq_subject"`
}

// NATS reports whether the queues live in NATS JetStream.
func (q Queue) NATS() bool { return q.URL != "" }

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

// Exporter is one OTLP/HTTP back end that kept traces are sent to: Phoenix,
// Langfuse, an OpenTelemetry Collector, or anything else that accepts OTLP
// traces over HTTP.
type Exporter struct {
	// Name identifies the back end in logs and errors.
	Name string `yaml:"name"`
	// Endpoint is the full OTLP/HTTP traces URL, path included.
	Endpoint string `yaml:"endpoint"`
	// Headers are sent with every request, as written.
	Headers map[string]string `yaml:"headers,omitempty"`
	// HeadersEnv names an environment variable holding more headers in the
	// OpenTelemetry format, "key=value,key2=value2", for secrets such as
	// auth tokens. They are added to Headers, replacing any of the same name.
	HeadersEnv string `yaml:"headers_env,omitempty"`
	// ResourceAttributes are set on the resource of every exported span,
	// e.g. openinference.project.name for a Phoenix project.
	ResourceAttributes map[string]string `yaml:"resource_attributes,omitempty"`
	// QueueSize bounds the traces waiting to be sent to this back end; past
	// it, a trace is not sent to it and the drop is reported. 0 means 1024.
	QueueSize int `yaml:"queue_size,omitempty"`
	// Concurrency is how many traces are sent to this back end at once. 0 means 4.
	Concurrency int `yaml:"concurrency,omitempty"`
	// MaxDeliver is how many times a trace is tried before it is given up
	// (dead-lettered on NATS). 0 means 3.
	MaxDeliver int `yaml:"max_deliver,omitempty"`
	// Backoff is the wait before each retry; the last value repeats.
	// Empty means [1s, 5s, 30s].
	Backoff []Duration `yaml:"backoff,omitempty"`

	// ResolvedHeaders are Headers plus those read from HeadersEnv, filled in
	// at load time; they are not part of the YAML.
	ResolvedHeaders map[string]string `yaml:"-"`
}

const (
	DefaultExportQueueSize   = 1024
	DefaultExportConcurrency = 4
)

// Observability configures where the engine's own errors are reported.
type Observability struct {
	// ErrorSinks lists every place an error is written. Each sink receives
	// every error.
	ErrorSinks []Sink `yaml:"error_sinks"`
}

// Sink is one place errors are written: stdout, or a file (with path).
type Sink struct {
	Kind string `yaml:"kind"`
	Path string `yaml:"path,omitempty"`
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
			URL:            "", // in process; set it to use NATS JetStream
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

			ExportStream:     "OBSERVER_EXPORTS",
			ExportSubject:    "observer.exports",
			ExportDLQStream:  "OBSERVER_EXPORTS_DLQ",
			ExportDLQSubject: "observer.exports_dlq",
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
		Exporters: []Exporter{{
			Name:               "phoenix",
			Endpoint:           "http://localhost:6006/v1/traces",
			ResourceAttributes: map[string]string{"openinference.project.name": "observer-ai"},
		}},
		Observability: Observability{
			ErrorSinks: []Sink{{Kind: SinkStdout}},
		},
	}
}
