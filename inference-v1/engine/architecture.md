# Architecture of the inference engine

How the Go service in this directory is put together: the stack, the code
layout, the design patterns it uses and the ones it deliberately does not,
and how `observer.yaml` becomes the running configuration.

The [README](README.md) covers what the engine does and how to run it. This
file is about the code.

## Short answers

| Question | Answer |
| --- | --- |
| Web framework? | None. The standard library's `net/http` and its `ServeMux`. No Gin, Echo, Fiber or chi. |
| Dependency injection? | Yes, by hand: constructors take what they need, and `main` builds everything once. No DI framework or container. |
| Factory pattern? | Abstract factories where there is a choice: where settings come from (`config.AbstractLoaderFactory`: a YAML file, or values built in code), which error sinks to build (`observe.AbstractSinkFactory`), which trace back ends to export to (`export.AbstractBackendFactory`), and where the queues live (`broker.AbstractFactory`: NATS JetStream or in process, building the trace queue and the export queues as a matching pair). Elsewhere, constructor functions (`scorer.New`, `policy.New`). |
| How are dependencies swapped in tests? | Small interfaces declared by the package that uses them, plus a few function-typed fields. |
| Config? | A `Compiler` with an injected loader factory. For the YAML file: `gopkg.in/yaml.v3`, decoded strictly over a struct of defaults, then six environment overrides, then validation. Read once at start-up. |
| Database? | None. The only state is the queues and the spans waiting to be assembled: in NATS JetStream when `queue.url` is set, otherwise in memory. |
| Logging? | `log/slog`, JSON to stdout, for events (start-up, decisions, shutdown). Errors go through their own pipeline: typed `fault.Error`s reported to `observe`, which writes them to the configured sinks. |

## Stack

| Concern | What is used |
| --- | --- |
| Language | Go 1.26, one static binary (`CGO_ENABLED=0`) |
| JSON API | `net/http` with `http.ServeMux` method patterns (`"POST /v1/traces"`), available since Go 1.22 |
| OTLP over HTTP | A second `net/http` server on its own port |
| OTLP over gRPC | `google.golang.org/grpc`, serving the standard OTLP `TraceService` |
| Wire formats | `google.golang.org/protobuf` and the generated OTLP types in `go.opentelemetry.io/proto/otlp` |
| Queue and storage | NATS JetStream through `github.com/nats-io/nats.go` (the `jetstream` package), or in-process channels and maps when no NATS URL is set |
| Calling Kev | `net/http` client, JSON |
| Sending to trace back ends | The OpenTelemetry Go SDK with the `otlptracehttp` exporter, one per configured back end (Phoenix, Langfuse, any OTLP/HTTP endpoint) |
| Config | `gopkg.in/yaml.v3` for `observer.yaml`, `encoding/json` for the model manifest |
| API docs | A hand-written OpenAPI file embedded with `go:embed`; Swagger UI loaded from a CDN |
| Tests | The standard `testing` package, `net/http/httptest`, and a real NATS server started in-process (`github.com/nats-io/nats-server/v2`, test-only) |

There is no ORM, no router library, no validation library, no CLI library
(two flags, read with `flag`) and no mocking library.

## Directory structure

```
cmd/observer/main.go     the only executable: flags, config, wiring, shutdown
pkg/                     the reusable engine: import these to embed it in another Go program
  pipeline/              the core: domain types, the interfaces the engine needs, model routing, calibration, and Engine
  trace/                 the Trace record (with its agent), its JSON decoding, and BuildState (the text Kev reads)
  fault/                 the error type: kind, component, operation, trace; constructors and classifiers
  observe/               where errors go: Reporter, the error pipeline, sinks (stdout, file) and their factory
  metrics/               the engine's own OpenTelemetry metrics: Recorder, its OTLP pipeline, and Noop
  config/                settings and manifests: loaders, compiler, validation, per-component settings
  scorer/                model adapter: /v1/systemone client, its factory, and the per-agent model builder
  policy/                keep or drop from a probability: threshold, random floor, severity
  broker/                chooses where the queues live: the NATS or in-process family of queues
  queue/                 the trace work queue and span store: AbstractBroker, on JetStream (Queue) or in memory (MemoryBroker)
    queuetest/           starts an in-process NATS server for tests
  export/                sends kept traces to every configured back end over OTLP, from a queue per back end
internal/                this binary's own pieces: not importable from another module
  server/                JSON API handlers: HTTP in, pipeline calls, HTTP out
  otlp/                  OTLP gRPC and HTTP receivers; turning spans into a Trace
  common/                what several packages share and no caller fills in: the NATS connection and pull
                         loop, a worker pool, retry policies, HTTP request metrics, small helpers
testdata/                trace fixtures, golden state strings, canned Kev responses
scripts/                 send_samples.py
observer.yaml            example config, also baked into the Docker image
observer.schema.yaml     JSON Schema for observer.yaml
```

`pkg/` is the public surface: a program can build a `config.Config` in code,
compile it, and assemble an `Engine` from its own `Scorer`, `Policy` or
`Exporter`, or from the factories here. `internal/` is ours: the HTTP and
OTLP servers that make up this binary, and `common`. The rule for `common`:
a public package may use it inside, but no public signature asks a caller to
build one of its types. Public settings stay plain DTOs (`queue.Config`
carries `MaxDeliver` and `RedeliveryBackoff`), and the package turns them
into `common.RetryPolicy` itself. The one exception is the NATS connection
(`common.Conn`), which only `broker.New` creates.

### Which package imports which

```
fault    (nothing)          trace   (nothing)
observe  -> fault
pipeline -> fault, observe, trace                  the core
metrics  -> fault, pipeline
common   -> fault, observe, metrics                internal helpers
policy   -> pipeline                               adapters implement the core's interfaces
scorer   -> fault, observe, pipeline, trace
queue    -> common, fault, observe, trace
export   -> common, fault, metrics, observe, otlp, pipeline
broker   -> common, export, queue
otlp     -> common, fault, metrics, observe, trace
server   -> common, fault, metrics, observe, pipeline, trace
config   -> broker, export, metrics, observe, pipeline, policy, queue, scorer, trace
main     -> all of them
```

Three things are worth noticing:

- **The core imports no adapter.** `pipeline` defines what it needs
  (`Scorer`, `Policy`, `Exporter`, `Publisher`, `Metrics`) and the types they
  exchange (`Score`, `Decision`, `Record`). The scorer, policy and exporter
  import it to implement those, never the other way round, and `server`
  depends on the core alone, not on the queue, scorer or exporter.

- **Only `main` imports `config`.** No other package knows the YAML exists.
  Each package that needs settings declares its own `Config` struct
  (`scorer.Config`, `queue.Config`, `policy.Config`). `config` builds those
  structs (`compile.go`), so it depends on the components and never the other
  way round. A component can therefore be tested, or reused, without a config
  file.
- **`trace` is the shared vocabulary.** The receivers, the queue, the
  pipeline and the exporter all pass a `trace.Trace` around; none of them
  depends on another's internals to do so.

## How a trace flows through the code

```
JSON  POST /v1/traces        server.handleEnqueue -> queue.Publish ─────────────────────┐
OTLP  gRPC / HTTP            otlp.Receiver -> queue.PublishSpans                         │
                                └─ queue.Assemblers -> otlp.Assembler -> queue.Publish ──┤
                                                                                         v
                                                       JetStream work queue (OBSERVER_TRACES)
                                                                                         │
                             queue.Workers -> pipeline.Engine.ProcessQueued <────────────┘
                                                 ModelRouter.Route(trace.Agent) -> the agent's Model
                                                 trace.BuildState -> Model.Scorer.Score
                                                 -> pipeline.Calibrate (Model.Temperature) -> Policy.Decide
                                                 -> Exporter.Export (kept traces only: queued per back end,
                                                    sent by that back end's workers)

JSON  POST /v1/traces:score  server.handleScore -> pipeline.Engine.Process   (same pipeline, no queue)
```

`pipeline.Engine` is the pipeline. Both the synchronous endpoint and the queue
workers call it; the only difference is what happens when Kev fails (see
[Error handling](#error-handling)).

## From observer.yaml to running configuration

Go does not "compile" the YAML ahead of time. At start-up a `Compiler` turns
settings from a source into one validated `Config`. Where the settings come
from is injected through an abstract factory, so the same compiler serves a
YAML file or values built in code:

```
pkg/config/
  config.go      the Config types and Default()
  loader.go      AbstractLoaderFactory -> AbstractConfigLoader, AbstractManifestLoader
                 YAMLFileFactory: observer.yaml and the manifest file it names
                 StaticFactory:   a Config and a Manifest built in code
  compiler.go    Compiler: the steps below. Load(path) is the YAML shortcut main uses
  validate.go    the rules a Config must meet
  manifest.go    reading and checking manifest.json
  compile.go     a Config to scorer.Config, queue.Config, policy.Config
  utils.go       small shared helpers
```

```go
cfg, err := config.Load("observer.yaml")                    // what the binary does
cfg, err := config.NewCompiler(config.NewStaticFactory(c, m)).Compile()   // settings built in code
cfg, err := config.NewCompiler(f, config.WithEnv(nil)).Compile()          // any factory, no environment
```

`Compiler.Compile` runs in this order:

1. **Load the settings, starting from defaults.** `config.Default()` returns a
   fully populated `Config` struct; every default lives in that one function.
   The YAML loader decodes the file over it: `yaml.v3` fills in only the keys
   the file
   contains, so anything left out keeps its default. The struct fields carry
   `yaml:"..."` tags that give the key names. Decoding is strict
   (`KnownFields(true)`): a misspelt or unknown key stops the engine with the
   line number instead of being silently ignored.
2. **Apply environment overrides.** Six variables replace their keys when
   set and non-empty: `OBSERVER_NATS_URL`, `OBSERVER_SCORER_ENDPOINT`,
   `OBSERVER_EXPORT_ENDPOINT` (the first exporter's endpoint), `OBSERVER_MANIFEST`,
   `OBSERVER_OTLP_GRPC_ADDR` and `OBSERVER_OTLP_HTTP_ADDR`. These are the
   values that differ between a laptop and a container. Nothing else can be
   set from the environment. The environment itself is injected
   (`WithEnv`, `os.Getenv` by default), so tests and embedding programs
   control it.
3. **Read the secrets from the environment.** The scorer's API key from the
   variable `scorer.api_key_env` names (`JEV_API_KEY` by default for
   `scorer.kind: jev`), and each exporter's headers from the variable its
   `headers_env` names, parsed in the OpenTelemetry `key=value,...` format
   and merged over its inline `headers`.
4. **Validate.** `validate` checks every value and collects all the
   problems before reporting (`errors.Join`), so one run shows every mistake.
   It includes rules that span keys, for example `queue.ack_wait` must be
   longer than the worst-case Kev call computed from `scorer.timeout`,
   `scorer.max_retries` and `scorer.backoff`.
5. **Load the manifest** through the factory's manifest loader. For a YAML
   file, `scorer.manifest` points at the fine-tuning run's `manifest.json` (a
   relative path resolves against the config file's directory); a
   `StaticFactory` can carry the manifest as a value instead. The engine takes the `questions` object, kept byte-for-byte in
   the order the model was trained on, and the temperature for
   `scorer.decision_question`, and checks that question exists and is a
   `noul`.

The result is one immutable `*config.Config`. `run` in `cmd/observer/main.go`
then hands each component only the part it needs, in the shape it takes:

```go
m, err := metrics.Start(ctx, cfg.MetricsConfig())
models, err := scorer.BuildRouter(scorer.HTTPFactory{}, cfg.Models(reporter))  // one model per agent
b, err := broker.New(cfg.BrokerConfig(logger))          // NATS if queue.url is set, else memory
q, err := b.Ingest(ctx, cfg.QueueConfig(logger))
exp, err := export.NewDispatcher(ctx, export.DispatcherConfig{Queues: b.ExportQueues(cfg.ExportStreams()), ...})
policy.New(cfg.PolicyConfig(), nil)
```

`config` and the components live in `pkg/`, so a program in another module
can do the same: build a `config.Config` with `StaticFactory`, compile it,
and wire the components itself, with its own implementations where it
wants them.

Details worth knowing:

- **Durations** are written as strings (`10s`, `2m`). `config.Duration` is a
  small type with its own `UnmarshalYAML` that calls `time.ParseDuration`;
  `.Std()` converts it to a `time.Duration` at the boundary, so no other
  package sees the YAML type.
- **No reload.** The config is read once. Changing it means restarting the
  engine.
- **No globals.** The config is passed down as arguments; nothing reads it
  from a package-level variable.
- **Two flags** sit outside the file: `-config` (path, default
  `observer.yaml`) and `-log-level`.

[observer.schema.yaml](observer.schema.yaml) describes every key, with its
type, default and limits. A test (`pkg/config/schema_test.go`) fails if
the schema and the Go structs disagree on a key or a default.

## Design patterns in use

### Manual dependency injection, with one composition root

Every component receives its collaborators through its constructor or its
struct fields. None of them creates its own dependencies or reaches for a
global. `run` in `main.go` is the one place where concrete types are chosen
and connected:

```go
a.engine = pipeline.NewEngine(pipeline.Deps{
    Models: models, Policy: policy.New(...), Exporter: a.exporter, Reporter: a.reporter, Metrics: a.metrics,
})
srv := server.New(server.Options{Engine: a.engine, Queue: a.queue, Reporter: a.reporter, Metrics: a.metrics})
```

`main` builds them in small steps on an `app` struct (`startMetrics`,
`startReporter`, `startBroker`, `startQueue`, `startExporter`,
`startEngine`, ...), so a failure part-way
stops exactly what was started.

There is no DI framework (no Wire, Fx or Dig). With about ten objects, wiring
by hand is shorter than configuring a container, and the start-up order is
readable top to bottom.

### Interfaces defined by the consumer

A package declares an interface for what it needs, next to the code that
uses it, and the concrete type lives elsewhere. All of them are small:

| Interface | Declared in | Methods | Real implementation |
| --- | --- | --- | --- |
| `pipeline.Scorer` | `pipeline/interfaces.go` | `Score`, `Ready` | `*scorer.Client` |
| `pipeline.ModelRouter` | `pipeline/model.go` | `Route`, `Ready` | `*pipeline.AgentRouter` |
| `scorer.AbstractScorerFactory` | `scorer/factory.go` | `Build` | `scorer.HTTPFactory` (Kev, Jev) |
| `pipeline.Policy` | `pipeline/interfaces.go` | `Decide`, `Fallback` | `*policy.Policy` |
| `pipeline.Exporter` | `pipeline/interfaces.go` | `Export`, `Flush` | `*export.Dispatcher` |
| `export.AbstractBackend` | `export/backend.go` | `Send`, `Flush`, `Shutdown` | `*export.OTLPBackend` |
| `export.AbstractBackendFactory` | `export/backend.go` | `Build` | `export.OTLPBackendFactory` |
| `pipeline.Publisher` | `pipeline/interfaces.go` | `Publish`, `Ready` | `queue.AbstractBroker` |
| `queue.AbstractBroker` | `queue/broker.go` | `Publish`, `PublishSpans`, `StartWorkers`, `StartAssemblers`, `Ready`, `Close` | `*queue.Queue` (JetStream), `*queue.MemoryBroker` |
| `export.AbstractQueue` | `export/queue.go` | `Enqueue`, `Flush`, `Shutdown` | JetStream and in-memory queues |
| `export.AbstractQueueFactory` | `export/queue.go` | `Queue` | `export.NATSQueueFactory`, `export.MemoryQueueFactory` |
| `broker.AbstractFactory` | `broker/broker.go` | `Ingest`, `ExportQueues`, `Kind`, `Close` | `*broker.NATS`, `broker.Memory` |
| `pipeline.Metrics` | `pipeline/interfaces.go` | `ScoreFinished`, `TraceDecided` | `*metrics.Recorder`, `metrics.Noop` |
| `export.SendMetrics` | `export/dispatcher.go` | `SendFinished` | `*metrics.Recorder`, `metrics.Noop` |
| `common.RequestMetrics` | `common/httpmetrics.go` | `RequestFinished` | `*metrics.Recorder`, `metrics.Noop` |
| `observe.ErrorCounter` | `observe/reporter.go` | `ErrorReported` | `*metrics.Recorder`, `metrics.Noop` |
| `observe.Reporter` | `observe/reporter.go` | `Report` | `*observe.Pipeline` |
| `observe.AbstractSink` | `observe/sink.go` | `Write`, `Close` | stdout and file sinks |
| `otlp.Sink` | `otlp/receiver.go` | `PublishSpans` | `queue.AbstractBroker` |
| `export.Uploader` | `export/otlp_backend.go` | `UploadTraces` | the OTLP/HTTP client |

This is what lets `otlp` store spans in the queue without importing the
`queue` package, and lets the tests replace Kev or a trace back end with a
few lines of fake.

### Functions as injection points

Where an interface would have a single method, a function type is used
instead:

- `queue.Handler`: what a worker does with a trace. `main` passes
  `engine.ProcessQueued`.
- `queue.AssembleFunc`: how spans become a trace. `main` passes
  `otlp.Assembler{AgentAttributes: ...}.Assemble`. This keeps `queue` free
  of any OTLP knowledge.
- `export.SendFunc`: how a queued record reaches its back end. The
  dispatcher wraps each back end's `Send` once, to time it for metrics, so
  both queue implementations report the same way.
- `policy.New(cfg, rnd)`: the random source, so tests can make the random
  floor deterministic.
- `scorer.Config.Sleep` and `Jitter`: so retry tests run without waiting.

### Constructor functions

Each component has a `New`-style function that validates its input, fills in
defaults for optional fields (a nil logger becomes `slog.Default()`, a nil
HTTP client a new one) and returns a ready value: `scorer.New`,
`policy.New`, `server.New`, `otlp.NewReceiver`, `export.NewDispatcher`.
These are plain constructors: each of these components has one
implementation, so there is nothing to choose between yet.

### One model per agent: a router in the core

The engine is meant to run one fine-tuned model per agent. A trace carries
its agent's name (`trace.Agent`): `otlp.Assembler` reads it from the span
attributes `agents.attributes` lists, the JSON API from the trace's `agent`
field. `pipeline.Engine` asks its `ModelRouter` for the trace's
`pipeline.Model` (a scorer, its name, its calibration temperature) and
scores and calibrates with that. `pipeline.AgentRouter` maps agent names to
models with a default for everything else; `pipeline.SingleModel` is the
one-model case.

The models are built from settings in two steps that keep `config` out of
the runtime: `config.Config.Models` turns the scorer section and each
`agents.models` entry into `scorer.Models` (plain DTOs), and
`scorer.BuildRouter` builds each one with an `AbstractScorerFactory` and
returns the router. A model server with another protocol is a new factory.

### Metrics through small hooks

Every package that has something worth measuring declares a one-method
hook for it: `pipeline.Metrics` (model calls, decisions),
`export.SendMetrics` (sends to a back end), `common.RequestMetrics`
(requests the API and the OTLP receivers answer, recorded by
`common.InstrumentHTTP` and the receiver's gRPC interceptor), and
`observe.ErrorCounter` (reported errors). `metrics.Recorder` implements all
of them on an OpenTelemetry meter; `metrics.Start` wires it to an OTLP/HTTP
exporter, or to a no-op meter when no endpoint is configured, and
`metrics.Noop` stands in for tests. Each hook takes a small struct
(`metrics.Request`, `metrics.Send`, `pipeline.ScoreResult`) rather than a
list of arguments.

### Abstract factory for configuration

Settings can come from more than one place: the YAML file the binary reads,
or values a program builds in code. `config.AbstractLoaderFactory` builds the
pair of loaders for one source, a config loader and a manifest loader, so the
two always agree (a YAML file's relative manifest path resolves against the
file; a `StaticFactory` can carry the manifest as a value). `config.Compiler`
is given a factory and an environment lookup and never knows which source it
has. A new source (flags, a database, a remote store) is a new factory, with
no change to the compiler or to validation.

### Abstract factory for trace back ends

`export.Dispatcher` is the engine's `pipeline.Exporter`. It is given a list
of `export.BackendConfig`s and an `export.AbstractBackendFactory`, builds one
`export.AbstractBackend` per entry, and gives each its own bounded queue and
workers. `Export` only enqueues, so a slow back end never holds up scoring
or the others; a full queue skips the trace for that back end and reports
it. `export.OTLPBackendFactory` builds an `OTLPBackend` for any OTLP/HTTP
endpoint, which is why Phoenix and Langfuse need no code of their own. A back
end that does not speak OTLP is a new `AbstractBackend` and factory, with no
change to the dispatcher or the pipeline. Tests pass a factory that hands
out an in-memory back end.

### Abstract factory for the queues

`broker.AbstractFactory` builds a family of related objects that must agree:
the trace work queue and span store (`queue.AbstractBroker`) and the export
queues (`export.AbstractQueueFactory`). `broker.New` picks the family from
`queue.url`: `broker.NATS` opens one connection (`common.Conn`) and builds
`queue.Queue` and `export.NATSQueueFactory` on it; `broker.Memory` builds
`queue.MemoryBroker` and `export.MemoryQueueFactory`. Nothing downstream
knows which it has: the server publishes to a `pipeline.Publisher`, the OTLP
receiver stores spans in an `otlp.Sink`, the dispatcher enqueues to an
`export.AbstractQueue`. Both families keep the same rules (bounded queues,
redelivery with backoff, a delivery limit, then a dead letter), and the
retry decision for exports (`export.settle`) is shared code. A new broker
(Kafka, Redis Streams) is a new family: one more factory and its two
implementations.

### Config structs, and functional options in one place

Components take a `Config` or `Options` struct rather than a long argument
list. The exporter is the one place that uses functional options
(`export.New(tp, export.WithForwarder(...))`),
because its extras are optional and tests construct it without them.

### Pipeline

`pipeline.Engine` runs a fixed sequence, each step its own small method
(`decide`, `scored`, `fallback`, `logDecision`): build the state string,
score it, calibrate, decide, export, log. Each step is a call into a package that
knows nothing about the others. The steps are not pluggable; the order is
the product.

### Worker pool and competing consumers

`queue.Workers` is a fixed pool of goroutines, each pulling one trace at a
time from a single durable JetStream consumer. Several engine processes
using the same consumer name share the work, and each trace goes to exactly
one of them. `queue.Assemblers` is a second pool of the same shape for
span assembly, and each exporter's queue has a third. In memory the pools
read from a bounded channel instead, and only one process takes part.

### Store first, then acknowledge

On NATS, intake answers only after NATS has confirmed the write: `202` on
the JSON API, success on OTLP. A worker acknowledges a trace only after it has a
decision. A crash at any point therefore leaves the trace in JetStream to be
picked up again, never lost between two steps. The cost is that a trace can
be processed twice; it is never processed zero times. The in-memory broker
gives up this guarantee for having nothing to run beside the engine.

### Trace assembly by marker

Spans of one run arrive in several batches. Each batch is stored under
`observer.spans.<trace id>`, and one marker per trace is left on a second
stream. An assembler picks up the marker, checks how long the trace has been
quiet, and either puts the marker back with a delay or assembles the trace,
queues it and removes the spans it read. The wait is held in NATS, not in
memory, so it survives a restart. In memory, a map of pending traces and one
scanner goroutine play the same part.

### Error handling

Every error a component returns is a `*fault.Error`: a **kind** (`invalid`,
`rejected`, `unavailable`, `timeout`, `full`, `internal`), the **component**
it came from, the **operation** it was doing, the **trace** it concerns, and
the cause. Components build them with constructors
(`fault.Unavailable(fault.Scorer, "score trace", err)`) and callers branch on
`fault.KindOf` and `fault.Retryable`, never on another package's sentinel
values or error strings.

Errors that are handled rather than returned (a retry, a dead letter, a
failed export) are **reported** to an `observe.Reporter`, which every
component is given. The engine's reporter is an `observe.Pipeline`: it turns
each error into an entry, counts it, and writes it to every sink configured
under `observability.error_sinks` (stdout and files today; an OpenTelemetry
sink is planned). Sinks are built by an `observe.AbstractSinkFactory`, and a
failing sink is noted on stderr without stopping the engine.

- **Errors are classified once, then acted on.** The scorer classifies each
  failure (`classify`): a 400, 413 or 422 is `rejected`, anything else
  (timeouts, 5xx, connection errors, garbage) is `unavailable` or `timeout`.
  The pipeline defers anything but `rejected` and `invalid` to the queue.
- **Two levels of retry.** Inside one worker, the scorer retries with
  jittered backoff. If that still fails with a system error, the worker
  returns the error and the queue redelivers the trace later, with its own
  backoff, and dead-letters it after `queue.max_deliver` attempts.
- **Degrade rather than fail** where retrying cannot help: a rejected trace,
  or any failure on the synchronous endpoint, gets the policy's random
  fallback decision instead of an error.
- **HTTP status from the kind, in one place.** `server.writeFault` maps
  `full` and the retryable kinds to 503 with `Retry-After`, `invalid` to 400,
  and anything else to 500.

### Graceful shutdown in dependency order

`signal.NotifyContext` turns SIGINT and SIGTERM into a cancelled context.
`run` then stops things in the order that loses nothing: the HTTP server,
the OTLP receivers, the assemblers, the workers, then the export queues,
which have their own time budget so a slow drain cannot starve them, and
last the queue and the broker's connection.

### Golden files for cross-language parity

`trace.BuildState` must produce exactly the text the Python training code
produced, byte for byte, including Python's `repr` of tool arguments
(`trace/pyrepr.go`). `testdata/golden/` holds states generated by the Python
code, and the Go tests compare against them. `make fuzz-golden` does the
same over a few thousand randomized traces.

## Patterns not used, and why

| Pattern | Why not |
| --- | --- |
| Web framework and middleware chain | Six routes and no cross-cutting middleware. Limits and timeouts are set where they apply. |
| DI container | The object graph is small and fixed. |
| Repository layer or ORM | There is no database. |
| Plugin or strategy registry | One scorer, one policy, one exporter. An interface is enough to fake them in tests. |
| Hexagonal or clean-architecture layering | The package split already separates transport, pipeline and integrations; formal ports and adapters would add names without adding separation. |
| Global state | Two process-wide settings exist, both set once in `main`: the default `slog` logger and OpenTelemetry's error handler. |
| Hot config reload | A restart is cheap, and queued work survives it. |

## Concurrency model

| What | Goroutines |
| --- | --- |
| JSON API | one per request (`net/http`) |
| OTLP HTTP and gRPC | one per request or call |
| Queue workers | `server.workers`, each handling one trace at a time |
| Assemblers | `otlp.assemblers` |
| Export | per back end: `exporters[].concurrency` workers draining its queue (a JetStream consumer or a channel), and the OpenTelemetry SDK's batch processor for rebuilt spans |
| In-memory broker | besides its workers and assemblers, one scanner for quiet traces and one timer per pending redelivery |

Components shared across goroutines (`scorer.Client`, `policy.Policy`,
`export.Dispatcher`, `export.OTLPBackend`, `queue.Queue`,
`queue.MemoryBroker`) are safe to call concurrently. There are no locks in
the pipeline itself; the in-memory export queue holds one, only to keep
`Enqueue` and `Shutdown` from racing, and `MemoryBroker` one around its span
map.

## Testing approach

- **No network and no Docker.** `make test` runs `go vet` and
  `go test -race`.
- **Real NATS, in-process.** `pkg/queue/queuetest` starts an actual
  NATS server on a loopback port, so redelivery, dead-lettering and stream
  limits are tested against JetStream itself.
- **Fakes at the interfaces.** Kev is an `httptest.Server` returning the
  canned responses in `testdata/kev/`; a trace back end is an in-memory span
  recorder or a fake `AbstractBackend`.
- **Specs kept in step by tests.** `TestOpenAPICoversRoutes` fails if a route
  is missing from `openapi.yaml`, and `TestSchemaMatchesConfig` fails if
  `observer.schema.yaml` and the config structs drift apart.

## Where to change things

| To change | Look in |
| --- | --- |
| A config key or default | `pkg/config/config.go` (`Default`), `validate.go`, and `compile.go` if a component reads it; then `observer.schema.yaml` and `observer.yaml` |
| Where settings come from | `pkg/config/loader.go`: a new `AbstractLoaderFactory` |
| An HTTP route | `internal/server/server.go`, then `internal/server/openapi.yaml` |
| What Kev is shown | `pkg/trace/slice.go`; it must stay identical to the Python `build_state` |
| How spans map to a trace | `internal/otlp/convert.go` |
| The keep or drop rule | `pkg/policy/policy.go` |
| Retry and dead-letter behaviour | `pkg/scorer/scorer.go`, `pkg/queue/worker.go` (NATS), `pkg/queue/memory.go` (memory), `pkg/export/queue.go` (exports) |
| Where the queues live, or a new broker | `pkg/broker/broker.go`: a new `AbstractFactory` with a `queue.AbstractBroker` and an `export.AbstractQueueFactory` |
| The pipeline's steps or its interfaces | `pkg/pipeline/engine.go`, `pkg/pipeline/interfaces.go` |
| How a trace finds its model | `pkg/pipeline/model.go` (routing), `internal/otlp/convert.go` (`Assembler.agentOf`) |
| A model server with another protocol | `pkg/scorer/factory.go`: a new `AbstractScorerFactory` |
| A metric | `pkg/metrics/metrics.go`, and the hook interface in the package that records it |
| Where errors are written | `observe.AbstractSinkFactory` in `pkg/observe/sink.go`, and `observability.error_sinks` in the config |
| What the trace back ends receive | `pkg/export/otlp_backend.go` (attributes), `internal/otlp/convert.go` (`Annotate`, for forwarded spans) |
| A new kind of trace back end | `pkg/export/backend.go`: a new `AbstractBackend` and `AbstractBackendFactory` |
| Start-up and shutdown order | `cmd/observer/main.go` |
