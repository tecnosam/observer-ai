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
| Factory pattern? | An abstract factory for where settings come from (`config.AbstractLoaderFactory`: a YAML file, or values built in code). Elsewhere, constructor functions (`scorer.New`, `queue.Connect`, `export.NewPhoenix`). |
| How are dependencies swapped in tests? | Small interfaces declared by the package that uses them, plus a few function-typed fields. |
| Config? | A `Compiler` with an injected loader factory. For the YAML file: `gopkg.in/yaml.v3`, decoded strictly over a struct of defaults, then six environment overrides, then validation. Read once at start-up. |
| Database? | None. NATS JetStream is the only state: the queues and the spans waiting to be assembled. |
| Logging? | `log/slog`, JSON to stdout. |

## Stack

| Concern | What is used |
| --- | --- |
| Language | Go 1.26, one static binary (`CGO_ENABLED=0`) |
| JSON API | `net/http` with `http.ServeMux` method patterns (`"POST /v1/traces"`), available since Go 1.22 |
| OTLP over HTTP | A second `net/http` server on its own port |
| OTLP over gRPC | `google.golang.org/grpc`, serving the standard OTLP `TraceService` |
| Wire formats | `google.golang.org/protobuf` and the generated OTLP types in `go.opentelemetry.io/proto/otlp` |
| Queue and storage | NATS JetStream through `github.com/nats-io/nats.go` (the `jetstream` package) |
| Calling Kev | `net/http` client, JSON |
| Sending to Phoenix | The OpenTelemetry Go SDK with the `otlptracehttp` exporter |
| Config | `gopkg.in/yaml.v3` for `observer.yaml`, `encoding/json` for the model manifest |
| API docs | A hand-written OpenAPI file embedded with `go:embed`; Swagger UI loaded from a CDN |
| Tests | The standard `testing` package, `net/http/httptest`, and a real NATS server started in-process (`github.com/nats-io/nats-server/v2`, test-only) |

There is no ORM, no router library, no validation library, no CLI library
(two flags, read with `flag`) and no mocking library.

## Directory structure

```
cmd/observer/main.go     the only executable: flags, config, wiring, shutdown
internal/
  config/                settings and manifest: loaders, compiler, validation, per-component settings
  trace/                 the Trace record, its JSON decoding, and BuildState (the text Kev reads)
  scorer/                HTTP client for Kev: retries, error classes, temperature calibration
  policy/                keep or drop from a probability: threshold, random floor, severity
  otlp/                  OTLP gRPC and HTTP receivers; turning spans into a Trace
  queue/                 JetStream: streams, publishing, the worker pool, span assembly, dead letters
    queuetest/           starts an in-process NATS server for tests
  export/                sends kept traces to Phoenix
  server/                JSON API handlers, and Engine: the scoring pipeline
testdata/                trace fixtures, golden state strings, canned Kev responses
scripts/                 send_samples.py
observer.yaml            example config, also baked into the Docker image
observer.schema.yaml     JSON Schema for observer.yaml
```

Everything is under `internal/`, so nothing here can be imported by another
module. Each directory is one package with one job.

### Which package imports which

```
scorer   (nothing)          policy  (nothing)          trace   (nothing)
otlp     -> trace
queue    -> trace
config   -> policy, queue, scorer
export   -> otlp, policy, trace
server   -> export, policy, queue, scorer, trace
main     -> all of them
```

Two things are worth noticing:

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
                                └─ queue.Assemblers -> otlp.Assemble -> queue.Publish ───┤
                                                                                         v
                                                       JetStream work queue (OBSERVER_TRACES)
                                                                                         │
                             queue.Workers -> server.Engine.ProcessQueued <──────────────┘
                                                 trace.BuildState -> scorer.Score
                                                 -> scorer.Calibrate -> policy.Decide
                                                 -> export.Export (kept traces only)

JSON  POST /v1/traces:score  server.handleScore -> server.Engine.Process   (same pipeline, no queue)
```

`server.Engine` is the pipeline. Both the synchronous endpoint and the queue
workers call it; the only difference is what happens when Kev fails (see
[Error handling](#error-handling)).

## From observer.yaml to running configuration

Go does not "compile" the YAML ahead of time. At start-up a `Compiler` turns
settings from a source into one validated `Config`. Where the settings come
from is injected through an abstract factory, so the same compiler serves a
YAML file or values built in code:

```
internal/config/
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
   `OBSERVER_PHOENIX_ENDPOINT`, `OBSERVER_MANIFEST`,
   `OBSERVER_OTLP_GRPC_ADDR` and `OBSERVER_OTLP_HTTP_ADDR`. These are the
   values that differ between a laptop and a container. Nothing else can be
   set from the environment. The environment itself is injected
   (`WithEnv`, `os.Getenv` by default), so tests and embedding programs
   control it.
3. **Read the scorer's API key** from the variable `scorer.api_key_env` names
   (`JEV_API_KEY` by default for `scorer.kind: jev`).
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
kev, err := scorer.New(cfg.ScorerConfig(logger))
q, err := queue.Connect(ctx, cfg.QueueConfig(logger))
policy.New(cfg.PolicyConfig(), nil)
```

`internal/` keeps the package private to this module. A program in another
module that wants to embed the engine needs `config` (and the components)
moved out of `internal/`, for example to `pkg/`.

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
type, default and limits. A test (`internal/config/schema_test.go`) fails if
the schema and the Go structs disagree on a key or a default.

## Design patterns in use

### Manual dependency injection, with one composition root

Every component receives its collaborators through its constructor or its
struct fields. None of them creates its own dependencies or reaches for a
global. `run` in `main.go` is the one place where concrete types are chosen
and connected:

```go
engine := &server.Engine{Scorer: kev, Policy: policy.New(...), Exporter: exporter, ...}
srv := server.New(server.Options{Engine: engine, Queue: q, Logger: logger})
workers := q.StartWorkers(cfg.Server.Workers, func(ctx context.Context, t trace.Trace) error {
    _, err := engine.ProcessQueued(ctx, t)
    return err
})
```

There is no DI framework (no Wire, Fx or Dig). With about ten objects, wiring
by hand is shorter than configuring a container, and the start-up order is
readable top to bottom.

### Interfaces defined by the consumer

A package declares an interface for what it needs, next to the code that
uses it, and the concrete type lives elsewhere. All of them are small:

| Interface | Declared in | Methods | Real implementation |
| --- | --- | --- | --- |
| `server.Scorer` | `server/engine.go` | `Score`, `Ready` | `*scorer.Client` |
| `server.Exporter` | `server/engine.go` | `Export`, `Flush` | `*export.Exporter` |
| `server.Publisher` | `server/server.go` | `Publish`, `Ready` | `*queue.Queue` |
| `otlp.Sink` | `otlp/receiver.go` | `PublishSpans` | `*queue.Queue` |
| `export.Uploader` | `export/export.go` | `UploadTraces` | the OTLP/HTTP client |

This is what lets `otlp` store spans in the queue without importing the
`queue` package, and lets the tests replace Kev or Phoenix with a few lines
of fake.

### Functions as injection points

Where an interface would have a single method, a function type is used
instead:

- `queue.Handler`: what a worker does with a trace. `main` passes
  `engine.ProcessQueued`.
- `queue.AssembleFunc`: how spans become a trace. `main` passes
  `otlp.Assemble`. This keeps `queue` free of any OTLP knowledge.
- `policy.New(cfg, rnd)`: the random source, so tests can make the random
  floor deterministic.
- `scorer.Config.Sleep` and `Jitter`: so retry tests run without waiting.

### Constructor functions

Each component has a `New`-style function that validates its input, fills in
defaults for optional fields (a nil logger becomes `slog.Default()`, a nil
HTTP client a new one) and returns a ready value: `scorer.New`,
`policy.New`, `server.New`, `otlp.NewReceiver`, `queue.Connect`,
`export.NewPhoenix`. These are plain constructors: each component has one
implementation, so there is nothing to choose between yet.

### Abstract factory for configuration

Settings can come from more than one place: the YAML file the binary reads,
or values a program builds in code. `config.AbstractLoaderFactory` builds the
pair of loaders for one source, a config loader and a manifest loader, so the
two always agree (a YAML file's relative manifest path resolves against the
file; a `StaticFactory` can carry the manifest as a value). `config.Compiler`
is given a factory and an environment lookup and never knows which source it
has. A new source (flags, a database, a remote store) is a new factory, with
no change to the compiler or to validation.

### Config structs, and functional options in one place

Components take a `Config` or `Options` struct rather than a long argument
list. The exporter is the one place that uses functional options
(`export.New(tp, export.WithForwarder(...), export.WithLogger(...))`),
because its extras are optional and tests construct it without them.

### Pipeline

`server.Engine.process` is a fixed sequence: build the state string, score
it, calibrate, decide, export, log. Each step is a call into a package that
knows nothing about the others. The steps are not pluggable; the order is
the product.

### Worker pool and competing consumers

`queue.Workers` is a fixed pool of goroutines, each pulling one trace at a
time from a single durable JetStream consumer. Several engine processes
using the same consumer name share the work, and each trace goes to exactly
one of them. `queue.Assemblers` is a second pool of the same shape for
span assembly.

### Store first, then acknowledge

Intake answers only after NATS has confirmed the write: `202` on the JSON
API, success on OTLP. A worker acknowledges a trace only after it has a
decision. A crash at any point therefore leaves the trace in JetStream to be
picked up again, never lost between two steps. The cost is that a trace can
be processed twice; it is never processed zero times.

### Trace assembly by marker

Spans of one run arrive in several batches. Each batch is stored under
`observer.spans.<trace id>`, and one marker per trace is left on a second
stream. An assembler picks up the marker, checks how long the trace has been
quiet, and either puts the marker back with a delay or assembles the trace,
queues it and removes the spans it read. The wait is held in NATS, not in
memory, so it survives a restart.

### Error handling

- **Errors are classified once, then acted on.** `scorer.IsSystemError`
  separates "Kev is unwell" (timeouts, 5xx, connection errors) from "Kev
  rejected this trace" (400, 413, 422).
- **Two levels of retry.** Inside one worker, the scorer retries with
  jittered backoff. If that still fails with a system error, the worker
  returns the error and the queue redelivers the trace later, with its own
  backoff, and dead-letters it after `queue.max_deliver` attempts.
- **Degrade rather than fail** where retrying cannot help: a rejected trace,
  or any failure on the synchronous endpoint, gets the policy's random
  fallback decision instead of an error.
- **Sentinel and typed errors**, checked with `errors.Is` and `errors.As`:
  `queue.ErrFull`, `queue.ErrUnavailable`, `otlp.ErrNotScorable`,
  `*scorer.StatusError`.

### Graceful shutdown in dependency order

`signal.NotifyContext` turns SIGINT and SIGTERM into a cancelled context.
`run` then stops things in the order that loses nothing: the HTTP server,
the OTLP receivers, the assemblers, the workers, and last the exporter's
flush, which has its own time budget so a slow drain cannot starve it.

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
| Phoenix export | the OpenTelemetry SDK's batch processor, in the background |

Components shared across goroutines (`scorer.Client`, `policy.Policy`,
`export.Exporter`, `queue.Queue`) hold no per-request state and are safe to
call concurrently. There are no locks in the pipeline itself.

## Testing approach

- **No network and no Docker.** `make test` runs `go vet` and
  `go test -race`.
- **Real NATS, in-process.** `internal/queue/queuetest` starts an actual
  NATS server on a loopback port, so redelivery, dead-lettering and stream
  limits are tested against JetStream itself.
- **Fakes at the interfaces.** Kev is an `httptest.Server` returning the
  canned responses in `testdata/kev/`; Phoenix is an in-memory span
  recorder.
- **Specs kept in step by tests.** `TestOpenAPICoversRoutes` fails if a route
  is missing from `openapi.yaml`, and `TestSchemaMatchesConfig` fails if
  `observer.schema.yaml` and the config structs drift apart.

## Where to change things

| To change | Look in |
| --- | --- |
| A config key or default | `internal/config/config.go` (`Default`), `validate.go`, and `compile.go` if a component reads it; then `observer.schema.yaml` and `observer.yaml` |
| Where settings come from | `internal/config/loader.go`: a new `AbstractLoaderFactory` |
| An HTTP route | `internal/server/server.go`, then `internal/server/openapi.yaml` |
| What Kev is shown | `internal/trace/slice.go`; it must stay identical to the Python `build_state` |
| How spans map to a trace | `internal/otlp/convert.go` |
| The keep or drop rule | `internal/policy/policy.go` |
| Retry and dead-letter behaviour | `internal/scorer/scorer.go`, `internal/queue/worker.go` |
| What Phoenix receives | `internal/export/export.go` |
| Start-up and shutdown order | `cmd/observer/main.go` |
