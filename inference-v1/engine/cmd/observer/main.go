// Command observer runs the Observer.ai inference engine: it takes agent
// traces as OTLP spans (gRPC and HTTP) or over a JSON API and queues them, in
// NATS JetStream or in process; a worker pool pulls them off the queue, asks the Kev
// decision model whether each is worth keeping, and sends the kept ones to
// every configured OTLP back end (Phoenix, Langfuse, ...).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/internal/server"
	"github.com/observer-ai/engine/pkg/broker"
	"github.com/observer-ai/engine/pkg/config"
	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/metrics"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/policy"
	"github.com/observer-ai/engine/pkg/queue"
	"github.com/observer-ai/engine/pkg/scorer"
	"github.com/observer-ai/engine/pkg/trace"
)

// exportShutdownTimeout bounds the final span flush, separately from the
// wait for in-flight traces, so a slow wait cannot leave the flush with no
// time at all.
const exportShutdownTimeout = 10 * time.Second

func main() {
	configPath := flag.String("config", "observer.yaml", "path to the YAML config file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "observer: invalid -log-level %q\n", *logLevel)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if err := run(*configPath, logger); err != nil {
		logger.Error("observer stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := &app{cfg: cfg, logger: logger}
	if err := a.start(ctx); err != nil {
		a.abort()
		return err
	}
	defer a.close()
	return a.serve(ctx, stop)
}

// app is the running engine: every component main starts, kept so they can
// be stopped in the right order.
type app struct {
	cfg    *config.Config
	logger *slog.Logger

	metrics    *metrics.Pipeline
	reporter   *observe.Pipeline
	broker     broker.AbstractFactory
	queue      queue.AbstractBroker
	exporter   *export.Dispatcher
	engine     *pipeline.Engine
	workers    queue.Stopper
	receiver   *otlp.Receiver
	assemblers queue.Stopper
	http       *http.Server
}

// start builds and starts every component, in dependency order.
func (a *app) start(ctx context.Context) error {
	steps := []func(context.Context) error{
		a.startMetrics, a.startReporter, a.startBroker, a.startQueue, a.startExporter,
		a.startEngine, a.startWorkers, a.startOTLP, a.startHTTP,
	}
	for _, step := range steps {
		if err := step(ctx); err != nil {
			return err
		}
	}
	a.logStart()
	a.probeScorer(ctx)
	return nil
}

// startMetrics starts the engine's own OpenTelemetry metrics, or a recorder
// that records nothing when observability.metrics.endpoint is unset.
func (a *app) startMetrics(ctx context.Context) error {
	m, err := metrics.Start(ctx, a.cfg.MetricsConfig())
	a.metrics = m
	return err
}

// startReporter builds the error pipeline every other component reports to.
func (a *app) startReporter(context.Context) error {
	reporter, err := observe.BuildPipeline(observe.DefaultSinkFactory{}, a.cfg.ErrorSinks(), a.metrics)
	a.reporter = reporter
	return err
}

// startBroker picks where the queues live: NATS JetStream when queue.url is
// set, else in process.
func (a *app) startBroker(context.Context) error {
	bcfg := a.cfg.BrokerConfig(a.logger)
	bcfg.Reporter = a.reporter
	b, err := broker.New(bcfg)
	a.broker = b
	return err
}

func (a *app) startQueue(ctx context.Context) error {
	qcfg := a.cfg.QueueConfig(a.logger)
	qcfg.Reporter = a.reporter
	q, err := a.broker.Ingest(ctx, qcfg)
	a.queue = q
	return err
}

// startExporter comes after startQueue, which waits for JetStream to be up.
func (a *app) startExporter(ctx context.Context) error {
	export.ReportErrors(a.reporter)
	exporter, err := export.NewDispatcher(ctx, export.DispatcherConfig{
		Backends: a.cfg.ExportBackends(),
		Factory:  export.OTLPBackendFactory{},
		Queues:   a.broker.ExportQueues(a.cfg.ExportStreams()),
		Reporter: a.reporter,
		Metrics:  a.metrics,
	})
	a.exporter = exporter
	return err
}

// startEngine builds every model (the default and one per configured
// agent) and the engine that routes traces between them.
func (a *app) startEngine(context.Context) error {
	models, err := scorer.BuildRouter(scorer.HTTPFactory{}, a.cfg.Models(a.reporter))
	if err != nil {
		return err
	}
	a.engine = pipeline.NewEngine(pipeline.Deps{
		Models:   models,
		Policy:   policy.New(a.cfg.PolicyConfig(), nil),
		Exporter: a.exporter,
		Reporter: a.reporter,
		Metrics:  a.metrics,
		Logger:   a.logger,
	})
	return nil
}

// startWorkers pulls traces off the queue. A system error comes back as an
// error and the queue redelivers the trace, dead-lettering it after
// queue.max_deliver attempts.
func (a *app) startWorkers(context.Context) error {
	a.workers = a.queue.StartWorkers(a.cfg.Server.Workers, func(ctx context.Context, t trace.Trace) error {
		_, err := a.engine.ProcessQueued(ctx, t)
		return err
	})
	return nil
}

// startOTLP starts the receivers, which store spans in NATS by trace ID, and
// the assemblers, which turn each completed trace into a record on the same
// work queue.
func (a *app) startOTLP(context.Context) error {
	if !a.cfg.OTLP.Enabled() {
		return nil
	}
	a.receiver = otlp.NewReceiver(otlp.ReceiverConfig{Sink: a.queue, Reporter: a.reporter, Metrics: a.metrics})
	if err := a.receiver.Start(a.cfg.OTLP.GRPCAddr, a.cfg.OTLP.HTTPAddr); err != nil {
		a.receiver = nil
		return explainAddrInUse(err)
	}
	assembler := otlp.Assembler{AgentAttributes: a.cfg.AgentAttributes()}
	a.assemblers = a.queue.StartAssemblers(a.cfg.OTLP.Assemblers, assembler.Assemble)
	return nil
}

func explainAddrInUse(err error) error {
	if !errors.Is(err, syscall.EADDRINUSE) {
		return err
	}
	return fmt.Errorf("%w (Phoenix also listens on 4317; set otlp.grpc_addr / otlp.http_addr or %s / %s)",
		err, config.EnvOTLPGRPCAddr, config.EnvOTLPHTTPAddr)
}

func (a *app) startHTTP(context.Context) error {
	srv := server.New(server.Options{Engine: a.engine, Queue: a.queue, Reporter: a.reporter, Metrics: a.metrics})
	a.http = &http.Server{
		Addr:              a.cfg.Server.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	return nil
}

func (a *app) logStart() {
	cfg := a.cfg
	a.logger.Info("observer starting",
		"addr", cfg.Server.Addr,
		"workers", cfg.Server.Workers,
		"otlp_grpc_addr", cfg.OTLP.GRPCAddr,
		"otlp_http_addr", cfg.OTLP.HTTPAddr,
		"otlp_idle_timeout", cfg.OTLP.IdleTimeout.Std().String(),
		"broker", a.broker.Kind(),
		"nats_url", cfg.Queue.URL,
		"stream", cfg.Queue.Stream,
		"dlq_stream", cfg.Queue.DLQStream,
		"max_deliver", cfg.Queue.MaxDeliver,
		"scorer_kind", cfg.Scorer.Kind,
		"scorer_api_key_env", cfg.Scorer.APIKeyEnv,
		"kev_endpoint", cfg.Scorer.Endpoint,
		"kev_model", cfg.Scorer.Model,
		"agent_models", agentModels(cfg),
		"agent_attributes", cfg.Agents.Attributes,
		"metrics_endpoint", cfg.Observability.Metrics.Endpoint,
		"manifest_model", cfg.Manifest.Model,
		"decision_question", cfg.Scorer.DecisionQuestion,
		"temperature", cfg.Manifest.Temperature,
		"keep_threshold", cfg.Policy.KeepThreshold,
		"random_floor", cfg.Policy.RandomFloor,
		"exporters", exporterNames(cfg.Exporters),
	)
}

// agentModels lists each agent with its own model as agent=model@endpoint.
func agentModels(cfg *config.Config) []string {
	out := make([]string, 0, len(cfg.Agents.Models))
	for agent, m := range cfg.Agents.Models {
		out = append(out, agent+"="+m.Model+"@"+m.Endpoint)
	}
	slices.Sort(out)
	return out
}

// exporterNames lists the back ends as name=endpoint, never their headers.
func exporterNames(exporters []config.Exporter) []string {
	out := make([]string, len(exporters))
	for i, e := range exporters {
		out[i] = e.Name + "=" + e.Endpoint
	}
	return out
}

// probeScorer warns, without failing start-up, when the model is not up yet.
func (a *app) probeScorer(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := a.engine.Ready(ctx); err != nil {
		a.reporter.Report(ctx, fault.Unavailable(fault.Scorer,
			"probe at start-up, queued traces will be redelivered until it answers", err))
	}
}

// serve runs the HTTP server until a signal arrives or the listener fails.
func (a *app) serve(ctx context.Context, stop context.CancelFunc) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- a.http.ListenAndServe() }()
	select {
	case err := <-serveErr:
		// The listener failed before any signal. Whatever the workers
		// picked up meanwhile goes back to the queue or finishes.
		a.abort()
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	stop() // a second signal now kills the process the default way
	return a.shutdown()
}

// shutdown stops taking work, lets the work in flight finish within
// server.shutdown_timeout, then flushes the exporter. Spans not yet
// assembled and traces still queued stay in JetStream for the next start.
func (a *app) shutdown() error {
	timeout := a.cfg.Server.ShutdownTimeout.Std()
	a.logger.Info("shutting down", "timeout", timeout.String())
	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errs := []error{
		wrapStop("stop http server", a.http.Shutdown(drainCtx)),
		a.stopOTLP(drainCtx),
		wrapStop("stop workers", a.workers.Stop(drainCtx)),
		a.flushExporter(),
		a.flushMetrics(),
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	a.logger.Info("shutdown complete")
	return nil
}

func (a *app) stopOTLP(ctx context.Context) error {
	if a.receiver == nil {
		return nil
	}
	return errors.Join(
		wrapStop("stop otlp receivers", a.receiver.Shutdown(ctx)),
		wrapStop("stop assemblers", a.assemblers.Stop(ctx)),
	)
}

// flushExporter has its own time budget, so a slow drain cannot leave the
// final flush with no time at all.
func (a *app) flushExporter() error {
	ctx, cancel := context.WithTimeout(context.Background(), exportShutdownTimeout)
	defer cancel()
	return wrapStop("flush exporter", a.exporter.Shutdown(ctx))
}

// flushMetrics sends the last metrics, after everything that records them
// has stopped.
func (a *app) flushMetrics() error {
	ctx, cancel := context.WithTimeout(context.Background(), exportShutdownTimeout)
	defer cancel()
	return wrapStop("flush metrics", a.metrics.Shutdown(ctx))
}

// abort stops whatever start managed to start, after a failure.
func (a *app) abort() {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout.Std())
	defer cancel()
	_ = a.stopOTLP(ctx)
	if a.workers != nil {
		_ = a.workers.Stop(ctx)
	}
	if a.exporter != nil {
		_ = a.exporter.Shutdown(context.Background())
	}
	if a.metrics != nil {
		_ = a.metrics.Shutdown(ctx)
	}
	a.close()
}

// close releases the queue, the broker connection and the error sinks, last.
func (a *app) close() {
	if a.queue != nil {
		a.queue.Close()
		a.queue = nil
	}
	if a.broker != nil {
		a.broker.Close()
		a.broker = nil
	}
	if a.reporter != nil {
		_ = a.reporter.Close()
		a.reporter = nil
	}
}

func wrapStop(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", what, err)
}
