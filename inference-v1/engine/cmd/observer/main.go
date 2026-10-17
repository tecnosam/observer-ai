// Command observer runs the Observer.ai inference engine: it takes agent
// traces as OTLP spans (gRPC and HTTP) or over a JSON API and queues them in
// NATS JetStream; a worker pool pulls them off the queue, asks the Kev
// decision model whether each is worth keeping, and sends the kept ones to
// Arize Phoenix.
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
	"syscall"
	"time"

	"github.com/observer-ai/engine/internal/config"
	"github.com/observer-ai/engine/internal/export"
	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/server"
	"github.com/observer-ai/engine/internal/trace"
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

	export.LogErrors(logger)
	exporter, err := export.NewPhoenix(ctx, cfg.Export.PhoenixEndpoint, cfg.Export.Project, logger)
	if err != nil {
		return err
	}

	kev, err := scorer.New(scorer.Config{
		Endpoint:         cfg.Scorer.Endpoint,
		Model:            cfg.Scorer.Model,
		Questions:        cfg.Manifest.Questions,
		DecisionQuestion: cfg.Scorer.DecisionQuestion,
		APIKey:           cfg.Scorer.APIKey,
		Timeout:          cfg.Scorer.Timeout.Std(),
		MaxRetries:       cfg.Scorer.MaxRetries,
		Backoff:          durations(cfg.Scorer.Backoff),
		Logger:           logger,
	})
	if err != nil {
		return err
	}

	q, err := queue.Connect(ctx, queue.Config{
		URL:               cfg.Queue.URL,
		ConnectTimeout:    cfg.Queue.ConnectTimeout.Std(),
		Stream:            cfg.Queue.Stream,
		Subject:           cfg.Queue.Subject,
		Consumer:          cfg.Queue.Consumer,
		MaxMessages:       cfg.Queue.MaxMessages,
		AckWait:           cfg.Queue.AckWait.Std(),
		MaxDeliver:        cfg.Queue.MaxDeliver,
		RedeliveryBackoff: durations(cfg.Queue.RedeliveryBackoff),
		DLQStream:         cfg.Queue.DLQStream,
		DLQSubject:        cfg.Queue.DLQSubject,
		DLQMaxAge:         cfg.Queue.DLQMaxAge.Std(),
		Spans:             spanConfig(cfg.OTLP),
		Logger:            logger,
	})
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		return err
	}
	defer q.Close()

	engine := &server.Engine{
		Scorer: kev,
		Policy: policy.New(policy.Config{
			KeepThreshold: cfg.Policy.KeepThreshold,
			RandomFloor:   cfg.Policy.RandomFloor,
			SeverityHigh:  cfg.Alert.SeverityHigh,
		}, nil),
		Exporter:    exporter,
		Temperature: cfg.Manifest.Temperature,
		Model:       cfg.Scorer.Model,
		Logger:      logger,
	}
	srv := server.New(server.Options{Engine: engine, Queue: q, Logger: logger})

	// A system error comes back as an error and the queue redelivers the
	// trace, dead-lettering it after queue.max_deliver attempts.
	workers := q.StartWorkers(cfg.Server.Workers, func(ctx context.Context, t trace.Trace) error {
		_, err := engine.ProcessQueued(ctx, t)
		return err
	})

	// OTLP: the receivers store spans in NATS by trace ID; the assemblers
	// turn each completed trace into a record on the same work queue.
	var receiver *otlp.Receiver
	var assemblers *queue.Assemblers
	if cfg.OTLP.Enabled() {
		receiver = otlp.NewReceiver(q, logger)
		if err := receiver.Start(cfg.OTLP.GRPCAddr, cfg.OTLP.HTTPAddr); err != nil {
			stopWorkers(workers, cfg.Server.ShutdownTimeout.Std())
			_ = exporter.Shutdown(context.Background())
			if errors.Is(err, syscall.EADDRINUSE) {
				err = fmt.Errorf("%w (Phoenix also listens on 4317; set otlp.grpc_addr / otlp.http_addr or %s / %s)",
					err, config.EnvOTLPGRPCAddr, config.EnvOTLPHTTPAddr)
			}
			return err
		}
		assemblers = q.StartAssemblers(cfg.OTLP.Assemblers, otlp.Assemble)
	}

	httpServer := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	logger.Info("observer starting",
		"addr", cfg.Server.Addr,
		"workers", cfg.Server.Workers,
		"otlp_grpc_addr", cfg.OTLP.GRPCAddr,
		"otlp_http_addr", cfg.OTLP.HTTPAddr,
		"otlp_idle_timeout", cfg.OTLP.IdleTimeout.Std().String(),
		"nats_url", cfg.Queue.URL,
		"stream", cfg.Queue.Stream,
		"dlq_stream", cfg.Queue.DLQStream,
		"max_deliver", cfg.Queue.MaxDeliver,
		"scorer_kind", cfg.Scorer.Kind,
		"scorer_api_key_env", cfg.Scorer.APIKeyEnv,
		"kev_endpoint", cfg.Scorer.Endpoint,
		"kev_model", cfg.Scorer.Model,
		"manifest_model", cfg.Manifest.Model,
		"decision_question", cfg.Scorer.DecisionQuestion,
		"temperature", cfg.Manifest.Temperature,
		"keep_threshold", cfg.Policy.KeepThreshold,
		"random_floor", cfg.Policy.RandomFloor,
		"phoenix_endpoint", cfg.Export.PhoenixEndpoint,
		"project", cfg.Export.Project,
	)
	probeCtx, cancelProbe := context.WithTimeout(ctx, 2*time.Second)
	if err := kev.Ready(probeCtx); err != nil {
		logger.Warn("kev is not reachable yet; queued traces will be redelivered until it is", "error", err.Error())
	}
	cancelProbe()

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	select {
	case err := <-serveErr:
		// The listener failed before any signal. Whatever the workers
		// picked up meanwhile goes back to the queue or finishes.
		stopOTLP(receiver, assemblers, cfg.Server.ShutdownTimeout.Std())
		stopWorkers(workers, cfg.Server.ShutdownTimeout.Std())
		_ = exporter.Shutdown(context.Background())
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	stop() // a second signal now kills the process the default way
	logger.Info("shutting down", "timeout", cfg.Server.ShutdownTimeout.Std().String())

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Std())
	defer cancelDrain()
	var errs []error
	if err := httpServer.Shutdown(drainCtx); err != nil {
		errs = append(errs, fmt.Errorf("stop http server: %w", err))
	}
	// Spans not yet assembled and traces still queued stay in JetStream for
	// the next start; only the work in flight needs to finish.
	if receiver != nil {
		if err := receiver.Shutdown(drainCtx); err != nil {
			errs = append(errs, fmt.Errorf("stop otlp receivers: %w", err))
		}
		if err := assemblers.Stop(drainCtx); err != nil {
			errs = append(errs, fmt.Errorf("stop assemblers: %w", err))
		}
	}
	if err := workers.Stop(drainCtx); err != nil {
		errs = append(errs, fmt.Errorf("stop workers: %w", err))
	}

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), exportShutdownTimeout)
	defer cancelFlush()
	if err := exporter.Shutdown(flushCtx); err != nil {
		errs = append(errs, fmt.Errorf("flush exporter: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

func durations(in []config.Duration) []time.Duration {
	out := make([]time.Duration, len(in))
	for i, d := range in {
		out[i] = d.Std()
	}
	return out
}

// spanConfig maps the OTLP settings onto the queue. With OTLP off it is
// zero, and the queue creates no span streams.
func spanConfig(o config.OTLP) queue.SpanConfig {
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

func stopWorkers(w *queue.Workers, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = w.Stop(ctx)
}

func stopOTLP(r *otlp.Receiver, a *queue.Assemblers, timeout time.Duration) {
	if r == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = r.Shutdown(ctx)
	_ = a.Stop(ctx)
}
