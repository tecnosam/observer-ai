// Command observer runs the Observer.ai inference engine: it takes agent
// traces over HTTP, asks the Kev decision model whether each is worth
// keeping, and exports the kept ones to Arize Phoenix.
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
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/server"
)

// exportShutdownTimeout bounds the final span flush, separately from the
// queue drain, so a slow drain cannot leave the flush with no time at all.
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
	exporter, err := export.NewPhoenix(ctx, cfg.Export.PhoenixEndpoint, cfg.Export.Project)
	if err != nil {
		return err
	}

	backoff := make([]time.Duration, len(cfg.Scorer.Backoff))
	for i, b := range cfg.Scorer.Backoff {
		backoff[i] = b.Std()
	}
	kev, err := scorer.New(scorer.Config{
		Endpoint:         cfg.Scorer.Endpoint,
		Model:            cfg.Scorer.Model,
		Questions:        cfg.Manifest.Questions,
		DecisionQuestion: cfg.Scorer.DecisionQuestion,
		Timeout:          cfg.Scorer.Timeout.Std(),
		MaxRetries:       cfg.Scorer.MaxRetries,
		Backoff:          backoff,
		Logger:           logger,
	})
	if err != nil {
		return err
	}

	srv := server.New(server.Options{
		Engine: &server.Engine{
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
		},
		Workers:   cfg.Server.Workers,
		QueueSize: cfg.Server.QueueSize,
		Logger:    logger,
	})

	httpServer := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	logger.Info("observer starting",
		"addr", cfg.Server.Addr,
		"workers", cfg.Server.Workers,
		"queue_size", cfg.Server.QueueSize,
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
		logger.Warn("kev is not reachable yet; traces will get fallback decisions until it is", "error", err.Error())
	}
	cancelProbe()

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	select {
	case err := <-serveErr:
		// The listener failed before any signal; nothing is queued yet.
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
	if err := srv.Drain(drainCtx); err != nil {
		errs = append(errs, fmt.Errorf("drain worker pool: %w", err))
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
