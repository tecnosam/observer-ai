package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/observer-ai/engine/internal/trace"
)

const (
	defaultMaxBodyBytes = 8 << 20
	readyTimeout        = 2 * time.Second
	syncFlushTimeout    = 5 * time.Second
	retryAfterSeconds   = "1"
)

// Options configures a Server.
type Options struct {
	Engine    *Engine
	Workers   int
	QueueSize int
	// MaxBodyBytes caps a request body; 0 means 8 MiB.
	MaxBodyBytes int64
	Logger       *slog.Logger
}

// Server holds the HTTP handlers and the worker pool behind POST /v1/traces.
type Server struct {
	engine       *Engine
	pool         *pool
	mux          *http.ServeMux
	logger       *slog.Logger
	maxBodyBytes int64

	// workCtx is the context queued traces are processed under. It outlives
	// any request and is cancelled only when a drain runs out of time.
	workCtx    context.Context
	cancelWork context.CancelFunc
}

func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultMaxBodyBytes
	}
	s := &Server{
		engine:       opts.Engine,
		mux:          http.NewServeMux(),
		logger:       opts.Logger,
		maxBodyBytes: opts.MaxBodyBytes,
	}
	s.workCtx, s.cancelWork = context.WithCancel(context.Background())
	s.pool = newPool(opts.Workers, opts.QueueSize, func(t trace.Trace) {
		s.engine.Process(s.workCtx, t)
	})

	s.mux.HandleFunc("POST /v1/traces", s.handleEnqueue)
	s.mux.HandleFunc("POST /v1/traces:score", s.handleScore)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("GET /docs", s.handleDocs)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

// Drain stops accepting queued work and waits for the workers to finish
// what is queued. If ctx ends first, in-flight Kev calls are cancelled so
// the remaining traces get fallback decisions instead of being lost, and
// Drain returns ctx's error once the workers have exited.
func (s *Server) Drain(ctx context.Context) error {
	s.pool.close()
	err := s.pool.wait(ctx)
	if err != nil {
		s.cancelWork()
		<-s.pool.done
	}
	s.cancelWork()
	return err
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	t, ok := s.readTrace(w, r)
	if !ok {
		return
	}
	switch err := s.pool.submit(t); {
	case errors.Is(err, errQueueFull):
		s.logger.Warn("queue full, rejecting trace", "trace_id", t.TraceID)
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "queue full, retry later")
	case errors.Is(err, errClosed):
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
	default:
		writeJSON(w, http.StatusAccepted, map[string]string{"trace_id": t.TraceID})
	}
}

func (s *Server) handleScore(w http.ResponseWriter, r *http.Request) {
	t, ok := s.readTrace(w, r)
	if !ok {
		return
	}
	res := s.engine.Process(r.Context(), t)
	if res.Exported {
		// Push the spans out now so a demo sees the trace in Phoenix as
		// soon as the response arrives, not a batch interval later.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), syncFlushTimeout)
		defer cancel()
		if err := s.engine.Exporter.Flush(ctx); err != nil {
			s.logger.Error("trace export flush failed", "trace_id", t.TraceID, "error", err.Error())
			res.Exported = false
		}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := s.engine.Scorer.Ready(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"error":  "kev is not reachable: " + err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// readTrace decodes and validates the request body, assigning a trace ID if
// the client sent none. On failure it writes the error response.
func (s *Server) readTrace(w http.ResponseWriter, r *http.Request) (trace.Trace, bool) {
	t, err := trace.Decode(http.MaxBytesReader(w, r.Body, s.maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return trace.Trace{}, false
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return trace.Trace{}, false
	}
	if t.TraceID == "" {
		t.TraceID = trace.NewID()
	}
	return t, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
