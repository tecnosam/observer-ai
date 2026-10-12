package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	defaultMaxBodyBytes = 8 << 20
	readyTimeout        = 2 * time.Second
	syncFlushTimeout    = 5 * time.Second
	retryAfterSeconds   = "1"
)

// Publisher puts a validated trace on the work queue. *queue.Queue
// implements it.
type Publisher interface {
	// Publish returns queue.ErrFull when the queue is at its limit.
	Publish(ctx context.Context, t trace.Trace) error
	// Ready reports whether the queue can currently be reached.
	Ready() error
}

// Options configures a Server.
type Options struct {
	Engine *Engine
	Queue  Publisher
	// MaxBodyBytes caps a request body; 0 means 8 MiB.
	MaxBodyBytes int64
	Logger       *slog.Logger
}

// Server holds the HTTP handlers.
type Server struct {
	engine       *Engine
	queue        Publisher
	mux          *http.ServeMux
	logger       *slog.Logger
	maxBodyBytes int64
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
		queue:        opts.Queue,
		mux:          http.NewServeMux(),
		logger:       opts.Logger,
		maxBodyBytes: opts.MaxBodyBytes,
	}
	s.mux.HandleFunc("POST /v1/traces", s.handleEnqueue)
	s.mux.HandleFunc("POST /v1/traces:score", s.handleScore)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("GET /docs", s.handleDocs)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

// handleEnqueue answers 202 only after the queue has confirmed the trace is
// stored, so an accepted trace survives a restart of this process.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	t, ok := s.readTrace(w, r)
	if !ok {
		return
	}
	switch err := s.queue.Publish(r.Context(), t); {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"trace_id": t.TraceID})
	case errors.Is(err, queue.ErrFull):
		s.logger.Warn("queue full, rejecting trace", "trace_id", t.TraceID)
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "queue full, retry later")
	default:
		s.logger.Error("queue publish failed, rejecting trace", "trace_id", t.TraceID, "error", err.Error())
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, retry later")
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
	var problem string
	if err := s.queue.Ready(); err != nil {
		problem = "queue is not reachable: " + err.Error()
	} else if err := s.engine.Scorer.Ready(ctx); err != nil {
		problem = "kev is not reachable: " + err.Error()
	}
	if problem != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": problem})
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
	t.OTLP = nil // spans come in through the OTLP receivers, not this API
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
