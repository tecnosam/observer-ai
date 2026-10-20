package common

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/observer-ai/engine/pkg/metrics"
)

// RequestMetrics records each request a server answers. metrics.Recorder
// and metrics.Noop implement it.
type RequestMetrics interface {
	RequestFinished(ctx context.Context, req metrics.Request)
}

// InstrumentHTTP times every request h answers and records it under server
// ("api" or "otlp"), with its route pattern and status.
func InstrumentHTTP(server string, h http.Handler, m RequestMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)
		m.RequestFinished(r.Context(), metrics.Request{
			Server:   server,
			Protocol: metrics.ProtocolHTTP,
			Method:   r.Method,
			Route:    routeOf(r),
			Status:   rec.status,
			Duration: time.Since(start),
		})
	})
}

// routeOf is the path of the pattern the ServeMux matched (it sets it on
// the request), so metrics are not split by raw path.
func routeOf(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	_, path, found := strings.Cut(r.Pattern, " ")
	if !found {
		return r.Pattern
	}
	return path
}

// statusRecorder remembers the status a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
