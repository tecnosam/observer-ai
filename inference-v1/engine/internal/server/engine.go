// Package server exposes the engine over HTTP: an asynchronous intake backed
// by a bounded worker pool, a synchronous scoring path, and health checks.
package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/observer-ai/engine/internal/export"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/trace"
)

// Scorer asks the decision model about a state. *scorer.Client implements it.
type Scorer interface {
	Score(ctx context.Context, state string) (scorer.Result, error)
	Ready(ctx context.Context) error
}

// Exporter ships kept traces. *export.Exporter implements it.
type Exporter interface {
	Export(ctx context.Context, rec export.Record) bool
	Flush(ctx context.Context) error
}

// Result is the decision for one trace, as returned by POST /v1/traces:score.
// p_keep and p_keep_raw are null when Kev could not be reached and the
// decision was a fallback.
type Result struct {
	TraceID   string   `json:"trace_id"`
	Decision  string   `json:"decision"`
	PKeep     *float64 `json:"p_keep"`
	PKeepRaw  *float64 `json:"p_keep_raw"`
	Reason    string   `json:"reason"`
	Severity  string   `json:"severity"`
	Model     string   `json:"model"`
	LatencyMS float64  `json:"latency_ms"`
	// Exported reports that the trace's spans were handed to the exporter.
	// On the sync path it also means the flush to Phoenix did not fail. On
	// the queued path delivery is batched and failures are only logged.
	Exported bool `json:"exported"`
}

// Engine runs one trace through slice, score, decide and export.
type Engine struct {
	Scorer   Scorer
	Policy   *policy.Policy
	Exporter Exporter
	// Temperature calibrates the raw probability (1 = unchanged).
	Temperature float64
	// Model names the scorer in fallback decisions, when Kev reported nothing.
	Model  string
	Logger *slog.Logger
}

// Process decides one trace. It always returns a decision: if Kev cannot be
// reached after its retries, the policy's random fallback decides.
func (e *Engine) Process(ctx context.Context, t trace.Trace) Result {
	start := time.Now()
	out := Result{TraceID: t.TraceID, Model: e.Model}

	var decision policy.Decision
	res, err := e.Scorer.Score(ctx, trace.BuildState(t))
	if err != nil {
		e.Logger.Warn("kev scoring failed, falling back to a random decision",
			"trace_id", t.TraceID, "attempts", res.Attempts, "error", err.Error())
		decision = e.Policy.Fallback()
		out.LatencyMS = msSince(start)
	} else {
		decision = e.Policy.Decide(res.PRaw, scorer.Calibrate(res.PRaw, e.Temperature))
		out.PKeep, out.PKeepRaw = &decision.PKeep, &decision.PKeepRaw
		out.Model, out.LatencyMS = res.Model, res.LatencyMS
	}

	out.Decision = "drop"
	if decision.Keep {
		out.Decision = "keep"
	}
	out.Reason, out.Severity = decision.Reason, decision.Severity
	out.Exported = e.Exporter.Export(ctx, export.Record{
		Trace: t, Decision: decision, Model: out.Model, LatencyMS: out.LatencyMS,
	})

	attrs := []any{
		"trace_id", out.TraceID,
		"decision", out.Decision,
		"reason", out.Reason,
		"severity", out.Severity,
		"model", out.Model,
		"latency_ms", out.LatencyMS,
		"duration_ms", msSince(start),
		"exported", out.Exported,
	}
	if decision.Scored {
		attrs = append(attrs, "p_keep", decision.PKeep, "p_keep_raw", decision.PKeepRaw)
	}
	e.Logger.Info("decision", attrs...)
	return out
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000
}
