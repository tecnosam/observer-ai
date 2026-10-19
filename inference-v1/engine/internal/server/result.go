package server

import "github.com/observer-ai/engine/internal/pipeline"

// Result is the decision for one trace, as returned by POST /v1/traces:score.
// p_keep and p_keep_raw are null when the model could not be reached and the
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
	// On the sync path it also means the flush did not fail. On the queued
	// path delivery is batched and failures are only reported.
	Exported bool `json:"exported"`
}

func resultOf(out pipeline.Outcome) Result {
	rec, d := out.Record, out.Record.Decision
	res := Result{
		TraceID:   rec.Trace.TraceID,
		Decision:  d.Label(),
		Reason:    string(d.Reason),
		Severity:  string(d.Severity),
		Model:     string(rec.Model),
		LatencyMS: pipeline.Milliseconds(rec.Latency),
		Exported:  out.Exported,
	}
	if d.Scored {
		pKeep, pKeepRaw := float64(d.PKeep), float64(d.PKeepRaw)
		res.PKeep, res.PKeepRaw = &pKeep, &pKeepRaw
	}
	return res
}
