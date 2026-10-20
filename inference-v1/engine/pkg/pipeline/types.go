// Package pipeline is the core of the engine: the domain types every other
// package speaks, the interfaces the engine needs from its parts (a scorer, a
// policy, an exporter, a publisher, metrics), and the Engine that runs one
// trace through them. Adapters import this package; it imports none of them.
package pipeline

import (
	"time"

	"github.com/observer-ai/engine/pkg/trace"
)

// Probability is a probability in [0, 1].
type Probability float64

// Temperature calibrates a probability; 1 leaves it unchanged.
type Temperature float64

// ModelName names the decision model that scored a trace.
type ModelName string

// Reason says why a trace was kept or dropped.
type Reason string

const (
	ReasonModel          Reason = "kev"             // kept: the calibrated probability reached the threshold
	ReasonRandomFloor    Reason = "random_floor"    // kept: a below-threshold trace sampled at random
	ReasonBelowThreshold Reason = "below_threshold" // dropped: below the threshold, not sampled
	ReasonFallback       Reason = "fallback"        // the model gave no answer: a random keep or drop
)

// Severity grades a kept trace.
type Severity string

const (
	SeverityHigh   Severity = "high"
	SeverityNormal Severity = "normal"
	SeverityNone   Severity = "none"
)

// Score is a decision model's answer for one trace.
type Score struct {
	Raw      Probability // uncalibrated probability that the answer is wrong
	Model    ModelName
	Latency  time.Duration // as the model server reported it, else the round trip
	Attempts int
}

// Decision is what happens to one trace.
type Decision struct {
	Keep bool
	// Scored is false when the model gave no answer; the probabilities are
	// then meaningless.
	Scored   bool
	PKeep    Probability // calibrated
	PKeepRaw Probability
	Reason   Reason
	Severity Severity
}

// Record is one decided trace, as exporters receive it.
type Record struct {
	Trace trace.Trace
	// Agent is the trace's agent as metrics and logs show it.
	Agent    AgentLabel
	Decision Decision
	Model    ModelName
	Latency  time.Duration // the scoring latency
}

// Outcome is everything the engine did with one trace.
type Outcome struct {
	Record   Record
	Exported bool          // handed to the exporter (kept traces only)
	Duration time.Duration // the whole run through the pipeline
}
