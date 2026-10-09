// Package policy turns a keep probability into a keep/drop decision.
package policy

import "math/rand/v2"

// Reasons recorded on a decision.
const (
	ReasonKev            = "kev"             // kept: p_keep reached the threshold
	ReasonRandomFloor    = "random_floor"    // kept: sampled below-threshold trace
	ReasonBelowThreshold = "below_threshold" // dropped: below threshold, not sampled
	ReasonFallback       = "fallback"        // Kev unavailable: random keep or drop
)

// Severities recorded on a decision.
const (
	SeverityHigh   = "high"
	SeverityNormal = "normal"
	SeverityNone   = "none"
)

// Config holds the policy thresholds, all in [0, 1].
type Config struct {
	KeepThreshold float64
	RandomFloor   float64
	SeverityHigh  float64
}

// Decision is the outcome for one trace.
type Decision struct {
	Keep bool
	// Scored is false when Kev gave no answer; PKeep and PKeepRaw are then
	// meaningless.
	Scored   bool
	PKeep    float64
	PKeepRaw float64
	Reason   string
	Severity string
}

// Policy decides keep or drop. It is safe for concurrent use as long as the
// random source is.
type Policy struct {
	cfg  Config
	rand func() float64
}

// New returns a Policy. rnd must return uniform values in [0, 1); nil uses
// math/rand, which is safe for concurrent use.
func New(cfg Config, rnd func() float64) *Policy {
	if rnd == nil {
		rnd = rand.Float64
	}
	return &Policy{cfg: cfg, rand: rnd}
}

// Decide applies the policy to a scored trace: keep when the calibrated
// probability reaches the threshold, otherwise keep a random_floor share.
func (p *Policy) Decide(pKeepRaw, pKeep float64) Decision {
	d := Decision{Scored: true, PKeep: pKeep, PKeepRaw: pKeepRaw}
	switch {
	case pKeep >= p.cfg.KeepThreshold:
		d.Keep, d.Reason = true, ReasonKev
	case p.rand() < p.cfg.RandomFloor:
		d.Keep, d.Reason = true, ReasonRandomFloor
	default:
		d.Reason = ReasonBelowThreshold
	}
	d.Severity = p.severity(d)
	return d
}

// Fallback decides for a trace Kev could not score: keep a random_floor
// share so a scorer outage still yields a sample for review.
func (p *Policy) Fallback() Decision {
	d := Decision{Reason: ReasonFallback, Severity: SeverityNone}
	if p.rand() < p.cfg.RandomFloor {
		d.Keep, d.Severity = true, SeverityNormal
	}
	return d
}

func (p *Policy) severity(d Decision) string {
	switch {
	case !d.Keep:
		return SeverityNone
	case d.PKeep >= p.cfg.SeverityHigh:
		return SeverityHigh
	default:
		return SeverityNormal
	}
}
