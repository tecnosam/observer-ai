// Package policy turns a keep probability into a keep/drop decision. It
// implements pipeline.Policy.
package policy

import (
	"math/rand/v2"

	"github.com/observer-ai/engine/pkg/pipeline"
)

// Config holds the policy thresholds.
type Config struct {
	// KeepThreshold: keep a trace whose calibrated probability reaches it.
	KeepThreshold pipeline.Probability
	// RandomFloor: the share of the remaining traces kept at random.
	RandomFloor pipeline.Probability
	// SeverityHigh: a kept trace at or above it is severity high.
	SeverityHigh pipeline.Probability
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
func (p *Policy) Decide(raw, calibrated pipeline.Probability) pipeline.Decision {
	d := pipeline.Decision{Scored: true, PKeep: calibrated, PKeepRaw: raw}
	d.Keep, d.Reason = p.keep(calibrated)
	d.Severity = p.severity(d)
	return d
}

func (p *Policy) keep(calibrated pipeline.Probability) (bool, pipeline.Reason) {
	switch {
	case calibrated >= p.cfg.KeepThreshold:
		return true, pipeline.ReasonModel
	case p.sampled():
		return true, pipeline.ReasonRandomFloor
	}
	return false, pipeline.ReasonBelowThreshold
}

// Fallback decides for a trace the model could not score: keep a
// random_floor share so a scorer outage still yields a sample for review.
func (p *Policy) Fallback() pipeline.Decision {
	if p.sampled() {
		return pipeline.Decision{Keep: true, Reason: pipeline.ReasonFallback, Severity: pipeline.SeverityNormal}
	}
	return pipeline.Decision{Reason: pipeline.ReasonFallback, Severity: pipeline.SeverityNone}
}

func (p *Policy) sampled() bool {
	return pipeline.Probability(p.rand()) < p.cfg.RandomFloor
}

func (p *Policy) severity(d pipeline.Decision) pipeline.Severity {
	switch {
	case !d.Keep:
		return pipeline.SeverityNone
	case d.PKeep >= p.cfg.SeverityHigh:
		return pipeline.SeverityHigh
	}
	return pipeline.SeverityNormal
}
