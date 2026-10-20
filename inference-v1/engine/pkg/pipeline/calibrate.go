package pipeline

import "math"

const probEps = 1e-6

// Calibrate applies temperature scaling to a raw probability:
// sigmoid(logit(p) / T), with p clamped to [1e-6, 1 - 1e-6]. T = 1 leaves p
// unchanged (apart from the clamp); T > 1 pulls p toward 0.5.
func Calibrate(raw Probability, t Temperature) Probability {
	p := float64(raw)
	if math.IsNaN(p) {
		return 0.5
	}
	p = math.Min(math.Max(p, probEps), 1-probEps)
	if !(t > 0) || t == 1 {
		return Probability(p)
	}
	logit := math.Log(p / (1 - p))
	return Probability(1 / (1 + math.Exp(-logit/float64(t))))
}
