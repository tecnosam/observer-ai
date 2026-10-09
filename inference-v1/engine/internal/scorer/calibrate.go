package scorer

import "math"

const probEps = 1e-6

// Calibrate applies temperature scaling to a raw probability:
// sigmoid(logit(p) / T), with p clamped to [1e-6, 1 - 1e-6]. T = 1 leaves p
// unchanged (apart from the clamp); T > 1 pulls p toward 0.5.
func Calibrate(pRaw, temperature float64) float64 {
	if math.IsNaN(pRaw) {
		return 0.5
	}
	p := math.Min(math.Max(pRaw, probEps), 1-probEps)
	if !(temperature > 0) || temperature == 1 {
		return p
	}
	logit := math.Log(p / (1 - p))
	return 1 / (1 + math.Exp(-logit/temperature))
}
