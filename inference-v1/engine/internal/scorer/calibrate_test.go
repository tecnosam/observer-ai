package scorer

import (
	"math"
	"testing"
)

func TestCalibrateIdentityAtT1(t *testing.T) {
	for _, p := range []float64{0.001, 0.02, 0.25, 0.5, 0.6087, 0.93, 0.999} {
		if got := Calibrate(p, 1); got != p {
			t.Errorf("Calibrate(%v, 1) = %v, want %v", p, got, p)
		}
	}
}

func TestCalibrateMatchesFormula(t *testing.T) {
	// sigmoid(logit(0.93) / 0.86), worked by hand: logit = ln(0.93/0.07).
	want := 1 / (1 + math.Exp(-math.Log(0.93/0.07)/0.86))
	if got := Calibrate(0.93, 0.86); math.Abs(got-want) > 1e-15 {
		t.Errorf("Calibrate(0.93, 0.86) = %v, want %v", got, want)
	}
	// A temperature a hair off 1 takes the formula path and must agree with
	// the identity to within rounding.
	if got := Calibrate(0.3, 1+1e-12); math.Abs(got-0.3) > 1e-9 {
		t.Errorf("Calibrate(0.3, 1+1e-12) = %v, want ~0.3", got)
	}
}

func TestCalibrateHighTemperaturePullsTowardHalf(t *testing.T) {
	for _, p := range []float64{0.01, 0.2, 0.45, 0.55, 0.8, 0.99} {
		for _, temp := range []float64{1.5, 3, 10} {
			got := Calibrate(p, temp)
			if math.Abs(got-0.5) >= math.Abs(p-0.5) {
				t.Errorf("Calibrate(%v, %v) = %v is not closer to 0.5", p, temp, got)
			}
			if (p > 0.5) != (got > 0.5) {
				t.Errorf("Calibrate(%v, %v) = %v crossed 0.5", p, temp, got)
			}
		}
	}
	if got := Calibrate(0.5, 4); got != 0.5 {
		t.Errorf("Calibrate(0.5, 4) = %v, want 0.5", got)
	}
}

func TestCalibrateLowTemperatureSharpens(t *testing.T) {
	if got := Calibrate(0.7, 0.5); got <= 0.7 {
		t.Errorf("Calibrate(0.7, 0.5) = %v, want > 0.7", got)
	}
	if got := Calibrate(0.3, 0.5); got >= 0.3 {
		t.Errorf("Calibrate(0.3, 0.5) = %v, want < 0.3", got)
	}
}

func TestCalibrateIsMonotonic(t *testing.T) {
	for _, temp := range []float64{0.5, 0.86, 1, 2} {
		prev := -1.0
		for p := 0.0; p <= 1.0; p += 0.01 {
			got := Calibrate(p, temp)
			if got < prev {
				t.Fatalf("T=%v: Calibrate(%v) = %v < previous %v", temp, p, got, prev)
			}
			prev = got
		}
	}
}

func TestCalibrateExtremeInputs(t *testing.T) {
	ps := []float64{0, 1, -1, 2, 1e-300, 1 - 1e-16, math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), math.NaN()}
	temps := []float64{1e-12, 1e-3, 0.86, 1, 1e3, 1e12, 0, -1, math.Inf(1), math.NaN()}
	for _, p := range ps {
		for _, temp := range temps {
			got := Calibrate(p, temp)
			if math.IsNaN(got) || got < 0 || got > 1 {
				t.Errorf("Calibrate(%v, %v) = %v, want a probability", p, temp, got)
			}
		}
	}
	// Inputs are clamped to [1e-6, 1 - 1e-6] before scaling.
	if got := Calibrate(0, 1); got != 1e-6 {
		t.Errorf("Calibrate(0, 1) = %v, want 1e-6", got)
	}
	if got := Calibrate(1, 1); got != 1-1e-6 {
		t.Errorf("Calibrate(1, 1) = %v, want 1 - 1e-6", got)
	}
}
