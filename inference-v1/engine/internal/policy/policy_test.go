package policy

import "testing"

var testConfig = Config{KeepThreshold: 0.5, RandomFloor: 0.02, SeverityHigh: 0.8}

// fixed returns a random source that yields the given values in order and
// fails the test if it is asked for more.
func fixed(t *testing.T, values ...float64) (rnd func() float64, draws *int) {
	t.Helper()
	n := 0
	return func() float64 {
		if n >= len(values) {
			t.Fatalf("random source drawn %d times, only %d values provided", n+1, len(values))
		}
		v := values[n]
		n++
		return v
	}, &n
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name      string
		pKeep     float64
		draw      []float64 // values the random source will return
		wantKeep  bool
		wantWhy   string
		wantSev   string
		wantDraws int
	}{
		{"kev keep, high severity", 0.93, nil, true, ReasonKev, SeverityHigh, 0},
		{"kev keep at severity boundary", 0.8, nil, true, ReasonKev, SeverityHigh, 0},
		{"kev keep, normal severity", 0.65, nil, true, ReasonKev, SeverityNormal, 0},
		{"kev keep at threshold", 0.5, nil, true, ReasonKev, SeverityNormal, 0},
		{"random-floor keep", 0.1, []float64{0.01}, true, ReasonRandomFloor, SeverityNormal, 1},
		{"drop just below threshold", 0.4999, []float64{0.5}, false, ReasonBelowThreshold, SeverityNone, 1},
		{"drop at floor boundary", 0.1, []float64{0.02}, false, ReasonBelowThreshold, SeverityNone, 1},
		{"drop", 0.1, []float64{0.9}, false, ReasonBelowThreshold, SeverityNone, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rnd, draws := fixed(t, tt.draw...)
			d := New(testConfig, rnd).Decide(tt.pKeep-0.05, tt.pKeep)
			if d.Keep != tt.wantKeep || d.Reason != tt.wantWhy || d.Severity != tt.wantSev {
				t.Errorf("decision = keep:%v reason:%s severity:%s, want keep:%v reason:%s severity:%s",
					d.Keep, d.Reason, d.Severity, tt.wantKeep, tt.wantWhy, tt.wantSev)
			}
			if !d.Scored || d.PKeep != tt.pKeep || d.PKeepRaw != tt.pKeep-0.05 {
				t.Errorf("probabilities not recorded: %+v", d)
			}
			if *draws != tt.wantDraws {
				t.Errorf("random draws = %d, want %d", *draws, tt.wantDraws)
			}
		})
	}
}

func TestFallback(t *testing.T) {
	rnd, _ := fixed(t, 0.01, 0.5)
	p := New(testConfig, rnd)

	keep := p.Fallback()
	if !keep.Keep || keep.Reason != ReasonFallback || keep.Severity != SeverityNormal || keep.Scored {
		t.Errorf("fallback keep = %+v", keep)
	}
	drop := p.Fallback()
	if drop.Keep || drop.Reason != ReasonFallback || drop.Severity != SeverityNone || drop.Scored {
		t.Errorf("fallback drop = %+v", drop)
	}
}

func TestRandomFloorExtremes(t *testing.T) {
	never := New(Config{KeepThreshold: 0.5, RandomFloor: 0, SeverityHigh: 0.8}, func() float64 { return 0 })
	if d := never.Decide(0.1, 0.1); d.Keep {
		t.Errorf("random_floor 0 kept a trace: %+v", d)
	}
	if d := never.Fallback(); d.Keep {
		t.Errorf("random_floor 0 kept a fallback trace: %+v", d)
	}

	always := New(Config{KeepThreshold: 0.5, RandomFloor: 1, SeverityHigh: 0.8}, func() float64 { return 0.999999 })
	if d := always.Decide(0.1, 0.1); !d.Keep || d.Reason != ReasonRandomFloor {
		t.Errorf("random_floor 1 dropped a trace: %+v", d)
	}
}

// With severity_high below the threshold a random-floor keep can still be
// high severity: severity depends only on p_keep.
func TestSeverityFollowsPKeep(t *testing.T) {
	p := New(Config{KeepThreshold: 0.9, RandomFloor: 1, SeverityHigh: 0.6}, func() float64 { return 0 })
	if d := p.Decide(0.7, 0.7); d.Reason != ReasonRandomFloor || d.Severity != SeverityHigh {
		t.Errorf("decision = %+v, want a high-severity random_floor keep", d)
	}
}

func TestDefaultRandomSourceRate(t *testing.T) {
	p := New(Config{KeepThreshold: 0.5, RandomFloor: 0.2, SeverityHigh: 0.8}, nil)
	kept := 0
	const n = 20000
	for i := 0; i < n; i++ {
		if p.Decide(0.1, 0.1).Keep {
			kept++
		}
	}
	if rate := float64(kept) / n; rate < 0.17 || rate > 0.23 {
		t.Errorf("random-floor keep rate = %.3f, want about 0.20", rate)
	}
}
