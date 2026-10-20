package common

import "time"

// RetryPolicy is how often, and how far apart, a piece of work is tried.
type RetryPolicy struct {
	// MaxDeliver is how many tries in all, the first included.
	MaxDeliver int
	// Backoff[i] is the wait after try i+1 fails; the last value repeats.
	// Empty means no wait.
	Backoff []time.Duration
}

// Exhausted reports whether attempts tries are all the policy allows.
func (p RetryPolicy) Exhausted(attempts int) bool {
	return attempts >= p.MaxDeliver
}

// Delay is the wait before the next try, after attempts tries so far.
func (p RetryPolicy) Delay(attempts int) time.Duration {
	if len(p.Backoff) == 0 {
		return 0
	}
	return p.Backoff[min(max(attempts-1, 0), len(p.Backoff)-1)]
}
