package common

import (
	"context"
	"sync"
)

// Wait waits for wg, or until ctx ends.
func Wait(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OrDefault is v if it is set (positive), else def.
func OrDefault(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}
