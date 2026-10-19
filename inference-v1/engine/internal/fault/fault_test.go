package fault

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestErrorKeepsItsContextThroughWrapping(t *testing.T) {
	cause := errors.New("connection refused")
	err := fmt.Errorf("worker: %w", Unavailable(Scorer, "score trace", cause).WithTrace("t1").With("attempts", 4))

	e, ok := As(err)
	if !ok {
		t.Fatal("As found no *Error in the chain")
	}
	if e.Kind != KindUnavailable || e.Component != Scorer || e.Op != "score trace" || e.TraceID != "t1" || e.Attrs["attempts"] != 4 {
		t.Errorf("error = %+v", e)
	}
	if !errors.Is(err, cause) {
		t.Error("the cause is not reachable with errors.Is")
	}
	if got := e.Error(); got != "scorer: score trace: connection refused" {
		t.Errorf("Error() = %q", got)
	}
}

func TestCopiesDoNotShareState(t *testing.T) {
	base := Invalid(OTLP, "store spans", nil)
	one := base.With("spans", 1)
	two := base.With("spans", 2)
	if base.Attrs != nil || one.Attrs["spans"] != 1 || two.Attrs["spans"] != 2 {
		t.Errorf("base %v, one %v, two %v", base.Attrs, one.Attrs, two.Attrs)
	}
	if base.AsSevere(); base.Severe {
		t.Error("AsSevere changed the original")
	}
}

func TestKindOfAndRetryable(t *testing.T) {
	tests := []struct {
		err       error
		kind      Kind
		retryable bool
	}{
		{Unavailable(Queue, "publish", nil), KindUnavailable, true},
		{Full(Queue, "publish", nil), KindFull, true},
		{Timeout(Scorer, "score", nil), KindTimeout, true},
		{Rejected(Scorer, "score", nil), KindRejected, false},
		{Invalid(Server, "decode", nil), KindInvalid, false},
		{fmt.Errorf("x: %w", context.DeadlineExceeded), KindTimeout, true},
		{errors.New("plain"), KindInternal, false},
	}
	for _, tt := range tests {
		if got := KindOf(tt.err); got != tt.kind {
			t.Errorf("KindOf(%v) = %s, want %s", tt.err, got, tt.kind)
		}
		if got := Retryable(tt.err); got != tt.retryable {
			t.Errorf("Retryable(%v) = %v", tt.err, got)
		}
	}
}
