// Package fault is the engine's error type. Every error a component returns
// is a *fault.Error: what kind of failure it is, which component hit it and
// what it was doing. Callers branch on the kind (KindOf, Retryable), never on
// another package's sentinel values or error strings, and the observe package
// turns the same fields into log entries and metrics.
package fault

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

// Kind says what went wrong, in the terms a caller acts on.
type Kind uint8

const (
	KindInternal    Kind = iota // a bug or an unexpected state
	KindInvalid                 // the input is wrong; retrying cannot help
	KindRejected                // a dependency refused this particular request; retrying cannot help
	KindUnavailable             // a dependency is down or refusing all requests; retry later
	KindTimeout                 // a dependency did not answer in time; retry later
	KindFull                    // a bounded resource is at capacity; retry later
)

var kindNames = map[Kind]string{
	KindInternal: "internal", KindInvalid: "invalid", KindRejected: "rejected",
	KindUnavailable: "unavailable", KindTimeout: "timeout", KindFull: "full",
}

func (k Kind) String() string { return kindNames[k] }

// Component names the part of the engine an error comes from.
type Component string

const (
	Config   Component = "config"
	Server   Component = "server"
	OTLP     Component = "otlp"
	Queue    Component = "queue"
	Pipeline Component = "pipeline"
	Scorer   Component = "scorer"
	Policy   Component = "policy"
	Export   Component = "export"
)

// Op is what the component was doing, e.g. "score trace".
type Op string

// Error is one failure, with the context needed to act on it and report it.
type Error struct {
	Kind      Kind
	Component Component
	Op        Op
	TraceID   string
	// Severe marks a failure that lost or gave up on work, such as a
	// dead-lettered trace; it is reported at error level whatever its kind.
	Severe bool
	Attrs  map[string]any
	Cause  error
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("%s: %s", e.Component, e.Op)
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Cause }

// WithTrace returns a copy that names the trace it concerns.
func (e *Error) WithTrace(id string) *Error {
	out := *e
	out.TraceID = id
	return &out
}

// With returns a copy carrying one more attribute for the report.
func (e *Error) With(key string, value any) *Error {
	out := *e
	out.Attrs = maps.Clone(e.Attrs)
	if out.Attrs == nil {
		out.Attrs = map[string]any{}
	}
	out.Attrs[key] = value
	return &out
}

// AsSevere returns a copy reported at error level.
func (e *Error) AsSevere() *Error {
	out := *e
	out.Severe = true
	return &out
}

// New builds an error of any kind; the named constructors below read better.
func New(kind Kind, component Component, op Op, cause error) *Error {
	return &Error{Kind: kind, Component: component, Op: op, Cause: cause}
}

func Internal(c Component, op Op, cause error) *Error    { return New(KindInternal, c, op, cause) }
func Invalid(c Component, op Op, cause error) *Error     { return New(KindInvalid, c, op, cause) }
func Rejected(c Component, op Op, cause error) *Error    { return New(KindRejected, c, op, cause) }
func Unavailable(c Component, op Op, cause error) *Error { return New(KindUnavailable, c, op, cause) }
func Timeout(c Component, op Op, cause error) *Error     { return New(KindTimeout, c, op, cause) }
func Full(c Component, op Op, cause error) *Error        { return New(KindFull, c, op, cause) }

// Msg is a cause made of text, for failures with no underlying error.
func Msg(format string, args ...any) error { return fmt.Errorf(format, args...) }

// As returns err's *Error, if it has one anywhere in its chain.
func As(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// KindOf classifies any error. A context deadline is a timeout; anything that
// is not a *fault.Error is internal.
func KindOf(err error) Kind {
	if e, ok := As(err); ok {
		return e.Kind
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return KindTimeout
	}
	return KindInternal
}

// Retryable reports whether trying the same work again later can succeed.
func Retryable(err error) bool {
	switch KindOf(err) {
	case KindUnavailable, KindTimeout, KindFull:
		return true
	}
	return false
}
