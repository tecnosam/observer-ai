// Package observe is where errors go. Components report them to a Reporter;
// the Pipeline turns each into an Entry, counts it, and writes it to every
// configured sink (stdout, a file, and later an OpenTelemetry collector).
package observe

import (
	"encoding/json"
	"time"

	"github.com/observer-ai/engine/pkg/fault"
)

// Level is how loudly an entry is reported.
type Level string

const (
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

// Entry is one reported error, ready for a sink.
type Entry struct {
	Time      time.Time
	Level     Level
	Op        fault.Op
	Kind      fault.Kind
	Component fault.Component
	TraceID   string
	Cause     string
	Attrs     map[string]any
}

// entryFor turns any error into an Entry. An error that is not a
// *fault.Error is reported as an internal error of an unknown component.
func entryFor(err error, now time.Time) Entry {
	e, ok := fault.As(err)
	if !ok {
		e = fault.Internal("unknown", "unclassified error", err)
	}
	entry := Entry{
		Time: now, Level: levelFor(e), Op: e.Op, Kind: e.Kind,
		Component: e.Component, TraceID: e.TraceID, Attrs: e.Attrs,
	}
	if e.Cause != nil {
		entry.Cause = e.Cause.Error()
	}
	return entry
}

// levelFor: work lost or a bug is an error; anything retried or refused is a warning.
func levelFor(e *fault.Error) Level {
	if e.Severe || e.Kind == fault.KindInternal {
		return LevelError
	}
	return LevelWarn
}

// MarshalJSON writes one flat object, in the same shape as the engine's other log lines.
func (e Entry) MarshalJSON() ([]byte, error) {
	out := map[string]any{}
	for k, v := range e.Attrs {
		out[k] = v
	}
	out["time"] = e.Time.UTC().Format(time.RFC3339Nano)
	out["level"] = e.Level
	out["msg"] = e.Op
	out["kind"] = e.Kind.String()
	out["component"] = e.Component
	if e.TraceID != "" {
		out["trace_id"] = e.TraceID
	}
	if e.Cause != "" {
		out["error"] = e.Cause
	}
	return json.Marshal(out)
}
