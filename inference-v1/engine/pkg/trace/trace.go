// Package trace defines the agent trace the engine receives and the state
// string ("slice") built from it for the decision model.
package trace

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// AgentName names the agent a trace came from. It picks the model that
// scores the trace; empty means the default model.
type AgentName string

// Trace is one agent run submitted for a keep/drop decision.
type Trace struct {
	TraceID   string     `json:"trace_id,omitempty"`
	Agent     AgentName  `json:"agent,omitempty"`
	Question  string     `json:"question"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	Evidence  []string   `json:"evidence,omitempty"`
	// FinalAnswer is "" for a run that ended without an answer. Such a run
	// is still scored, so the field must be present but may be empty.
	FinalAnswer string `json:"final_answer"`

	// OTLP holds the trace's original spans (a serialized OTLP TracesData)
	// when the trace arrived through the OTLP receivers. A kept trace is
	// then forwarded as these spans instead of being rebuilt from the
	// fields above. The JSON API does not accept it.
	OTLP []byte `json:"otlp_spans,omitempty"`
}

// ToolCall is one tool invocation. Args and Status stay as raw JSON because
// the slice renders them the way Python would, which depends on whether the
// key was present and on its JSON type.
type ToolCall struct {
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args,omitempty"`
	Status json.RawMessage `json:"status,omitempty"`
}

// StatusText returns the status as plain text: the string itself, "ok" when
// absent, or the Python rendering of any other JSON value.
func (c ToolCall) StatusText() string {
	if len(c.Status) == 0 {
		return "ok"
	}
	s, err := pyStr(c.Status)
	if err != nil {
		return string(c.Status)
	}
	return s
}

// Decode parses and validates a trace from JSON. final_answer must be
// present; an empty string means the agent gave no answer.
func Decode(r io.Reader) (Trace, error) {
	// The outer final_answer shadows Trace's, so a missing key can be told
	// from an empty answer.
	var in struct {
		Trace
		FinalAnswer *string `json:"final_answer"`
	}
	dec := json.NewDecoder(r)
	if err := dec.Decode(&in); err != nil {
		return Trace{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Trace{}, errors.New("invalid JSON: unexpected data after the trace object")
	}
	t := in.Trace
	if in.FinalAnswer != nil {
		t.FinalAnswer = *in.FinalAnswer
	}
	if err := t.Validate(); err != nil {
		return Trace{}, err
	}
	if in.FinalAnswer == nil {
		return Trace{}, errors.New("final_answer is required")
	}
	return t, nil
}

// Validate checks the question and that every tool call can be sliced. An
// empty FinalAnswer is valid: the run ended without an answer.
func (t Trace) Validate() error {
	if t.Question == "" {
		return errors.New("question is required")
	}
	for i, c := range t.ToolCalls {
		if c.Name == "" {
			return fmt.Errorf("tool_calls[%d].name is required", i)
		}
		if len(c.Args) > 0 {
			if _, err := pyRepr(c.Args); err != nil {
				return fmt.Errorf("tool_calls[%d].args: %w", i, err)
			}
		}
	}
	return nil
}

// NewID returns a random 32-character hex trace ID.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("trace: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// CompactArgs returns the args as compact JSON, or "" when absent.
func (c ToolCall) CompactArgs() string {
	if len(c.Args) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, c.Args); err != nil {
		return string(c.Args)
	}
	return buf.String()
}
