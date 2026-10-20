package trace

import (
	"strconv"
	"strings"
)

// These mirror MAX_EVIDENCE and MAX_EVIDENCE_CHARS in the Python training
// code. Changing them changes what the model sees.
const (
	MaxEvidence      = 3
	MaxEvidenceChars = 1500
)

// State is the text of a trace the decision model reads.
type State string

// BuildState renders the trace into the state string the decision model was
// trained on. It must stay identical to the fine-tuning SDK's build_state
// (finetune/observer_finetune/observer_finetune/state.py, and its copy in
// testdata/gen_golden.py); the golden tests on both sides enforce that.
func BuildState(t Trace) State {
	var b strings.Builder

	b.WriteString("USER QUESTION:\n")
	b.WriteString(t.Question)

	b.WriteString("\n\nTOOL CALLS:\n")
	if len(t.ToolCalls) == 0 {
		b.WriteString("(none)")
	}
	for i, c := range t.ToolCalls {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString(". ")
		b.WriteString(c.Name)
		b.WriteByte('(')
		b.WriteString(c.ArgsText())
		b.WriteString(") -> ")
		b.WriteString(c.StatusText())
	}

	b.WriteString("\n\nEVIDENCE:\n")
	evidence := t.Evidence
	if len(evidence) > MaxEvidence {
		evidence = evidence[:MaxEvidence]
	}
	if len(evidence) == 0 {
		b.WriteString("(none)")
	}
	for i, e := range evidence {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteByte('[')
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString("] ")
		b.WriteString(truncateRunes(e, MaxEvidenceChars))
	}

	b.WriteString("\n\nFINAL ANSWER:\n")
	b.WriteString(t.FinalAnswer)
	return State(b.String())
}

// ArgsText is the call's arguments as compact JSON, or "" when it has none.
func (c ToolCall) ArgsText() string {
	if len(c.Args) == 0 {
		return ""
	}
	s, err := compactJSON(c.Args)
	if err != nil {
		// Decode only accepts valid JSON, so a trace never gets here.
		return string(c.Args)
	}
	return s
}

// truncateRunes keeps the first n code points of s, like Python's s[:n].
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}
