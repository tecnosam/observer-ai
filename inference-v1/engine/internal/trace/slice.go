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

// BuildState renders the trace into the state string the decision model was
// trained on. It must stay byte-for-byte identical to the Python build_state
// (see testdata/gen_golden.py); the golden tests enforce that.
func BuildState(t Trace) string {
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
		b.WriteString(c.argsRepr())
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
	return b.String()
}

// argsRepr is the Python repr of the args, or of the empty string when the
// call has none (c.get("args", "") in the training code).
func (c ToolCall) argsRepr() string {
	if len(c.Args) == 0 {
		return "''"
	}
	s, err := pyRepr(c.Args)
	if err != nil {
		// Validate rejects such args before a trace gets here.
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
