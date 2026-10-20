package trace

import (
	"regexp"
	"strings"
	"testing"
)

func TestDecode(t *testing.T) {
	in := `{
		"trace_id": "7f3a91c2e8",
		"question": "Which river flows through the capital of France?",
		"tool_calls": [{"name": "search", "args": {"query": "capital of France"}, "status": "ok"}],
		"evidence": ["Paris is the capital of France.", "The Seine flows through Paris."],
		"final_answer": "The Seine",
		"unknown_field": 1
	}`
	tr, err := Decode(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if tr.TraceID != "7f3a91c2e8" || tr.FinalAnswer != "The Seine" || len(tr.Evidence) != 2 {
		t.Errorf("decoded trace = %+v", tr)
	}
	if got := tr.ToolCalls[0].CompactArgs(); got != `{"query":"capital of France"}` {
		t.Errorf("CompactArgs = %s", got)
	}
}

// A run that ended without an answer is a valid trace.
func TestDecodeEmptyFinalAnswer(t *testing.T) {
	tr, err := Decode(strings.NewReader(`{"question": "q", "final_answer": ""}`))
	if err != nil {
		t.Fatal(err)
	}
	if tr.Question != "q" || tr.FinalAnswer != "" {
		t.Errorf("got %q / %q", tr.Question, tr.FinalAnswer)
	}
	if got := string(BuildState(tr)); !strings.HasSuffix(got, "FINAL ANSWER:\n") {
		t.Errorf("state ends %q, want an empty final answer", got[len(got)-20:])
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct{ name, in, wantErr string }{
		{"not JSON", `{"question": `, "invalid JSON"},
		{"missing question", `{"final_answer": "a"}`, "question is required"},
		{"empty question", `{"question": "", "final_answer": "a"}`, "question is required"},
		{"missing final answer", `{"question": "q"}`, "final_answer is required"},
		{"null final answer", `{"question": "q", "final_answer": null}`, "final_answer is required"},
		{"wrong type", `{"question": 5, "final_answer": "a"}`, "invalid JSON"},
		{"evidence not strings", `{"question": "q", "final_answer": "a", "evidence": [1]}`, "invalid JSON"},
		{"tool call without name", `{"question": "q", "final_answer": "a", "tool_calls": [{"args": "x"}]}`, "tool_calls[0].name is required"},
		{"trailing data", `{"question": "q", "final_answer": "a"} {}`, "unexpected data"},
		{"array", `[]`, "invalid JSON"},
		{"agent too long", `{"question": "q", "final_answer": "a", "agent": "` + strings.Repeat("x", MaxAgentLength+1) + `"}`, "agent is longer than 128 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(strings.NewReader(tt.in))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestStatusText(t *testing.T) {
	tests := []struct{ status, want string }{
		{``, "ok"},
		{`"error"`, "error"},
		{`""`, ""},
		{`null`, "null"},
		{`404`, "404"},
		{`true`, "true"},
		{`{"code": 404}`, `{"code":404}`},
	}
	for _, tt := range tests {
		if got := (ToolCall{Name: "t", Status: []byte(tt.status)}).StatusText(); got != tt.want {
			t.Errorf("StatusText(%s) = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestNewID(t *testing.T) {
	hex32 := regexp.MustCompile(`^[0-9a-f]{32}$`)
	a, b := NewID(), NewID()
	if !hex32.MatchString(a) || !hex32.MatchString(b) {
		t.Errorf("IDs are not 32 hex chars: %q %q", a, b)
	}
	if a == b {
		t.Error("two generated IDs are equal")
	}
}
