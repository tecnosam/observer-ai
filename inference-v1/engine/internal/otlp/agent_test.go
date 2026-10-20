package otlp

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/observer-ai/engine/pkg/trace"
)

// assembleWith runs spans sent by service through an Assembler looking for
// the agent in attrs.
func assembleWith(t *testing.T, attrs []string, service string, spans ...*tracepb.Span) trace.Trace {
	t.Helper()
	byTrace, _, err := Split([]*tracepb.ResourceSpans{resourceSpans(service, spans...)})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Assembler{AgentAttributes: attrs}.Assemble(testTraceHex, [][]byte{byTrace[testTraceHex]})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func rootWith(kv ...string) *tracepb.Span {
	return span(1, 0, "run", 100, append([]string{
		attrSpanKind, kindAgent, attrInputValue, "q", attrOutputValue, "a",
	}, kv...)...)
}

func TestAssemblerFindsTheAgent(t *testing.T) {
	attrs := []string{"agent.name", "service.name"}
	cases := []struct {
		name  string
		attrs []string
		root  *tracepb.Span
		want  trace.AgentName
	}{
		{"root span attribute", attrs, rootWith("agent.name", "support-bot"), "support-bot"},
		{"resource attribute when the span has none", attrs, rootWith(), "billing-service"},
		{"first attribute wins", []string{"service.name", "agent.name"}, rootWith("agent.name", "support-bot"), "billing-service"},
		{"no attributes configured", nil, rootWith("agent.name", "support-bot"), ""},
		{"attribute set nowhere", []string{"team"}, rootWith(), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := assembleWith(t, tc.attrs, "billing-service", tc.root).Agent; got != tc.want {
				t.Errorf("agent = %q, want %q", got, tc.want)
			}
		})
	}
}

// The plain Assemble names no agent: the trace goes to the default model.
func TestAssembleNamesNoAgent(t *testing.T) {
	tr, err := assemble(t, []*tracepb.Span{rootWith("agent.name", "support-bot")})
	if err != nil || tr.Agent != "" {
		t.Errorf("agent = %q, error %v", tr.Agent, err)
	}
}
