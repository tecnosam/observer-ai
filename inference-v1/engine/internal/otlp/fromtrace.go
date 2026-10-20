package otlp

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/observer-ai/engine/pkg/trace"
)

// SpansFromTrace renders a trace record as OpenInference spans: an AGENT root
// and one TOOL child per tool call. It sets the override attributes
// (evidence on the root, status on each tool), so sending these spans to the
// receiver reproduces the record exactly. Tests and sample senders use it.
func SpansFromTrace(traceID []byte, t trace.Trace) []*tracepb.Span {
	const rootID = 1
	id := func(n int) []byte { return []byte{0, 0, 0, 0, 0, 0, byte(n >> 8), byte(n)} }

	root := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            id(rootID),
		Name:              "agent_run",
		StartTimeUnixNano: 1_000,
		EndTimeUnixNano:   1_000_000,
		Attributes: []*commonpb.KeyValue{
			StringAttr(attrSpanKind, kindAgent),
			StringAttr(attrInputValue, t.Question),
			StringAttr(attrOutputValue, t.FinalAnswer),
		},
	}
	for i, e := range t.Evidence {
		root.Attributes = append(root.Attributes, StringAttr(fmt.Sprintf(AttrEvidenceFmt, i), e))
	}

	spans := []*tracepb.Span{root}
	for i, c := range t.ToolCalls {
		start := uint64(2_000 + i*1_000)
		s := &tracepb.Span{
			TraceId:           traceID,
			SpanId:            id(rootID + 1 + i),
			ParentSpanId:      id(rootID),
			Name:              c.Name,
			StartTimeUnixNano: start,
			EndTimeUnixNano:   start + 500,
			Attributes: []*commonpb.KeyValue{
				StringAttr(attrSpanKind, kindTool),
				StringAttr(attrToolName, c.Name),
				StringAttr(AttrToolStatus, c.StatusText()),
			},
		}
		if len(c.Args) > 0 {
			s.Attributes = append(s.Attributes, StringAttr(attrInputValue, string(c.Args)))
		}
		spans = append(spans, s)
	}
	return spans
}
