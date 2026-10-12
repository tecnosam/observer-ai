// Package otlp receives OpenTelemetry traces and turns them into the
// engine's trace record. Spans are read using the OpenInference conventions
// (https://github.com/Arize-ai/openinference), which is what Phoenix and
// its instrumentors emit.
package otlp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/trace"
)

// OpenInference attribute names and span kinds.
const (
	attrSpanKind       = "openinference.span.kind"
	attrInputValue     = "input.value"
	attrOutputValue    = "output.value"
	attrToolName       = "tool.name"
	attrToolParameters = "tool.parameters"
	attrProjectName    = "openinference.project.name"

	kindAgent     = "AGENT"
	kindChain     = "CHAIN"
	kindTool      = "TOOL"
	kindRetriever = "RETRIEVER"

	retrievalDocFmt = "retrieval.documents.%d.document.content"
)

// Attributes an agent can set to tell the engine exactly what the model
// should see, instead of having it inferred from tool output.
const (
	// AttrEvidenceFmt, on the root span, lists the evidence items in order
	// (observer.evidence.0, observer.evidence.1, ...).
	AttrEvidenceFmt = "observer.evidence.%d"
	// AttrToolStatus, on a TOOL span, is the tool call's status.
	AttrToolStatus = "observer.tool.status"
)

// ErrNotScorable means the spans do not describe an agent run the model can
// judge: there is no root span carrying both a question and an answer.
var ErrNotScorable = errors.New("trace is not scorable")

// Split groups spans by trace. The result maps each trace ID (32 hex
// characters) to a serialized TracesData holding that trace's spans from
// this batch, with their resource and scope. Spans without a valid trace or
// span ID are skipped and counted.
func Split(batch []*tracepb.ResourceSpans) (byTrace map[string][]byte, skipped int, err error) {
	grouped := map[string]*tracepb.TracesData{}
	for _, rs := range batch {
		for _, ss := range rs.GetScopeSpans() {
			// Spans of one trace within one scope stay together.
			perTrace := map[string]*tracepb.ScopeSpans{}
			for _, span := range ss.GetSpans() {
				if !validID(span.GetTraceId(), 16) || !validID(span.GetSpanId(), 8) {
					skipped++
					continue
				}
				id := hex.EncodeToString(span.GetTraceId())
				out, ok := perTrace[id]
				if !ok {
					out = &tracepb.ScopeSpans{Scope: ss.GetScope(), SchemaUrl: ss.GetSchemaUrl()}
					perTrace[id] = out
					td := grouped[id]
					if td == nil {
						td = &tracepb.TracesData{}
						grouped[id] = td
					}
					td.ResourceSpans = append(td.ResourceSpans, &tracepb.ResourceSpans{
						Resource:   rs.GetResource(),
						SchemaUrl:  rs.GetSchemaUrl(),
						ScopeSpans: []*tracepb.ScopeSpans{out},
					})
				}
				out.Spans = append(out.Spans, span)
			}
		}
	}

	byTrace = make(map[string][]byte, len(grouped))
	for id, td := range grouped {
		data, err := proto.Marshal(td)
		if err != nil {
			return nil, skipped, fmt.Errorf("otlp: encode spans of trace %s: %w", id, err)
		}
		byTrace[id] = data
	}
	return byTrace, skipped, nil
}

func validID(id []byte, size int) bool {
	if len(id) != size {
		return false
	}
	for _, b := range id {
		if b != 0 {
			return true
		}
	}
	return false
}

// Assemble merges the span batches collected for one trace (as produced by
// Split) and builds the trace record the model is asked about. The record
// carries the merged spans in its OTLP field. It returns ErrNotScorable when
// the spans hold no agent run with a question and an answer.
func Assemble(traceID string, payloads [][]byte) (trace.Trace, error) {
	merged, spans, err := merge(payloads)
	if err != nil {
		return trace.Trace{}, err
	}
	root := findRoot(spans)
	if root == nil {
		return trace.Trace{}, fmt.Errorf("%w: no spans", ErrNotScorable)
	}
	question := messageText(attr(root, attrInputValue), questionKeys)
	answer := messageText(attr(root, attrOutputValue), answerKeys)
	if strings.TrimSpace(question) == "" {
		return trace.Trace{}, fmt.Errorf("%w: root span %q has no %s", ErrNotScorable, root.GetName(), attrInputValue)
	}
	if strings.TrimSpace(answer) == "" {
		return trace.Trace{}, fmt.Errorf("%w: root span %q has no %s", ErrNotScorable, root.GetName(), attrOutputValue)
	}

	tools := spansOfKind(spans, kindTool)
	t := trace.Trace{
		TraceID:     traceID,
		Question:    question,
		FinalAnswer: answer,
		ToolCalls:   toolCalls(tools),
		Evidence:    evidence(root, spans, tools),
	}
	if t.OTLP, err = proto.Marshal(merged); err != nil {
		return trace.Trace{}, fmt.Errorf("otlp: encode trace %s: %w", traceID, err)
	}
	return t, nil
}

// merge joins span batches, dropping spans already seen (an exporter that
// retries a batch sends its spans twice).
func merge(payloads [][]byte) (*tracepb.TracesData, []*tracepb.Span, error) {
	merged := &tracepb.TracesData{}
	var spans []*tracepb.Span
	seen := map[string]bool{}
	for _, payload := range payloads {
		var td tracepb.TracesData
		if err := proto.Unmarshal(payload, &td); err != nil {
			return nil, nil, fmt.Errorf("otlp: decode stored spans: %w", err)
		}
		for _, rs := range td.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				kept := ss.Spans[:0]
				for _, span := range ss.GetSpans() {
					if id := string(span.GetSpanId()); !seen[id] {
						seen[id] = true
						kept = append(kept, span)
					}
				}
				ss.Spans = kept
				spans = append(spans, kept...)
			}
			merged.ResourceSpans = append(merged.ResourceSpans, rs)
		}
	}
	return merged, spans, nil
}

// findRoot picks the span that represents the agent run: one whose parent is
// not among the spans, preferring an AGENT span, then a CHAIN span, then
// whichever started first.
func findRoot(spans []*tracepb.Span) *tracepb.Span {
	ids := make(map[string]bool, len(spans))
	for _, s := range spans {
		ids[string(s.GetSpanId())] = true
	}
	rank := func(s *tracepb.Span) int {
		switch attr(s, attrSpanKind) {
		case kindAgent:
			return 0
		case kindChain:
			return 1
		}
		return 2
	}
	var root *tracepb.Span
	for _, s := range spans {
		if len(s.GetParentSpanId()) != 0 && ids[string(s.GetParentSpanId())] {
			continue
		}
		if root == nil || rank(s) < rank(root) ||
			(rank(s) == rank(root) && s.GetStartTimeUnixNano() < root.GetStartTimeUnixNano()) {
			root = s
		}
	}
	return root
}

func spansOfKind(spans []*tracepb.Span, kind string) []*tracepb.Span {
	var out []*tracepb.Span
	for _, s := range spans {
		if attr(s, attrSpanKind) == kind {
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].GetStartTimeUnixNano() < out[j].GetStartTimeUnixNano()
	})
	return out
}

func toolCalls(tools []*tracepb.Span) []trace.ToolCall {
	var calls []trace.ToolCall
	for _, s := range tools {
		name := attr(s, attrToolName)
		if name == "" {
			name = s.GetName()
		}
		call := trace.ToolCall{Name: name, Args: toolArgs(s)}
		call.Status, _ = json.Marshal(toolStatus(s))
		calls = append(calls, call)
	}
	return calls
}

// toolArgs returns the call's arguments as JSON: the span's input if that is
// JSON, else its declared parameters, else the input as a JSON string.
func toolArgs(s *tracepb.Span) json.RawMessage {
	input := attr(s, attrInputValue)
	for _, candidate := range []string{input, attr(s, attrToolParameters)} {
		if candidate != "" && json.Valid([]byte(candidate)) {
			return json.RawMessage(candidate)
		}
	}
	if input == "" {
		return nil
	}
	quoted, _ := json.Marshal(input)
	return quoted
}

func toolStatus(s *tracepb.Span) string {
	if status, ok := lookup(s, AttrToolStatus); ok {
		return status
	}
	if s.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
		return "error"
	}
	return "ok"
}

// evidence returns what the agent found, from the most explicit source
// available: evidence attributes on the root span, then documents on
// RETRIEVER spans, then the paragraphs of successful tool outputs.
func evidence(root *tracepb.Span, spans, tools []*tracepb.Span) []string {
	if explicit := indexed(root, AttrEvidenceFmt); len(explicit) > 0 {
		return explicit
	}
	var out []string
	seen := map[string]bool{}
	add := func(text string) {
		if text = strings.TrimSpace(text); text != "" && !seen[text] {
			seen[text] = true
			out = append(out, text)
		}
	}
	for _, s := range spansOfKind(spans, kindRetriever) {
		for _, doc := range indexed(s, retrievalDocFmt) {
			add(doc)
		}
	}
	if len(out) > 0 {
		return out
	}
	for _, s := range tools {
		if toolStatus(s) != "ok" {
			continue
		}
		for _, paragraph := range strings.Split(messageText(attr(s, attrOutputValue), outputKeys), "\n\n") {
			add(paragraph)
		}
	}
	return out
}

// indexed collects attributes named by format with 0, 1, 2, ... until one is
// missing.
func indexed(s *tracepb.Span, format string) []string {
	var out []string
	for i := 0; ; i++ {
		v, ok := lookup(s, fmt.Sprintf(format, i))
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

var (
	questionKeys = []string{"question", "input", "query", "prompt", "text", "content"}
	answerKeys   = []string{"final_answer", "answer", "output", "result", "response", "text", "content"}
	outputKeys   = []string{"result", "output", "content", "text"}
)

// messageText extracts the text from an input or output value. Plain text is
// returned as is. A JSON object is searched for the first of keys holding a
// string, after stepping into a "response" object (the shape of a function
// response); a JSON string is unquoted. Anything else is returned unchanged.
func messageText(value string, keys []string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	switch trimmed[0] {
	case '"':
		var s string
		if json.Unmarshal([]byte(trimmed), &s) == nil {
			return s
		}
	case '{':
		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(trimmed), &obj) != nil {
			return value
		}
		if inner, ok := obj["response"]; ok {
			var nested map[string]json.RawMessage
			if json.Unmarshal(inner, &nested) == nil {
				obj = nested
			}
		}
		for _, key := range keys {
			var s string
			if raw, ok := obj[key]; ok && json.Unmarshal(raw, &s) == nil {
				return s
			}
		}
	}
	return value
}

func attr(s *tracepb.Span, key string) string {
	v, _ := lookup(s, key)
	return v
}

func lookup(s *tracepb.Span, key string) (string, bool) {
	for _, kv := range s.GetAttributes() {
		if kv.GetKey() == key {
			return anyString(kv.GetValue()), true
		}
	}
	return "", false
}

func anyString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	}
	return ""
}

// Annotate prepares a kept trace for forwarding: it decodes the spans stored
// by Assemble, files them under project, and sets attrs on the root span,
// replacing any attribute of the same name.
func Annotate(stored []byte, project string, attrs []*commonpb.KeyValue) ([]*tracepb.ResourceSpans, error) {
	var td tracepb.TracesData
	if err := proto.Unmarshal(stored, &td); err != nil {
		return nil, fmt.Errorf("otlp: decode stored spans: %w", err)
	}
	var spans []*tracepb.Span
	for _, rs := range td.GetResourceSpans() {
		if rs.Resource == nil {
			rs.Resource = &resourcepb.Resource{}
		}
		rs.Resource.Attributes = setAttr(rs.Resource.Attributes, StringAttr(attrProjectName, project))
		for _, ss := range rs.GetScopeSpans() {
			spans = append(spans, ss.GetSpans()...)
		}
	}
	root := findRoot(spans)
	if root == nil {
		return nil, fmt.Errorf("%w: no spans", ErrNotScorable)
	}
	for _, kv := range attrs {
		root.Attributes = setAttr(root.Attributes, kv)
	}
	return td.GetResourceSpans(), nil
}

func setAttr(attrs []*commonpb.KeyValue, kv *commonpb.KeyValue) []*commonpb.KeyValue {
	for i, existing := range attrs {
		if existing.GetKey() == kv.GetKey() {
			attrs[i] = kv
			return attrs
		}
	}
	return append(attrs, kv)
}

// StringAttr and FloatAttr build span attributes.
func StringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

func FloatAttr(key string, value float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: value}}}
}
