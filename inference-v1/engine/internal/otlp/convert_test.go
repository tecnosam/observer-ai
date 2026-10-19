package otlp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/trace"
)

var testTraceID = []byte{0x5b, 0x8e, 0xfa, 0x3c, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

const testTraceHex = "5b8efa3c0102030405060708090a0b0c"

func spanID(n byte) []byte { return []byte{0, 0, 0, 0, 0, 0, 0, n} }

// span builds a span in the test trace. kv is alternating keys and values.
func span(id, parent byte, name string, start uint64, kv ...string) *tracepb.Span {
	s := &tracepb.Span{
		TraceId:           testTraceID,
		SpanId:            spanID(id),
		Name:              name,
		StartTimeUnixNano: start,
		EndTimeUnixNano:   start + 10,
	}
	if parent != 0 {
		s.ParentSpanId = spanID(parent)
	}
	for i := 0; i < len(kv); i += 2 {
		s.Attributes = append(s.Attributes, StringAttr(kv[i], kv[i+1]))
	}
	return s
}

func resourceSpans(service string, spans ...*tracepb.Span) *tracepb.ResourceSpans {
	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			StringAttr("service.name", service),
			StringAttr(attrProjectName, "observer_agent"),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: "test"}, Spans: spans}},
	}
}

// assemble runs spans through Split and Assemble, as the receiver and the
// assembler do, optionally in several batches.
func assemble(t *testing.T, batches ...[]*tracepb.Span) (trace.Trace, error) {
	t.Helper()
	var payloads [][]byte
	for _, batch := range batches {
		byTrace, skipped, err := Split([]*tracepb.ResourceSpans{resourceSpans("observer_agent", batch...)})
		if err != nil {
			t.Fatalf("Split: %v", err)
		}
		if skipped != 0 || len(byTrace) != 1 {
			t.Fatalf("Split: %d traces, %d skipped; want 1 and 0", len(byTrace), skipped)
		}
		payloads = append(payloads, byTrace[testTraceHex])
	}
	return Assemble(testTraceHex, payloads)
}

// adkRun is the shape the test agent produces with the Google ADK
// OpenInference instrumentor: an AGENT root, nested framework spans, LLM
// spans, and TOOL spans whose output is a JSON function response.
func adkRun() []*tracepb.Span {
	return []*tracepb.Span{
		span(1, 0, "observer_agent.run", 100,
			attrSpanKind, kindAgent,
			attrInputValue, "Which river flows through the capital of France?",
			attrOutputValue, "Seine",
			"observer.stop_reason", "answered"),
		span(2, 1, "invocation [observer_agent]", 110,
			attrSpanKind, kindChain,
			attrInputValue, `{"user_id": "observer", "new_message": {"parts": [{"text": "Which river?"}]}}`,
			attrOutputValue, `{"content": {"parts": [{"text": "Seine"}]}}`),
		span(3, 2, "call_llm", 120, attrSpanKind, "LLM", attrInputValue, `{"contents": []}`),
		// Started second, listed first: tool calls are ordered by start time.
		span(5, 2, "execute_tool lookup", 300,
			attrSpanKind, kindTool, attrToolName, "lookup",
			attrInputValue, `{"title": "Paris"}`, attrToolParameters, `{"title": "Paris"}`,
			attrOutputValue, `{"id": "call_2", "name": "lookup", "response": {"result": "[Paris] Paris is the capital of France."}}`),
		span(4, 2, "execute_tool search", 200,
			attrSpanKind, kindTool, attrToolName, "search",
			attrInputValue, `{"query": "capital of France river"}`,
			attrOutputValue, `{"id": "call_1", "name": "search", "response": {"result": "[Paris] Paris is the capital of France.\n\n[Seine] The Seine flows through Paris."}}`),
		span(6, 2, "call_llm", 400, attrSpanKind, "LLM"),
	}
}

func TestAssembleADKShapedTrace(t *testing.T) {
	got, err := assemble(t, adkRun())
	if err != nil {
		t.Fatal(err)
	}
	if got.TraceID != testTraceHex {
		t.Errorf("trace ID = %q", got.TraceID)
	}
	// The AGENT root wins over the CHAIN span nested under it.
	if got.Question != "Which river flows through the capital of France?" || got.FinalAnswer != "Seine" {
		t.Errorf("question = %q, final answer = %q", got.Question, got.FinalAnswer)
	}
	if len(got.ToolCalls) != 2 {
		t.Fatalf("tool calls = %+v, want 2 (LLM and CHAIN spans are not tool calls)", got.ToolCalls)
	}
	search, lookup := got.ToolCalls[0], got.ToolCalls[1]
	if search.Name != "search" || string(search.Args) != `{"query": "capital of France river"}` || search.StatusText() != "ok" {
		t.Errorf("first tool call = %s(%s) -> %s", search.Name, search.Args, search.StatusText())
	}
	if lookup.Name != "lookup" || string(lookup.Args) != `{"title": "Paris"}` {
		t.Errorf("second tool call = %s(%s)", lookup.Name, lookup.Args)
	}
	// Evidence: the paragraphs the tools returned, in order, without repeats.
	wantEvidence := []string{"[Paris] Paris is the capital of France.", "[Seine] The Seine flows through Paris."}
	if !reflect.DeepEqual(got.Evidence, wantEvidence) {
		t.Errorf("evidence = %q, want %q", got.Evidence, wantEvidence)
	}

	want := "USER QUESTION:\nWhich river flows through the capital of France?\n\n" +
		"TOOL CALLS:\n1. search({'query': 'capital of France river'}) -> ok\n2. lookup({'title': 'Paris'}) -> ok\n\n" +
		"EVIDENCE:\n[1] [Paris] Paris is the capital of France.\n[2] [Seine] The Seine flows through Paris.\n\n" +
		"FINAL ANSWER:\nSeine"
	if state := string(trace.BuildState(got)); state != want {
		t.Errorf("state =\n%s\nwant\n%s", state, want)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("assembled trace is not valid: %v", err)
	}
}

// With the override attributes set, a trace sent as spans produces exactly
// the slice the same trace produces through the JSON API, for every fixture.
func TestAssembleWithOverridesMatchesGolden(t *testing.T) {
	paths, err := filepath.Glob("../../testdata/traces/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no trace fixtures: %v", err)
	}
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			src, err := trace.Decode(f)
			if err != nil {
				t.Fatal(err)
			}
			golden, err := os.ReadFile(filepath.Join("../../testdata/golden", name+".txt"))
			if err != nil {
				t.Fatal(err)
			}

			got, err := assemble(t, SpansFromTrace(testTraceID, src))
			if err != nil {
				t.Fatalf("Assemble: %v", err)
			}
			if state := string(trace.BuildState(got)); state != string(golden) {
				t.Errorf("state via OTLP differs from the golden slice\n got %q\nwant %q", state, golden)
			}
		})
	}
}

func TestAssembleEvidenceSources(t *testing.T) {
	root := func(extra ...string) *tracepb.Span {
		return span(1, 0, "run", 100, append([]string{attrSpanKind, kindAgent, attrInputValue, "q", attrOutputValue, "a"}, extra...)...)
	}
	tool := span(2, 1, "search", 200, attrSpanKind, kindTool, attrToolName, "search",
		attrOutputValue, "tool paragraph one\n\ntool paragraph two")
	retriever := span(3, 1, "retrieve", 150, attrSpanKind, kindRetriever,
		fmt.Sprintf(retrievalDocFmt, 0), "retrieved doc A",
		fmt.Sprintf(retrievalDocFmt, 1), "retrieved doc B")

	tests := []struct {
		name  string
		spans []*tracepb.Span
		want  []string
	}{
		{"explicit attributes win", []*tracepb.Span{
			root(fmt.Sprintf(AttrEvidenceFmt, 0), "explicit one", fmt.Sprintf(AttrEvidenceFmt, 1), "explicit two"), tool, retriever,
		}, []string{"explicit one", "explicit two"}},
		{"retriever documents beat tool output", []*tracepb.Span{root(), tool, retriever}, []string{"retrieved doc A", "retrieved doc B"}},
		{"tool output paragraphs", []*tracepb.Span{root(), tool}, []string{"tool paragraph one", "tool paragraph two"}},
		{"no evidence", []*tracepb.Span{root()}, nil},
		{"failed tool output is not evidence", []*tracepb.Span{root(), func() *tracepb.Span {
			s := proto.Clone(tool).(*tracepb.Span)
			s.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}
			return s
		}()}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := assemble(t, tt.spans)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Evidence, tt.want) {
				t.Errorf("evidence = %q, want %q", got.Evidence, tt.want)
			}
		})
	}
}

func TestAssembleToolCallDetails(t *testing.T) {
	failed := span(2, 1, "execute_tool search", 200, attrSpanKind, kindTool, attrToolName, "search", attrInputValue, `{"query": "x"}`)
	failed.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "boom"}
	spans := []*tracepb.Span{
		span(1, 0, "run", 100, attrSpanKind, kindAgent, attrInputValue, "q", attrOutputValue, "a"),
		failed,
		// Explicit status beats the span status.
		span(3, 1, "lookup", 300, attrSpanKind, kindTool, attrToolName, "lookup", attrInputValue, `{"title": "T"}`, AttrToolStatus, "not_found"),
		// No tool.name: the span name is used. Input that is not JSON is
		// passed as a string.
		span(4, 1, "calculator", 400, attrSpanKind, kindTool, attrInputValue, "2 + 2"),
		// Only tool.parameters.
		span(5, 1, "fetch", 500, attrSpanKind, kindTool, attrToolName, "fetch", attrToolParameters, `{"url": "u"}`),
		// No input at all.
		span(6, 1, "now", 600, attrSpanKind, kindTool, attrToolName, "now"),
	}
	got, err := assemble(t, spans)
	if err != nil {
		t.Fatal(err)
	}
	want := "USER QUESTION:\nq\n\nTOOL CALLS:\n" +
		"1. search({'query': 'x'}) -> error\n" +
		"2. lookup({'title': 'T'}) -> not_found\n" +
		"3. calculator('2 + 2') -> ok\n" +
		"4. fetch({'url': 'u'}) -> ok\n" +
		"5. now('') -> ok\n\n" +
		"EVIDENCE:\n(none)\n\nFINAL ANSWER:\na"
	if state := string(trace.BuildState(got)); state != want {
		t.Errorf("state =\n%s\nwant\n%s", state, want)
	}
}

func TestAssembleQuestionAndAnswerFromJSON(t *testing.T) {
	tests := []struct{ input, output, wantQ, wantA string }{
		{"plain question", "plain answer", "plain question", "plain answer"},
		{`{"question": "What?"}`, `{"answer": "This."}`, "What?", "This."},
		{`{"input": "What?", "session": 3}`, `{"output": "This."}`, "What?", "This."},
		{`"quoted question"`, `"quoted answer"`, "quoted question", "quoted answer"},
		// JSON without a recognised key is passed through whole.
		{`{"messages": []}`, `{"choices": []}`, `{"messages": []}`, `{"choices": []}`},
		{"{not json", "[1, 2]", "{not json", "[1, 2]"},
	}
	for _, tt := range tests {
		got, err := assemble(t, []*tracepb.Span{
			span(1, 0, "run", 100, attrSpanKind, kindAgent, attrInputValue, tt.input, attrOutputValue, tt.output),
		})
		if err != nil {
			t.Errorf("input %q: %v", tt.input, err)
			continue
		}
		if got.Question != tt.wantQ || got.FinalAnswer != tt.wantA {
			t.Errorf("input %q output %q: got %q / %q, want %q / %q", tt.input, tt.output, got.Question, got.FinalAnswer, tt.wantQ, tt.wantA)
		}
	}
}

// A run that gave up still has an output.value, an empty one. It is scored
// like any other trace instead of being discarded.
func TestAssembleEmptyAnswer(t *testing.T) {
	adkEmpty := `{"content": {"parts": [{"text": ""}], "role": "model"}, "author": "agent"}`
	for _, output := range []string{"", "  ", `""`, adkEmpty} {
		got, err := assemble(t, []*tracepb.Span{
			span(1, 0, "run", 100, attrSpanKind, kindAgent, attrInputValue, "q", attrOutputValue, output),
		})
		if err != nil {
			t.Errorf("output %q: %v", output, err)
			continue
		}
		if got.Question != "q" || got.FinalAnswer != "" {
			t.Errorf("output %q: got %q / %q, want an empty answer", output, got.Question, got.FinalAnswer)
		}
	}
}

// The root span of a Google ADK run, as its OpenInference instrumentor
// records it when the agent adds no span of its own.
func TestAssembleADKInvocation(t *testing.T) {
	input := `{"user_id": "u", "session_id": "s", "new_message": {"parts": [{"text": "Which river flows through Paris?"}], "role": "user"}, "run_config": {"max_llm_calls": 10}}`
	output := `{"content": {"parts": [{"text": "thinking", "thought": true}, {"text": "The Seine"}], "role": "model"}, "author": "agent", "actions": {"state_delta": {}}}`
	got, err := assemble(t, []*tracepb.Span{
		span(1, 0, "invocation [agent]", 100, attrSpanKind, kindChain, attrInputValue, input, attrOutputValue, output),
		span(2, 1, "agent_run [agent]", 110, attrSpanKind, kindAgent, attrOutputValue, output),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Question != "Which river flows through Paris?" || got.FinalAnswer != "The Seine" {
		t.Errorf("got %q / %q", got.Question, got.FinalAnswer)
	}
}

func TestAssembleNotScorable(t *testing.T) {
	tests := []struct {
		name    string
		spans   []*tracepb.Span
		wantErr string
	}{
		{"no question", []*tracepb.Span{span(1, 0, "GET /users", 100, "http.method", "GET")}, "has no input.value"},
		{"no answer", []*tracepb.Span{span(1, 0, "run", 100, attrSpanKind, kindAgent, attrInputValue, "q")}, "has no output.value"},
		// Only children arrived: the best available root is a tool span.
		{"root span missing", []*tracepb.Span{span(2, 1, "search", 100, attrSpanKind, kindTool, attrInputValue, `{"q": 1}`)}, "has no output.value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := assemble(t, tt.spans)
			if !errors.Is(err, ErrNotScorable) {
				t.Fatalf("error = %v, want ErrNotScorable", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
	if _, err := Assemble(testTraceHex, nil); !errors.Is(err, ErrNotScorable) {
		t.Errorf("no payloads: error = %v, want ErrNotScorable", err)
	}
	if _, err := Assemble(testTraceHex, [][]byte{{0xff, 0xff}}); err == nil || errors.Is(err, ErrNotScorable) {
		t.Errorf("corrupt payload: error = %v, want a decode error", err)
	}
}

// Spans arriving in several batches, some of them twice, are merged into one
// trace with each span once.
func TestAssembleMergesBatchesAndDropsDuplicates(t *testing.T) {
	all := adkRun()
	children, root := all[1:], all[:1]
	got, err := assemble(t, children, root, children) // children re-sent by a retrying exporter
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ToolCalls) != 2 {
		t.Errorf("%d tool calls, want 2: duplicates must not be counted twice", len(got.ToolCalls))
	}

	var td tracepb.TracesData
	if err := proto.Unmarshal(got.OTLP, &td); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, rs := range td.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			n += len(ss.Spans)
		}
	}
	if n != len(all) {
		t.Errorf("stored spans = %d, want %d", n, len(all))
	}
}

func TestSplitGroupsByTrace(t *testing.T) {
	other := []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	a1 := span(1, 0, "a1", 1)
	a2 := span(2, 1, "a2", 2)
	b1 := span(3, 0, "b1", 3)
	b1.TraceId = other
	noTrace := span(4, 0, "bad", 4)
	noTrace.TraceId = nil
	zeroSpan := span(5, 0, "bad", 5)
	zeroSpan.SpanId = make([]byte, 8)

	byTrace, skipped, err := Split([]*tracepb.ResourceSpans{
		resourceSpans("svc-1", a1, b1, noTrace),
		resourceSpans("svc-2", a2, zeroSpan),
	})
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 2 {
		t.Errorf("skipped = %d, want 2", skipped)
	}
	if len(byTrace) != 2 {
		t.Fatalf("%d traces, want 2", len(byTrace))
	}

	var a tracepb.TracesData
	if err := proto.Unmarshal(byTrace[testTraceHex], &a); err != nil {
		t.Fatal(err)
	}
	// Trace A has a span from each service, each under its own resource.
	if len(a.ResourceSpans) != 2 {
		t.Fatalf("trace A has %d resource groups, want 2", len(a.ResourceSpans))
	}
	services := []string{}
	for _, rs := range a.ResourceSpans {
		services = append(services, rs.Resource.Attributes[0].Value.GetStringValue())
		if len(rs.ScopeSpans) != 1 || len(rs.ScopeSpans[0].Spans) != 1 || rs.ScopeSpans[0].Scope.GetName() != "test" {
			t.Errorf("unexpected scope spans in trace A: %v", rs.ScopeSpans)
		}
	}
	if !reflect.DeepEqual(services, []string{"svc-1", "svc-2"}) {
		t.Errorf("services = %v", services)
	}

	var b tracepb.TracesData
	if err := proto.Unmarshal(byTrace[hex.EncodeToString(other)], &b); err != nil {
		t.Fatal(err)
	}
	if len(b.ResourceSpans) != 1 || b.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "b1" {
		t.Errorf("trace B = %v", b.ResourceSpans)
	}
}

func TestAnnotate(t *testing.T) {
	assembled, err := assemble(t, adkRun())
	if err != nil {
		t.Fatal(err)
	}
	rs, err := Annotate(assembled.OTLP, Annotation{
		Resource: []*commonpb.KeyValue{StringAttr(attrProjectName, "observer-ai")},
		Root: []*commonpb.KeyValue{
			StringAttr("eval.decision", "keep"),
			FloatAttr("eval.p_keep", 0.91),
			StringAttr("observer.stop_reason", "overwritten"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var root *tracepb.Span
	total := 0
	for _, r := range rs {
		project := ""
		for _, kv := range r.Resource.Attributes {
			if kv.Key == attrProjectName {
				project = kv.Value.GetStringValue()
			}
		}
		if project != "observer-ai" {
			t.Errorf("resource project = %q, want it replaced with observer-ai", project)
		}
		if n := len(r.Resource.Attributes); n != 2 {
			t.Errorf("resource has %d attributes, want the original 2", n)
		}
		for _, ss := range r.ScopeSpans {
			for _, s := range ss.Spans {
				total++
				if s.Name == "observer_agent.run" {
					root = s
				}
			}
		}
	}
	if total != 6 {
		t.Errorf("forwarding %d spans, want all 6", total)
	}
	if root == nil {
		t.Fatal("root span missing")
	}
	// IDs are untouched, so the trace keeps its identity in Phoenix.
	if hex.EncodeToString(root.TraceId) != testTraceHex {
		t.Errorf("trace ID changed to %x", root.TraceId)
	}
	if attr(root, "eval.decision") != "keep" || attr(root, "eval.p_keep") != "0.91" {
		t.Errorf("eval attributes missing from root: %v", root.Attributes)
	}
	if attr(root, "observer.stop_reason") != "overwritten" {
		t.Errorf("existing attribute not replaced: %q", attr(root, "observer.stop_reason"))
	}
	if attr(root, attrInputValue) == "" {
		t.Error("original attributes lost")
	}
	count := 0
	for _, kv := range root.Attributes {
		if kv.Key == "observer.stop_reason" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("observer.stop_reason appears %d times, want 1", count)
	}
}
