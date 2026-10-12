package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/queue/queuetest"
	"github.com/observer-ai/engine/internal/scorer"
	"github.com/observer-ai/engine/internal/trace"
)

// capture records what would be sent to Phoenix.
type capture struct {
	mu      sync.Mutex
	uploads [][]*tracepb.ResourceSpans
}

func (c *capture) UploadTraces(_ context.Context, spans []*tracepb.ResourceSpans) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploads = append(c.uploads, spans)
	return nil
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.uploads)
}

// otlpPipeline is the whole OTLP path, wired as in main: gRPC and HTTP
// receivers, span storage and assembly in JetStream, the worker pool, a Kev
// client against a fake Kev, and the exporter forwarding to a capture.
type otlpPipeline struct {
	*harness
	js       jetstream.JetStream
	phoenix  *capture
	grpcAddr string
	httpAddr string
}

func newOTLPPipeline(t *testing.T, kev *fakeKev) *otlpPipeline {
	t.Helper()
	srv := queuetest.StartServer(t)
	cfg := queue.Config{
		URL: srv.ClientURL(), ConnectTimeout: 5 * time.Second,
		Stream: "TRACES", Subject: "traces.in", Consumer: "workers", MaxMessages: 100,
		AckWait: 30 * time.Second, MaxDeliver: 2, RedeliveryBackoff: []time.Duration{20 * time.Millisecond},
		DLQStream: "TRACES_DLQ", DLQSubject: "traces.dlq", DLQMaxAge: time.Hour,
		Spans: queue.SpanConfig{
			Stream: "SPANS", Subject: "spans", MaxAge: time.Hour,
			PendingStream: "PENDING", PendingSubject: "pending",
			IdleTimeout: 300 * time.Millisecond, MaxWait: 10 * time.Second,
		},
		PollWait: 100 * time.Millisecond,
		Logger:   quiet,
	}
	q, err := queue.Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)

	phoenix := &capture{}
	h := newHarness(t, harnessOpts{scorer: kev.client(t, 0), queue: q, temp: 0.86, uploader: phoenix})

	free := func() string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()
		return lis.Addr().String()
	}
	grpcAddr, httpAddr := free(), free()
	receiver := otlp.NewReceiver(q, quiet)
	if err := receiver.Start(grpcAddr, httpAddr); err != nil {
		t.Fatal(err)
	}
	assemblers := q.StartAssemblers(2, otlp.Assemble)
	workers := q.StartWorkers(2, func(ctx context.Context, tr trace.Trace) error {
		_, err := h.engine.ProcessQueued(ctx, tr)
		return err
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		receiver.Shutdown(ctx)
		assemblers.Stop(ctx)
		workers.Stop(ctx)
	})

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	return &otlpPipeline{harness: h, js: js, phoenix: phoenix, grpcAddr: grpcAddr, httpAddr: httpAddr}
}

func (p *otlpPipeline) messages(t *testing.T, stream string) uint64 {
	t.Helper()
	s, err := p.js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func (p *otlpPipeline) exportGRPC(t *testing.T, spans ...*tracepb.Span) {
	t.Helper()
	conn, err := grpc.NewClient(p.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := coltracepb.NewTraceServiceClient(conn).Export(ctx, exportOf(spans)); err != nil {
		t.Fatalf("gRPC export: %v", err)
	}
}

func (p *otlpPipeline) exportHTTP(t *testing.T, spans ...*tracepb.Span) {
	t.Helper()
	body, _ := proto.Marshal(exportOf(spans))
	resp, err := http.Post("http://"+p.httpAddr+otlp.HTTPPath, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP export: status %d", resp.StatusCode)
	}
}

func exportOf(spans []*tracepb.Span) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			otlp.StringAttr("service.name", "observer_agent"),
			otlp.StringAttr("openinference.project.name", "observer_agent"),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
}

// fixtureSpans renders a JSON fixture as the spans an agent would export.
func fixtureSpans(t *testing.T, name string, traceID []byte) []*tracepb.Span {
	t.Helper()
	tr, err := trace.Decode(bytes.NewReader(readFile(t, "traces", name)))
	if err != nil {
		t.Fatal(err)
	}
	return otlp.SpansFromTrace(traceID, tr)
}

var (
	wrongID   = []byte{0x11, 0x11, 0x11, 0x11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	correctID = []byte{0x22, 0x22, 0x22, 0x22, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
)

func TestOTLPPipelineKeepForwardsOriginalSpans(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, state string) {
		if strings.Contains(state, "Lyon") {
			w.Write(readFile(t, "kev", "success.json")) // 0.93: keep
			return
		}
		w.Write(readFile(t, "kev", "success_low.json")) // 0.12: drop
	})
	p := newOTLPPipeline(t, kev)

	// The kept trace arrives the way a batching exporter sends it: tool
	// spans first over gRPC, the root span later over HTTP.
	wrong := fixtureSpans(t, "wrong_river.json", wrongID)
	p.exportGRPC(t, wrong[1:]...)
	time.Sleep(100 * time.Millisecond)
	p.exportHTTP(t, wrong[0])
	// The dropped trace arrives in one batch.
	p.exportGRPC(t, fixtureSpans(t, "correct_river.json", correctID)...)

	waitUntil(t, "both traces to be scored", func() bool { return kev.calls.Load() == 2 })
	waitUntil(t, "the kept trace to be forwarded", func() bool { return p.phoenix.count() == 1 })
	waitUntil(t, "the queues to empty", func() bool {
		return p.messages(t, "TRACES") == 0 && p.messages(t, "SPANS") == 0 && p.messages(t, "PENDING") == 0
	})

	// Kev saw exactly the slice the JSON API produces for the same trace,
	// built from spans that arrived in two batches over two protocols.
	want := string(readFile(t, "golden", "wrong_river.txt"))
	kev.mu.Lock()
	found := false
	for _, s := range kev.states {
		found = found || s == want
	}
	kev.mu.Unlock()
	if !found {
		t.Errorf("no state sent to Kev matches the golden slice: %q", kev.states)
	}

	// Phoenix gets the original spans under the original trace ID, with
	// the decision on the root span. The dropped trace goes nowhere.
	p.phoenix.mu.Lock()
	defer p.phoenix.mu.Unlock()
	if len(p.phoenix.uploads) != 1 {
		t.Fatalf("%d traces forwarded, want only the kept one", len(p.phoenix.uploads))
	}
	spans := 0
	var root *tracepb.Span
	for _, rs := range p.phoenix.uploads[0] {
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				spans++
				if hex.EncodeToString(s.TraceId) != hex.EncodeToString(wrongID) {
					t.Errorf("forwarded span %q has trace ID %x, want %x", s.Name, s.TraceId, wrongID)
				}
				if len(s.ParentSpanId) == 0 {
					root = s
				}
			}
		}
	}
	if spans != len(wrong) {
		t.Errorf("forwarded %d spans, want %d", spans, len(wrong))
	}
	if root == nil {
		t.Fatal("no root span forwarded")
	}
	attrs := map[string]*commonpb.AnyValue{}
	for _, kv := range root.Attributes {
		attrs[kv.Key] = kv.Value
	}
	if attrs["eval.decision"].GetStringValue() != "keep" || attrs["eval.reason"].GetStringValue() != "kev" ||
		attrs["eval.severity"].GetStringValue() != "high" {
		t.Errorf("eval attributes on the root = %v", attrs)
	}
	if got := attrs["eval.p_keep"].GetDoubleValue(); got != scorer.Calibrate(0.93, 0.86) {
		t.Errorf("eval.p_keep = %v", got)
	}
	// Nothing was rebuilt through the SDK exporter.
	if n := len(p.recorder.Started()); n != 0 {
		t.Errorf("%d spans rebuilt, want 0", n)
	}
}

// Spans that are not an agent run (no question and answer) are discarded
// before they cost a Kev call.
func TestOTLPPipelineIgnoresNonAgentSpans(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) { w.Write(readFile(t, "kev", "success.json")) })
	p := newOTLPPipeline(t, kev)

	p.exportGRPC(t, &tracepb.Span{
		TraceId: correctID, SpanId: []byte{0, 0, 0, 0, 0, 0, 0, 9}, Name: "GET /healthz",
		Attributes: []*commonpb.KeyValue{otlp.StringAttr("http.method", "GET")},
	})
	waitUntil(t, "the spans to be discarded", func() bool {
		return p.messages(t, "SPANS") == 0 && p.messages(t, "PENDING") == 0
	})
	if kev.calls.Load() != 0 || p.phoenix.count() != 0 || p.messages(t, "TRACES") != 0 || p.messages(t, "TRACES_DLQ") != 0 {
		t.Errorf("non-agent spans went further than assembly: kev=%d phoenix=%d queued=%d dlq=%d",
			kev.calls.Load(), p.phoenix.count(), p.messages(t, "TRACES"), p.messages(t, "TRACES_DLQ"))
	}
}

// A system error on an OTLP trace dead-letters it with its spans, so a
// replay can still forward the originals.
func TestOTLPPipelineDeadLetterKeepsSpans(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		http.Error(w, "model not available", http.StatusServiceUnavailable)
	})
	p := newOTLPPipeline(t, kev)
	p.exportHTTP(t, fixtureSpans(t, "wrong_river.json", wrongID)...)

	waitUntil(t, "the dead letter", func() bool { return p.messages(t, "TRACES_DLQ") == 1 })
	if p.phoenix.count() != 0 {
		t.Error("an unscored trace was forwarded")
	}

	s, err := p.js.Stream(context.Background(), "TRACES_DLQ")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := s.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := dead.Header.Get(queue.HeaderTraceID); got != hex.EncodeToString(wrongID) {
		t.Errorf("dead letter trace ID = %q", got)
	}
	var replay trace.Trace
	if err := json.Unmarshal(dead.Data, &replay); err != nil {
		t.Fatal(err)
	}
	if len(replay.OTLP) == 0 {
		t.Fatal("dead letter lost the original spans")
	}
	var td tracepb.TracesData
	if err := proto.Unmarshal(replay.OTLP, &td); err != nil {
		t.Fatalf("dead letter spans do not decode: %v", err)
	}
	if got, want := trace.BuildState(replay), string(readFile(t, "golden", "wrong_river.txt")); got != want {
		t.Errorf("replayed dead letter builds a different slice")
	}
}

// The JSON API must not let a client smuggle in spans to be forwarded.
func TestJSONAPIIgnoresOTLPField(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	w := h.do("POST", "/v1/traces", `{"question": "q", "final_answer": "a", "otlp_spans": "AAEC"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d", w.Code)
	}
	if got := h.queue.published[0].OTLP; got != nil {
		t.Errorf("published trace carries client-supplied spans: %v", got)
	}
}
