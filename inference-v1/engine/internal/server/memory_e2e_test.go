package server

import (
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/observer-ai/engine/internal/queue"
)

// memoryBroker is the in-process broker main uses when queue.url is empty.
func memoryBroker(t *testing.T) *queue.MemoryBroker {
	t.Helper()
	b := queue.NewMemory(queue.Config{
		MaxMessages:       100,
		MaxDeliver:        2,
		RedeliveryBackoff: []time.Duration{20 * time.Millisecond},
		Spans:             queue.SpanConfig{IdleTimeout: 300 * time.Millisecond, MaxWait: 10 * time.Second},
		Logger:            quiet,
	})
	t.Cleanup(b.Close)
	return b
}

// The whole OTLP path without NATS: spans over gRPC and HTTP, assembled in
// memory, scored, and the kept trace forwarded as its original spans.
func TestMemoryOTLPPipelineKeepsAndDrops(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, state string) {
		if strings.Contains(state, "Lyon") {
			w.Write(readFile(t, "kev", "success.json")) // 0.93: keep
			return
		}
		w.Write(readFile(t, "kev", "success_low.json")) // 0.12: drop
	})
	p := newOTLPPipelineOn(t, kev, memoryBroker(t))

	wrong := fixtureSpans(t, "wrong_river.json", wrongID)
	p.exportGRPC(t, wrong[1:]...)
	p.exportHTTP(t, wrong[0])
	p.exportGRPC(t, fixtureSpans(t, "correct_river.json", correctID)...)

	waitUntil(t, "both traces to be scored", func() bool { return kev.calls.Load() == 2 })
	waitUntil(t, "the kept trace to be forwarded", func() bool { return p.phoenix.count() == 1 })
	time.Sleep(100 * time.Millisecond) // nothing else follows

	p.phoenix.mu.Lock()
	defer p.phoenix.mu.Unlock()
	if len(p.phoenix.uploads) != 1 {
		t.Fatalf("%d traces forwarded, want only the kept one", len(p.phoenix.uploads))
	}
	for _, rs := range p.phoenix.uploads[0] {
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				if hex.EncodeToString(s.TraceId) != hex.EncodeToString(wrongID) {
					t.Errorf("forwarded span %q has trace ID %x, want %x", s.Name, s.TraceId, wrongID)
				}
			}
		}
	}
}

// The JSON API through the in-process queue: accepted, scored by a worker,
// and the kept trace exported.
func TestMemoryQueuedPipeline(t *testing.T) {
	kev := newFakeKev(t, func(w http.ResponseWriter, _ string) {
		w.Write(readFile(t, "kev", "success.json"))
	})
	p := newOTLPPipelineOn(t, kev, memoryBroker(t))

	if w := p.do("POST", "/v1/traces", string(readFile(t, "traces", "wrong_river.json"))); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	waitUntil(t, "the trace to be scored", func() bool { return kev.calls.Load() == 1 })
	waitUntil(t, "the kept trace to be exported", func() bool { return len(p.recorder.Ended()) == 3 })
	if a := rootAttrs(t, p.recorder); a["eval.decision"].AsString() != "keep" {
		t.Errorf("eval.decision = %v", a["eval.decision"].Emit())
	}
}
