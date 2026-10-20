package natsq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
	"github.com/observer-ai/engine/pkg/trace"
)

// What every implementation must do is in export's conformance suite. These
// tests cover what only JetStream does: a stream per back end, a dead-letter
// stream, and how records are stored.

var keepHigh = pipeline.Decision{Keep: true, Scored: true, PKeep: 0.95, PKeepRaw: 0.93, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}

func factory(t *testing.T) (Factory, jetstream.JetStream) {
	t.Helper()
	srv := queuetest.StartServer(t)
	conn, err := common.DialNATS(common.NATSConfig{URL: srv.ClientURL()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return Factory{Conn: conn, Streams: Streams{
		Stream: "EXPORTS", Subject: "export", Consumer: "observer",
		DLQStream: "EXPORTS_DLQ", DLQSubject: "export_dlq", DLQMaxAge: time.Hour,
		PollWait: 100 * time.Millisecond,
	}}, conn.JS
}

func open(t *testing.T, f Factory, cfg export.QueueConfig) export.AbstractQueue {
	t.Helper()
	q, err := f.Queue(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Shutdown(context.Background()) })
	return q
}

func streamMsgs(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	s, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

type faults struct {
	mu   sync.Mutex
	errs []*fault.Error
}

func (f *faults) Report(_ context.Context, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fe, _ := fault.As(err)
	f.errs = append(f.errs, fe)
}

// Each back end gets its own stream, named after it.
func TestStreamPerBackend(t *testing.T) {
	f, js := factory(t)
	ok := func(context.Context, pipeline.Record) error { return nil }
	for _, name := range []string{"phoenix", "lang-fuse"} {
		q := open(t, f, export.QueueConfig{Name: name, Send: ok})
		if err := q.Enqueue(context.Background(), pipeline.Record{Trace: trace.Trace{TraceID: "t1"}, Decision: keepHigh}); err != nil {
			t.Fatal(err)
		}
		if err := q.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, stream := range []string{"EXPORTS_PHOENIX", "EXPORTS_LANG_FUSE"} {
		if n := streamMsgs(t, js, stream); n != 0 {
			t.Errorf("%s holds %d messages after flush, want it to exist and be empty", stream, n)
		}
	}
}

// What the back end receives is the record that was queued.
func TestRecordSurvivesTheStream(t *testing.T) {
	f, _ := factory(t)
	got := make(chan pipeline.Record, 1)
	q := open(t, f, export.QueueConfig{Name: "a", Send: func(_ context.Context, rec pipeline.Record) error {
		got <- rec
		return nil
	}})
	want := pipeline.Record{
		Trace:    trace.Trace{TraceID: "t1", Agent: "support-bot", Question: "q", FinalAnswer: "a", OTLP: []byte{1, 2, 3}},
		Agent:    "support-bot",
		Decision: keepHigh,
		Model:    "kev-latest",
		Latency:  93 * time.Millisecond,
	}
	if err := q.Enqueue(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-got:
		if rec.Trace.TraceID != "t1" || rec.Trace.Agent != "support-bot" || rec.Agent != "support-bot" || string(rec.Trace.OTLP) != "\x01\x02\x03" ||
			rec.Decision != want.Decision || rec.Model != want.Model || rec.Latency != want.Latency {
			t.Errorf("received %+v, want %+v", rec, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("record not sent")
	}
}

// A record given up on goes to the dead-letter stream, where it can be
// replayed.
func TestGivenUpRecordIsDeadLettered(t *testing.T) {
	f, js := factory(t)
	reported := &faults{}
	q := open(t, f, export.QueueConfig{
		Name: "a", MaxDeliver: 2, Backoff: []time.Duration{10 * time.Millisecond}, Reporter: reported,
		Send: func(context.Context, pipeline.Record) error {
			return fault.Unavailable(fault.Export, "forward spans", errors.New("connection refused"))
		},
	})
	if err := q.Enqueue(context.Background(), pipeline.Record{Trace: trace.Trace{TraceID: "t1"}, Decision: keepHigh}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := streamMsgs(t, js, "EXPORTS_DLQ"); n != 1 {
		t.Fatalf("%d dead letters, want 1", n)
	}
	s, _ := js.Stream(context.Background(), "EXPORTS_DLQ")
	dead, err := s.GetLastMsgForSubject(context.Background(), "export_dlq")
	if err != nil {
		t.Fatal(err)
	}
	if dead.Header.Get(headerExporter) != "a" || dead.Header.Get(headerTraceID) != "t1" || dead.Header.Get(headerDeliveries) != "2" {
		t.Errorf("dead-letter headers = %v", dead.Header)
	}
	if rec, err := decodeRecord(dead.Data); err != nil || rec.Trace.TraceID != "t1" {
		t.Errorf("dead letter does not replay: %v %+v", err, rec)
	}
	reported.mu.Lock()
	defer reported.mu.Unlock()
	if last := reported.errs[len(reported.errs)-1]; last.Op != "export trace, dead-lettered" || !last.Severe {
		t.Errorf("last report = %+v", last)
	}
}

func TestEncodeRecordDropsSpansWhenTooBig(t *testing.T) {
	rec := pipeline.Record{Trace: trace.Trace{TraceID: "t1", Question: "q", OTLP: make([]byte, 4096)}, Decision: keepHigh}
	data, err := encodeRecord(rec, 1024)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Trace.OTLP) != 0 || got.Decision != keepHigh {
		t.Errorf("decoded %+v, want the decision without the spans", got)
	}
}
