package export

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
	"github.com/observer-ai/engine/pkg/trace"
)

var testStreams = NATSStreams{
	Stream:     "EXPORTS",
	Subject:    "export",
	Consumer:   "observer",
	DLQStream:  "EXPORTS_DLQ",
	DLQSubject: "export_dlq",
	DLQMaxAge:  time.Hour,
	PollWait:   100 * time.Millisecond,
}

func natsQueues(t *testing.T) (NATSQueueFactory, jetstream.JetStream) {
	t.Helper()
	srv := queuetest.StartServer(t)
	conn, err := common.DialNATS(common.NATSConfig{URL: srv.ClientURL()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return NATSQueueFactory{Conn: conn, Streams: testStreams}, conn.JS
}

func natsDispatcher(t *testing.T, queues NATSQueueFactory, factory fakeFactory, reporter *recordingReporter, backends ...BackendConfig) *Dispatcher {
	t.Helper()
	cfg := DispatcherConfig{Backends: backends, Factory: factory, Queues: queues}
	if reporter != nil {
		cfg.Reporter = reporter
	}
	d, err := NewDispatcher(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	return d
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

func TestNATSQueueSendsToEveryBackend(t *testing.T) {
	queues, js := natsQueues(t)
	a, b := &fakeBackend{}, &fakeBackend{}
	d := natsDispatcher(t, queues, fakeFactory{"phoenix": a, "lang-fuse": b}, nil,
		BackendConfig{Name: "phoenix"}, BackendConfig{Name: "lang-fuse"})

	for _, id := range []string{"t1", "t2"} {
		if !d.Export(context.Background(), kept(id)) {
			t.Fatalf("Export(%s) returned false", id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for name, backend := range map[string]*fakeBackend{"phoenix": a, "lang-fuse": b} {
		if sent, flushed, _ := backend.state(); len(sent) != 2 || !flushed {
			t.Errorf("%s: sent %v, flushed %v", name, sent, flushed)
		}
	}
	// One work-queue stream per back end, emptied once sent.
	for _, stream := range []string{"EXPORTS_PHOENIX", "EXPORTS_LANG_FUSE"} {
		if n := streamMsgs(t, js, stream); n != 0 {
			t.Errorf("%s holds %d messages after flush", stream, n)
		}
	}
}

// What the back end receives is the record that was queued.
func TestNATSQueueKeepsTheRecord(t *testing.T) {
	queues, _ := natsQueues(t)
	got := make(chan pipeline.Record, 1)
	q, err := queues.Queue(context.Background(), QueueConfig{Name: "a", Send: func(_ context.Context, rec pipeline.Record) error {
		got <- rec
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Shutdown(context.Background())

	want := pipeline.Record{
		Trace:    trace.Trace{TraceID: "t1", Question: "q", FinalAnswer: "a", OTLP: []byte{1, 2, 3}},
		Decision: keepHigh,
		Model:    "kev-latest",
		Latency:  93 * time.Millisecond,
	}
	if err := q.Enqueue(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-got:
		if rec.Trace.TraceID != "t1" || string(rec.Trace.OTLP) != "\x01\x02\x03" || rec.Decision != want.Decision ||
			rec.Model != want.Model || rec.Latency != want.Latency {
			t.Errorf("received %+v, want %+v", rec, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("record not sent")
	}
}

func TestNATSQueueDeadLettersAfterMaxDeliver(t *testing.T) {
	queues, js := natsQueues(t)
	failing := &fakeBackend{err: fault.Unavailable(fault.Export, "forward spans", errors.New("connection refused"))}
	reported := &recordingReporter{}
	d := natsDispatcher(t, queues, fakeFactory{"a": failing}, reported,
		BackendConfig{Name: "a", MaxDeliver: 2, Backoff: []time.Duration{10 * time.Millisecond}})

	d.Export(context.Background(), kept("t1"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if sent, _, _ := failing.state(); len(sent) != 2 {
		t.Errorf("tried %d times, want max_deliver (2)", len(sent))
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
	errs := reported.all()
	last, _ := fault.As(errs[len(errs)-1])
	if last.Op != "export trace, dead-lettered" || !last.Severe {
		t.Errorf("last report = %+v", last)
	}
}

// A full stream refuses the record for that back end only.
func TestNATSQueueFull(t *testing.T) {
	queues, _ := natsQueues(t)
	stuck := &fakeBackend{gate: make(chan struct{}), started: make(chan struct{}, 1)}
	reported := &recordingReporter{}
	d := natsDispatcher(t, queues, fakeFactory{"stuck": stuck}, reported,
		BackendConfig{Name: "stuck", QueueSize: 1, Concurrency: 1})
	defer close(stuck.gate)

	d.Export(context.Background(), kept("t1")) // held by the worker, still in the stream
	<-stuck.started
	if d.Export(context.Background(), kept("t2")) {
		t.Error("Export returned true with the only back end's stream full")
	}
	if errs := reported.all(); len(errs) != 1 || fault.KindOf(errs[0]) != fault.KindFull {
		t.Errorf("reported %v, want one full fault", errs)
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
