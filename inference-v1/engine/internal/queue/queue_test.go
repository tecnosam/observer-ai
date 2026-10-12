package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/queue"
	"github.com/observer-ai/engine/internal/queue/queuetest"
	"github.com/observer-ai/engine/internal/trace"
)

const waitFor = 10 * time.Second

func testConfig(url string) queue.Config {
	return queue.Config{
		URL:               url,
		ConnectTimeout:    5 * time.Second,
		Stream:            "TRACES",
		Subject:           "traces.in",
		Consumer:          "workers",
		MaxMessages:       1000,
		AckWait:           30 * time.Second,
		MaxDeliver:        3,
		RedeliveryBackoff: []time.Duration{20 * time.Millisecond},
		DLQStream:         "TRACES_DLQ",
		DLQSubject:        "traces.dlq",
		DLQMaxAge:         time.Hour,
		PollWait:          100 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

type fixture struct {
	srv *server.Server
	q   *queue.Queue
	js  jetstream.JetStream // a separate connection, for inspecting streams
	cfg queue.Config
}

func setup(t *testing.T, mutate func(*queue.Config)) *fixture {
	t.Helper()
	srv := queuetest.StartServer(t)
	cfg := testConfig(srv.ClientURL())
	if mutate != nil {
		mutate(&cfg)
	}
	q, err := queue.Connect(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(q.Close)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{srv: srv, q: q, js: js, cfg: cfg}
}

// start runs workers and stops them when the test ends.
func (f *fixture) start(t *testing.T, n int, h queue.Handler) *queue.Workers {
	t.Helper()
	w := f.q.StartWorkers(n, h)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitFor)
		defer cancel()
		w.Stop(ctx)
	})
	return w
}

func (f *fixture) messages(t *testing.T, stream string) uint64 {
	t.Helper()
	s, err := f.js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// deadLetters returns every message in the dead-letter stream.
func (f *fixture) deadLetters(t *testing.T) []*jetstream.RawStreamMsg {
	t.Helper()
	s, err := f.js.Stream(context.Background(), f.cfg.DLQStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []*jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := s.GetMsg(context.Background(), seq)
		if err != nil {
			t.Fatalf("dead letter %d: %v", seq, err)
		}
		out = append(out, m)
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func sample(id string) trace.Trace {
	return trace.Trace{TraceID: id, Question: "q " + id, FinalAnswer: "a"}
}

func mustPublish(t *testing.T, q *queue.Queue, tr trace.Trace) {
	t.Helper()
	if err := q.Publish(context.Background(), tr); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// A trace survives the queue unchanged: the slice built on the worker side
// is byte-for-byte the golden slice for every fixture.
func TestPublishedTraceBuildsTheSameState(t *testing.T) {
	f := setup(t, nil)
	paths, err := filepath.Glob("../../testdata/traces/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no trace fixtures: %v", err)
	}

	want := map[string]string{}
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := trace.Decode(file)
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		golden, err := os.ReadFile(filepath.Join("../../testdata/golden", name+".txt"))
		if err != nil {
			t.Fatal(err)
		}
		tr.TraceID = name
		want[name] = string(golden)
		mustPublish(t, f.q, tr)
	}

	var mu sync.Mutex
	got := map[string]string{}
	f.start(t, 3, func(_ context.Context, tr trace.Trace) error {
		mu.Lock()
		defer mu.Unlock()
		got[tr.TraceID] = trace.BuildState(tr)
		return nil
	})
	eventually(t, "all traces to be processed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == len(want)
	})
	for name, state := range want {
		if got[name] != state {
			t.Errorf("%s: state after the queue differs from the golden slice\n got %q\nwant %q", name, got[name], state)
		}
	}
	eventually(t, "the work queue to empty", func() bool { return f.messages(t, f.cfg.Stream) == 0 })
	if n := f.messages(t, f.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0", n)
	}
}

func TestSystemErrorIsRedeliveredThenSucceeds(t *testing.T) {
	f := setup(t, nil)
	mustPublish(t, f.q, sample("t1"))

	var calls atomic.Int32
	f.start(t, 2, func(context.Context, trace.Trace) error {
		if calls.Add(1) == 1 {
			return errors.New("kev returned HTTP 503")
		}
		return nil
	})
	eventually(t, "the trace to be acknowledged", func() bool { return f.messages(t, f.cfg.Stream) == 0 })
	if calls.Load() != 2 {
		t.Errorf("handler called %d times, want 2", calls.Load())
	}
	if n := f.messages(t, f.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0: the retry succeeded", n)
	}
}

func TestPersistentSystemErrorIsDeadLettered(t *testing.T) {
	f := setup(t, nil) // max_deliver 3
	tr := sample("doomed")
	tr.Evidence = []string{"é evidence"}
	mustPublish(t, f.q, tr)
	mustPublish(t, f.q, sample("fine"))

	var doomedCalls, fineCalls atomic.Int32
	f.start(t, 2, func(_ context.Context, got trace.Trace) error {
		if got.TraceID == "doomed" {
			doomedCalls.Add(1)
			return errors.New("scorer: giving up after 4 attempts:\nkev returned HTTP 503: model loading")
		}
		fineCalls.Add(1)
		return nil
	})

	eventually(t, "the trace to reach the dead-letter stream", func() bool { return f.messages(t, f.cfg.DLQStream) == 1 })
	eventually(t, "the work queue to empty", func() bool { return f.messages(t, f.cfg.Stream) == 0 })
	if doomedCalls.Load() != 3 {
		t.Errorf("failing trace delivered %d times, want max_deliver = 3", doomedCalls.Load())
	}
	if fineCalls.Load() != 1 {
		t.Errorf("healthy trace processed %d times, want 1", fineCalls.Load())
	}

	dead := f.deadLetters(t)[0]
	if dead.Subject != f.cfg.DLQSubject {
		t.Errorf("dead letter subject = %q, want %q", dead.Subject, f.cfg.DLQSubject)
	}
	wantHeaders := map[string]string{
		queue.HeaderTraceID:        "doomed",
		queue.HeaderDeadReason:     queue.ReasonSystemError,
		queue.HeaderDeliveries:     "3",
		queue.HeaderSourceStream:   "TRACES",
		queue.HeaderSourceSequence: "1",
		// Newlines are flattened so the error fits in a header.
		queue.HeaderDeadError: "scorer: giving up after 4 attempts: kev returned HTTP 503: model loading",
	}
	for k, want := range wantHeaders {
		if got := dead.Header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if _, err := time.Parse(time.RFC3339, dead.Header.Get(queue.HeaderDeadAt)); err != nil {
		t.Errorf("header %s = %q is not RFC 3339", queue.HeaderDeadAt, dead.Header.Get(queue.HeaderDeadAt))
	}

	// The body is the original trace, ready to be replayed.
	var replay trace.Trace
	if err := json.Unmarshal(dead.Data, &replay); err != nil {
		t.Fatalf("dead letter body is not a trace: %v", err)
	}
	if replay.TraceID != "doomed" || replay.Question != tr.Question || len(replay.Evidence) != 1 || replay.Evidence[0] != "é evidence" {
		t.Errorf("dead letter body = %+v, want the original trace", replay)
	}
}

func TestMaxDeliverOneDeadLettersOnFirstFailure(t *testing.T) {
	f := setup(t, func(c *queue.Config) { c.MaxDeliver = 1 })
	mustPublish(t, f.q, sample("t1"))
	var calls atomic.Int32
	f.start(t, 1, func(context.Context, trace.Trace) error {
		calls.Add(1)
		return errors.New("connection refused")
	})
	eventually(t, "the dead letter", func() bool { return f.messages(t, f.cfg.DLQStream) == 1 })
	if calls.Load() != 1 {
		t.Errorf("handler called %d times, want 1", calls.Load())
	}
}

// Something that is not a trace cannot be fixed by retrying: it goes to the
// dead-letter stream on first sight and the handler never sees it.
func TestInvalidPayloadIsDeadLetteredImmediately(t *testing.T) {
	f := setup(t, nil)
	if _, err := f.js.Publish(context.Background(), f.cfg.Subject, []byte(`{"question": "no answer"}`)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	f.start(t, 1, func(context.Context, trace.Trace) error {
		calls.Add(1)
		return nil
	})
	eventually(t, "the dead letter", func() bool { return f.messages(t, f.cfg.DLQStream) == 1 })
	eventually(t, "the work queue to empty", func() bool { return f.messages(t, f.cfg.Stream) == 0 })

	dead := f.deadLetters(t)[0]
	if got := dead.Header.Get(queue.HeaderDeadReason); got != queue.ReasonInvalidPayload {
		t.Errorf("reason = %q, want %q", got, queue.ReasonInvalidPayload)
	}
	if got := dead.Header.Get(queue.HeaderDeadError); !strings.Contains(got, "final_answer is required") {
		t.Errorf("error header = %q", got)
	}
	if got := dead.Header.Get(queue.HeaderDeliveries); got != "1" {
		t.Errorf("deliveries = %q, want 1", got)
	}
	if string(dead.Data) != `{"question": "no answer"}` {
		t.Errorf("body = %s, want the original bytes", dead.Data)
	}
	if calls.Load() != 0 {
		t.Errorf("handler called %d times for an invalid payload", calls.Load())
	}
}

func TestPublishWhenStreamIsFull(t *testing.T) {
	f := setup(t, func(c *queue.Config) { c.MaxMessages = 2 })
	mustPublish(t, f.q, sample("t1"))
	mustPublish(t, f.q, sample("t2"))

	err := f.q.Publish(context.Background(), sample("t3"))
	if !errors.Is(err, queue.ErrFull) {
		t.Fatalf("Publish to a full stream = %v, want ErrFull", err)
	}
	if n := f.messages(t, f.cfg.Stream); n != 2 {
		t.Errorf("stream holds %d messages, want the first 2", n)
	}

	// Once a worker makes room, publishing works again.
	f.start(t, 1, func(context.Context, trace.Trace) error { return nil })
	eventually(t, "room in the stream", func() bool { return f.messages(t, f.cfg.Stream) == 0 })
	mustPublish(t, f.q, sample("t4"))
}

func TestPublishAndReadyWhenNATSIsDown(t *testing.T) {
	f := setup(t, nil)
	if err := f.q.Ready(); err != nil {
		t.Fatalf("Ready with NATS up = %v", err)
	}

	f.srv.Shutdown()
	f.srv.WaitForShutdown()
	eventually(t, "the client to notice the disconnect", func() bool { return f.q.Ready() != nil })

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := f.q.Publish(ctx, sample("t1"))
	if !errors.Is(err, queue.ErrUnavailable) {
		t.Errorf("Publish with NATS down = %v, want ErrUnavailable", err)
	}
}

func TestWorkersRunConcurrently(t *testing.T) {
	const n = 4
	f := setup(t, nil)
	for i := range n {
		mustPublish(t, f.q, sample(string(rune('a'+i))))
	}

	var running atomic.Int32
	allRunning := make(chan struct{})
	var once sync.Once
	f.start(t, n, func(ctx context.Context, _ trace.Trace) error {
		if running.Add(1) == n {
			once.Do(func() { close(allRunning) })
		}
		select {
		case <-allRunning:
		case <-ctx.Done():
		}
		return nil
	})
	select {
	case <-allRunning:
	case <-time.After(waitFor):
		t.Fatalf("only %d of %d workers ran at once", running.Load(), n)
	}
	eventually(t, "the work queue to empty", func() bool { return f.messages(t, f.cfg.Stream) == 0 })
}

func TestStopWaitsForInFlightWork(t *testing.T) {
	f := setup(t, nil)
	mustPublish(t, f.q, sample("t1"))

	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	w := f.q.StartWorkers(1, func(context.Context, trace.Trace) error {
		close(started)
		<-release
		finished.Store(true)
		return nil
	})
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- w.Stop(context.Background()) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a trace was still being processed")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !finished.Load() {
		t.Error("the in-flight trace did not finish")
	}
	if n := f.messages(t, f.cfg.Stream); n != 0 {
		t.Errorf("%d messages left in the queue, want 0", n)
	}
}

// When shutdown runs out of time the trace is neither lost nor
// dead-lettered: it goes back to the queue for the next worker.
func TestStopTimeoutReturnsTraceToQueue(t *testing.T) {
	f := setup(t, func(c *queue.Config) { c.MaxDeliver = 1 }) // even on its last delivery
	mustPublish(t, f.q, sample("t1"))

	started := make(chan struct{})
	w := f.q.StartWorkers(1, func(ctx context.Context, _ trace.Trace) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want a deadline error", err)
	}
	if n := f.messages(t, f.cfg.Stream); n != 1 {
		t.Errorf("%d messages in the queue, want the trace still there", n)
	}
	if n := f.messages(t, f.cfg.DLQStream); n != 0 {
		t.Errorf("%d dead letters, want 0: shutdown is not a system error", n)
	}

	// A fresh pool picks it up.
	var got atomic.Value
	f.start(t, 1, func(_ context.Context, tr trace.Trace) error {
		got.Store(tr.TraceID)
		return nil
	})
	eventually(t, "the trace to be redelivered", func() bool { return got.Load() == "t1" })
}

func TestConnectFailsWhenNATSNeverAppears(t *testing.T) {
	cfg := testConfig("nats://127.0.0.1:1")
	cfg.ConnectTimeout = 300 * time.Millisecond
	start := time.Now()
	_, err := queue.Connect(context.Background(), cfg)
	if err == nil {
		t.Fatal("Connect succeeded with nothing listening")
	}
	if !strings.Contains(err.Error(), "not ready after 300ms") {
		t.Errorf("error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Connect took %v, want it bounded by the connect timeout", elapsed)
	}
}

// Connecting twice (a restart, or a second replica) reuses the streams and
// the consumer rather than failing or resetting them.
func TestConnectIsIdempotent(t *testing.T) {
	f := setup(t, nil)
	mustPublish(t, f.q, sample("t1"))

	second, err := queue.Connect(context.Background(), f.cfg)
	if err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	defer second.Close()
	if n := f.messages(t, f.cfg.Stream); n != 1 {
		t.Errorf("%d messages after reconnecting, want the queued trace kept", n)
	}

	var got atomic.Int32
	w := second.StartWorkers(1, func(context.Context, trace.Trace) error {
		got.Add(1)
		return nil
	})
	defer w.Stop(context.Background())
	eventually(t, "the second connection to consume the trace", func() bool { return got.Load() == 1 })
}

// A producer that publishes straight to the subject may leave trace_id out.
// The worker assigns one, and it is the same on every delivery.
func TestTraceWithoutIDGetsStableID(t *testing.T) {
	f := setup(t, nil)
	if _, err := f.js.Publish(context.Background(), f.cfg.Subject, []byte(`{"question": "q", "final_answer": "a"}`)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var ids []string
	f.start(t, 1, func(_ context.Context, tr trace.Trace) error {
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, tr.TraceID)
		if len(ids) == 1 {
			return errors.New("kev returned HTTP 503")
		}
		return nil
	})
	eventually(t, "two deliveries", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ids) == 2
	})
	if len(ids[0]) != 32 || strings.Trim(ids[0], "0123456789abcdef") != "" {
		t.Errorf("assigned trace ID = %q, want 32 hex characters", ids[0])
	}
	if ids[0] != ids[1] {
		t.Errorf("trace ID changed between deliveries: %q then %q", ids[0], ids[1])
	}
}
