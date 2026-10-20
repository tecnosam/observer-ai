package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/observer-ai/engine/pkg/export"
	exportnats "github.com/observer-ai/engine/pkg/export/natsq"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/queue"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
	"github.com/observer-ai/engine/pkg/trace"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func queueConfig() queue.Config {
	return queue.Config{
		ConnectTimeout: 5 * time.Second,
		Stream:         "TRACES", Subject: "traces.in", Consumer: "workers",
		MaxMessages: 10, AckWait: 30 * time.Second, MaxDeliver: 2,
		DLQStream: "TRACES_DLQ", DLQSubject: "traces.dlq", DLQMaxAge: time.Hour,
		PollWait: 100 * time.Millisecond, Logger: quiet,
	}
}

var streams = exportnats.Streams{
	Stream: "EXPORTS", Subject: "exports", Consumer: "workers-export",
	DLQStream: "EXPORTS_DLQ", DLQSubject: "exports_dlq", DLQMaxAge: time.Hour, PollWait: 100 * time.Millisecond,
}

func TestNewChoosesByURL(t *testing.T) {
	f, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if f.Kind() != KindMemory {
		t.Errorf("no URL: kind %s, want memory", f.Kind())
	}

	srv := queuetest.StartServer(t)
	f, err = New(Config{URL: srv.ClientURL(), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.Kind() != KindNATS {
		t.Errorf("with URL: kind %s, want nats", f.Kind())
	}
}

// Each family builds a working trace queue and export queue.
func TestFamiliesWork(t *testing.T) {
	srv := queuetest.StartServer(t)
	for _, url := range []string{"", srv.ClientURL()} {
		f, err := New(Config{URL: url, Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		t.Run(string(f.Kind()), func(t *testing.T) {
			defer f.Close()
			q, err := f.Ingest(context.Background(), queueConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			got := make(chan string, 1)
			w := q.StartWorkers(1, func(_ context.Context, tr trace.Trace) error {
				got <- tr.TraceID
				return nil
			})
			defer w.Stop(context.Background())
			if err := q.Publish(context.Background(), trace.Trace{TraceID: "t1", Question: "q", FinalAnswer: "a"}); err != nil {
				t.Fatal(err)
			}
			wantReceived(t, got, "t1")

			sent := make(chan string, 1)
			eq, err := f.ExportQueues(streams).Queue(context.Background(), export.QueueConfig{
				Name: "phoenix",
				Send: func(_ context.Context, rec pipeline.Record) error {
					sent <- rec.Trace.TraceID
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer eq.Shutdown(context.Background())
			if err := eq.Enqueue(context.Background(), pipeline.Record{Trace: trace.Trace{TraceID: "t2"}}); err != nil {
				t.Fatal(err)
			}
			wantReceived(t, sent, "t2")
		})
	}
}

func wantReceived(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Errorf("received %q, want %q", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%q never arrived", want)
	}
}
