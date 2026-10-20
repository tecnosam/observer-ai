package export_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/export/natsq"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/queue/queuetest"
	"github.com/observer-ai/engine/pkg/trace"
)

var keepHigh = pipeline.Decision{Keep: true, Scored: true, PKeep: 0.95, PKeepRaw: 0.93, Reason: pipeline.ReasonModel, Severity: pipeline.SeverityHigh}

func kept(id string) pipeline.Record {
	return pipeline.Record{Trace: trace.Trace{TraceID: id}, Decision: keepHigh}
}

// recordingReporter keeps every reported error, for assertions.
type recordingReporter struct {
	mu   sync.Mutex
	errs []error
}

func (r *recordingReporter) Report(_ context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *recordingReporter) all() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}

// natsQueues is a JetStream export-queue factory on an in-process server.
func natsQueues(t *testing.T) (natsq.Factory, jetstream.JetStream) {
	t.Helper()
	srv := queuetest.StartServer(t)
	conn, err := common.DialNATS(common.NATSConfig{URL: srv.ClientURL()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	streams := natsq.Streams{
		Stream: "EXPORTS", Subject: "export", Consumer: "observer",
		DLQStream: "EXPORTS_DLQ", DLQSubject: "export_dlq", DLQMaxAge: time.Hour,
		PollWait: 100 * time.Millisecond,
	}
	return natsq.Factory{Conn: conn, Streams: streams}, conn.JS
}
