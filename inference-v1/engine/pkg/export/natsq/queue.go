// Package natsq is the export.AbstractQueue on NATS JetStream: a record
// queued for a back end survives an engine restart and a back-end outage
// (up to MaxDeliver tries), and engines sharing the streams share the
// sending. The conformance suite in package export checks it keeps memq's
// rules.
package natsq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	natsPublishTimeout = 5 * time.Second
	natsAckTimeout     = 5 * time.Second
	natsFlushPoll      = 25 * time.Millisecond
	defaultNATSPoll    = 500 * time.Millisecond // short: the export workers stop last, after the trace workers
	// natsAckWait outlasts the longest send (an OTLP forward gives up
	// after 15s), so a record is not handed to a second worker while the
	// first is still sending it.
	natsAckWait = time.Minute

	headerTraceID    = "Observer-Trace-Id"
	headerExporter   = "Observer-Exporter"
	headerDeadError  = "Observer-Dead-Error"
	headerDeadAt     = "Observer-Dead-At"
	headerDeliveries = "Observer-Deliveries"
)

// Streams names the JetStream objects the export queues use. Each back
// end gets its own work-queue stream <Stream>_<NAME>, on subject
// <Subject>.<name>, read by the durable consumer <Consumer>-<name>. Records
// given up after MaxDeliver go to the shared DLQStream on DLQSubject.
type Streams struct {
	Stream     string
	Subject    string
	Consumer   string
	DLQStream  string
	DLQSubject string
	DLQMaxAge  time.Duration
	// Replicas is how many copies of each stream a NATS cluster keeps.
	Replicas int
	// PollWait bounds how long an idle worker takes to notice Shutdown.
	PollWait time.Duration
}

// Factory keeps export queues in NATS JetStream: a record queued
// for a back end survives an engine restart and a back-end outage (up to
// MaxDeliver tries), and engines sharing the streams share the sending.
type Factory struct {
	Conn    *common.Conn
	Streams Streams
}

func (f Factory) Queue(ctx context.Context, cfg export.QueueConfig) (export.AbstractQueue, error) {
	cfg = cfg.WithDefaults()
	s := f.Streams
	if s.PollWait <= 0 {
		s.PollWait = defaultNATSPoll
	}
	q := &natsQueue{
		cfg: cfg, conn: f.Conn, streams: s, pool: common.NewPool(),
		subject: s.Subject + "." + cfg.Name,
		stream:  s.Stream + "_" + strings.ToUpper(strings.ReplaceAll(cfg.Name, "-", "_")),
	}
	if err := q.setup(ctx); err != nil {
		return nil, fmt.Errorf("export queue %s: %w", cfg.Name, err)
	}
	q.pool.Start(cfg.Concurrency, func() {
		common.Pull(q.pool, q.cons, s.PollWait, q.handle, func(err error) {
			q.report(fault.Unavailable(fault.Export, "pull trace for export", err))
		})
	})
	return q, nil
}

type natsQueue struct {
	cfg     export.QueueConfig
	conn    *common.Conn
	streams Streams
	subject string
	stream  string
	cons    jetstream.Consumer
	pool    *common.Pool
}

func (q *natsQueue) setup(ctx context.Context) error {
	if err := q.conn.EnsureStreams(ctx, q.workStream(), q.deadLetterStream()); err != nil {
		return err
	}
	// MaxDeliver stays unlimited on the server: the workers dead-letter
	// themselves, so a record is never stranded by a worker dying.
	cons, err := q.conn.JS.CreateOrUpdateConsumer(ctx, q.stream, jetstream.ConsumerConfig{
		Durable:     q.streams.Consumer + "-" + q.cfg.Name,
		Description: "Observer engine exporter " + q.cfg.Name,
		AckPolicy:   jetstream.AckExplicitPolicy,
		AckWait:     natsAckWait,
		MaxDeliver:  -1,
	})
	if err != nil {
		return fmt.Errorf("consumer %s-%s: %w", q.streams.Consumer, q.cfg.Name, err)
	}
	q.cons = cons
	return nil
}

// workStream holds the records waiting for this back end. DiscardNew: a
// full queue refuses the record instead of evicting one never sent.
func (q *natsQueue) workStream() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        q.stream,
		Description: "Kept traces waiting to be sent to the " + q.cfg.Name + " back end",
		Subjects:    []string{q.subject},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
		Replicas:    q.streams.Replicas,
		MaxMsgs:     int64(q.cfg.Size),
		Discard:     jetstream.DiscardNew,
	}
}

func (q *natsQueue) deadLetterStream() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        q.streams.DLQStream,
		Description: "Kept traces that could not be sent to a back end",
		Subjects:    []string{q.streams.DLQSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Replicas:    q.streams.Replicas,
		MaxAge:      q.streams.DLQMaxAge,
	}
}

func (q *natsQueue) Enqueue(ctx context.Context, rec pipeline.Record) error {
	if q.pool.Stopping() {
		return fault.Unavailable(fault.Export, "queue trace for export", export.ErrShutDown)
	}
	data, err := encodeRecord(rec, q.conn.MaxPayload())
	if err != nil {
		return fault.Invalid(fault.Export, "encode trace for export", err)
	}
	msg := nats.NewMsg(q.subject)
	msg.Data = data
	msg.Header.Set(headerTraceID, rec.Trace.TraceID)

	ctx, cancel := context.WithTimeout(ctx, natsPublishTimeout)
	defer cancel()
	if _, err := q.conn.JS.PublishMsg(ctx, msg); err != nil {
		if common.IsStreamFull(err) {
			return fault.Full(fault.Export, "queue trace for export, skipped it", err)
		}
		return fault.Unavailable(fault.Export, "queue trace for export", err)
	}
	return nil
}

func (q *natsQueue) handle(msg jetstream.Msg) {
	attempts := 1
	if meta, err := msg.Metadata(); err == nil {
		attempts = int(meta.NumDelivered)
	}
	rec, err := decodeRecord(msg.Data())
	if err != nil {
		q.deadLetter(msg, pipeline.Record{Trace: trace.Trace{TraceID: msg.Headers().Get(headerTraceID)}}, attempts,
			fault.Invalid(fault.Export, "decode queued trace", err))
		return
	}
	sendErr := q.cfg.Send(context.Background(), rec)
	switch result, delay := export.Settle(q.cfg, rec, attempts, sendErr); result {
	case export.Sent:
		q.ack(msg, rec.Trace.TraceID)
	case export.Retried:
		if err := msg.NakWithDelay(delay); err != nil {
			q.report(fault.Unavailable(fault.Export, "return trace to export queue", err).WithTrace(rec.Trace.TraceID))
		}
	case export.GivenUp:
		q.deadLetter(msg, rec, attempts, sendErr)
	}
}

// deadLetter copies the record to the dead-letter stream and only then
// removes it from the queue. If the copy fails it stays queued.
func (q *natsQueue) deadLetter(msg jetstream.Msg, rec pipeline.Record, attempts int, cause error) {
	dead := nats.NewMsg(q.streams.DLQSubject)
	dead.Data = msg.Data()
	dead.Header.Set(headerTraceID, rec.Trace.TraceID)
	dead.Header.Set(headerExporter, q.cfg.Name)
	dead.Header.Set(headerDeadError, common.HeaderValue(cause.Error()))
	dead.Header.Set(headerDeadAt, time.Now().UTC().Format(time.RFC3339))
	dead.Header.Set(headerDeliveries, strconv.Itoa(attempts))

	ctx, cancel := context.WithTimeout(context.Background(), natsPublishTimeout)
	defer cancel()
	if _, err := q.conn.JS.PublishMsg(ctx, dead); err != nil {
		q.report(fault.Unavailable(fault.Export, "dead-letter exported trace, it stays queued", err).AsSevere().WithTrace(rec.Trace.TraceID))
		_ = msg.NakWithDelay(q.cfg.RetryDelay(attempts))
		return
	}
	q.report(export.SendFault(q.cfg, rec, attempts, "export trace, dead-lettered", cause).AsSevere().With("dlq_stream", q.streams.DLQStream))
	q.ack(msg, rec.Trace.TraceID)
}

func (q *natsQueue) ack(msg jetstream.Msg, traceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), natsAckTimeout)
	defer cancel()
	if err := msg.DoubleAck(ctx); err != nil {
		q.report(fault.Unavailable(fault.Export, "ack exported trace, it may be sent again", err).WithTrace(traceID))
	}
}

func (q *natsQueue) report(err *fault.Error) {
	q.cfg.Reporter.Report(context.Background(), err.With("exporter", q.cfg.Name))
}

// Depth counts the back end's records not yet acknowledged, across every
// engine sharing the stream.
func (q *natsQueue) Depth(ctx context.Context) (int64, error) {
	info, err := q.cons.Info(ctx)
	if err != nil {
		return 0, err
	}
	return int64(info.NumPending) + int64(info.NumAckPending), nil
}

// Flush waits until the back end's stream is empty: every record sent,
// dead-lettered, or (with other engines sharing it) taken by them.
func (q *natsQueue) Flush(ctx context.Context) error {
	for {
		info, err := q.cons.Info(ctx)
		if err != nil {
			return err
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(natsFlushPoll):
		}
	}
}

// Shutdown stops the workers after the record each is sending. What is
// still queued stays in JetStream for the next start.
func (q *natsQueue) Shutdown(ctx context.Context) error {
	return q.pool.Stop(ctx)
}

// recordMessage is a pipeline.Record as it is stored in JetStream.
type recordMessage struct {
	Trace     trace.Trace `json:"trace"`
	Agent     string      `json:"agent"`
	Keep      bool        `json:"keep"`
	Scored    bool        `json:"scored"`
	PKeep     float64     `json:"p_keep"`
	PKeepRaw  float64     `json:"p_keep_raw"`
	Reason    string      `json:"reason"`
	Severity  string      `json:"severity"`
	Model     string      `json:"model"`
	LatencyNS int64       `json:"latency_ns"`
}

// encodeRecord serializes rec. A trace too big for the server keeps its
// decision but loses its original spans, and is exported rebuilt.
func encodeRecord(rec pipeline.Record, maxPayload int) ([]byte, error) {
	d := rec.Decision
	m := recordMessage{
		Trace: rec.Trace, Agent: string(rec.Agent), Keep: d.Keep, Scored: d.Scored, PKeep: float64(d.PKeep), PKeepRaw: float64(d.PKeepRaw),
		Reason: string(d.Reason), Severity: string(d.Severity), Model: string(rec.Model), LatencyNS: int64(rec.Latency),
	}
	data, err := json.Marshal(m)
	if err != nil || maxPayload <= 0 || len(data) <= maxPayload || len(m.Trace.OTLP) == 0 {
		return data, err
	}
	m.Trace.OTLP = nil
	return json.Marshal(m)
}

func decodeRecord(data []byte) (pipeline.Record, error) {
	var m recordMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return pipeline.Record{}, err
	}
	return pipeline.Record{
		Trace: m.Trace,
		Agent: pipeline.AgentLabel(m.Agent),
		Decision: pipeline.Decision{
			Keep: m.Keep, Scored: m.Scored, PKeep: pipeline.Probability(m.PKeep), PKeepRaw: pipeline.Probability(m.PKeepRaw),
			Reason: pipeline.Reason(m.Reason), Severity: pipeline.Severity(m.Severity),
		},
		Model:   pipeline.ModelName(m.Model),
		Latency: time.Duration(m.LatencyNS),
	}, nil
}
