package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/natsconn"
	"github.com/observer-ai/engine/internal/pipeline"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	natsPublishTimeout = 5 * time.Second
	natsAckTimeout     = 5 * time.Second
	natsPullErrorPause = time.Second
	natsFlushPoll      = 25 * time.Millisecond
	defaultNATSPoll    = 500 * time.Millisecond // short: the export workers stop last, after the trace workers
	// natsAckWait outlasts the longest send (a forward gives up after
	// forwardMaxElapsed), so a record is not handed to a second worker
	// while the first is still sending it.
	natsAckWait = 4 * forwardMaxElapsed

	headerTraceID    = "Observer-Trace-Id"
	headerExporter   = "Observer-Exporter"
	headerDeadError  = "Observer-Dead-Error"
	headerDeadAt     = "Observer-Dead-At"
	headerDeliveries = "Observer-Deliveries"
	maxHeaderError   = 1024
)

// NATSStreams names the JetStream objects the export queues use. Each back
// end gets its own work-queue stream <Stream>_<NAME>, on subject
// <Subject>.<name>, read by the durable consumer <Consumer>-<name>. Records
// given up after MaxDeliver go to the shared DLQStream on DLQSubject.
type NATSStreams struct {
	Stream     string
	Subject    string
	Consumer   string
	DLQStream  string
	DLQSubject string
	DLQMaxAge  time.Duration
	// PollWait bounds how long an idle worker takes to notice Shutdown.
	PollWait time.Duration
}

// NATSQueueFactory keeps export queues in NATS JetStream: a record queued
// for a back end survives an engine restart and a back-end outage (up to
// MaxDeliver tries), and engines sharing the streams share the sending.
type NATSQueueFactory struct {
	Conn    *natsconn.Conn
	Streams NATSStreams
}

func (f NATSQueueFactory) Queue(ctx context.Context, cfg QueueConfig) (AbstractQueue, error) {
	cfg = withQueueDefaults(cfg)
	s := f.Streams
	if s.PollWait <= 0 {
		s.PollWait = defaultNATSPoll
	}
	q := &natsQueue{cfg: cfg, conn: f.Conn, streams: s, subject: s.Subject + "." + cfg.Name, stop: make(chan struct{})}
	if err := q.setup(ctx); err != nil {
		return nil, fmt.Errorf("export queue %s: %w", cfg.Name, err)
	}
	for range cfg.Concurrency {
		q.workers.Add(1)
		go q.work()
	}
	return q, nil
}

type natsQueue struct {
	cfg     QueueConfig
	conn    *natsconn.Conn
	streams NATSStreams
	subject string
	stream  string
	cons    jetstream.Consumer

	stopOnce sync.Once
	stop     chan struct{}
	workers  sync.WaitGroup
}

func (q *natsQueue) setup(ctx context.Context) error {
	js, s := q.conn.JS, q.streams
	q.stream = s.Stream + "_" + strings.ToUpper(strings.ReplaceAll(q.cfg.Name, "-", "_"))

	// DiscardNew: a full queue refuses the record instead of evicting one
	// that was never sent.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        q.stream,
		Description: "Kept traces waiting to be sent to the " + q.cfg.Name + " back end",
		Subjects:    []string{q.subject},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
		MaxMsgs:     int64(q.cfg.Size),
		Discard:     jetstream.DiscardNew,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", q.stream, err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        s.DLQStream,
		Description: "Kept traces that could not be sent to a back end",
		Subjects:    []string{s.DLQSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      s.DLQMaxAge,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", s.DLQStream, err)
	}
	// MaxDeliver stays unlimited on the server: the workers dead-letter
	// themselves, so a record is never stranded by a worker dying.
	cons, err := js.CreateOrUpdateConsumer(ctx, q.stream, jetstream.ConsumerConfig{
		Durable:     s.Consumer + "-" + q.cfg.Name,
		Description: "Observer engine exporter " + q.cfg.Name,
		AckPolicy:   jetstream.AckExplicitPolicy,
		AckWait:     natsAckWait,
		MaxDeliver:  -1,
	})
	if err != nil {
		return fmt.Errorf("consumer %s-%s: %w", s.Consumer, q.cfg.Name, err)
	}
	q.cons = cons
	return nil
}

func (q *natsQueue) Enqueue(ctx context.Context, rec pipeline.Record) error {
	if q.stopping() {
		return fault.Unavailable(fault.Export, "queue trace for export", errShutDown)
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
		if isStreamFull(err) {
			return fault.Full(fault.Export, "queue trace for export, skipped it", err)
		}
		return fault.Unavailable(fault.Export, "queue trace for export", err)
	}
	return nil
}

func isStreamFull(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && strings.Contains(apiErr.Description, "maximum messages exceeded")
}

func (q *natsQueue) stopping() bool {
	select {
	case <-q.stop:
		return true
	default:
		return false
	}
}

func (q *natsQueue) work() {
	defer q.workers.Done()
	for !q.stopping() {
		msg, err := q.cons.Next(jetstream.FetchMaxWait(q.streams.PollWait))
		if err != nil {
			if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) && !q.stopping() {
				q.report(fault.Unavailable(fault.Export, "pull trace for export", err))
				select {
				case <-q.stop:
				case <-time.After(natsPullErrorPause):
				}
			}
			continue
		}
		q.handle(msg)
	}
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
	switch result, delay := settle(q.cfg, rec, attempts, sendErr); result {
	case sent:
		q.ack(msg, rec.Trace.TraceID)
	case retried:
		if err := msg.NakWithDelay(delay); err != nil {
			q.report(fault.Unavailable(fault.Export, "return trace to export queue", err).WithTrace(rec.Trace.TraceID))
		}
	case givenUp:
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
	dead.Header.Set(headerDeadError, oneLine(cause.Error()))
	dead.Header.Set(headerDeadAt, time.Now().UTC().Format(time.RFC3339))
	dead.Header.Set(headerDeliveries, strconv.Itoa(attempts))

	ctx, cancel := context.WithTimeout(context.Background(), natsPublishTimeout)
	defer cancel()
	if _, err := q.conn.JS.PublishMsg(ctx, dead); err != nil {
		q.report(fault.Unavailable(fault.Export, "dead-letter exported trace, it stays queued", err).AsSevere().WithTrace(rec.Trace.TraceID))
		_ = msg.NakWithDelay(backoffFor(q.cfg.Backoff, attempts))
		return
	}
	q.report(sendFault(q.cfg, rec, attempts, "export trace, dead-lettered", cause).AsSevere().With("dlq_stream", q.streams.DLQStream))
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
	q.stopOnce.Do(func() { close(q.stop) })
	return wait(ctx, &q.workers)
}

// recordMessage is a pipeline.Record as it is stored in JetStream.
type recordMessage struct {
	Trace     trace.Trace `json:"trace"`
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
		Trace: rec.Trace, Keep: d.Keep, Scored: d.Scored, PKeep: float64(d.PKeep), PKeepRaw: float64(d.PKeepRaw),
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
		Decision: pipeline.Decision{
			Keep: m.Keep, Scored: m.Scored, PKeep: pipeline.Probability(m.PKeep), PKeepRaw: pipeline.Probability(m.PKeepRaw),
			Reason: pipeline.Reason(m.Reason), Severity: pipeline.Severity(m.Severity),
		},
		Model:   pipeline.ModelName(m.Model),
		Latency: time.Duration(m.LatencyNS),
	}, nil
}

// oneLine makes s safe to carry in a NATS header: one line, bounded.
func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxHeaderError {
		s = s[:maxHeaderError] + "..."
	}
	return s
}
