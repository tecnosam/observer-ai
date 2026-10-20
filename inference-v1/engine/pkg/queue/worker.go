package queue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/common"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	ackTimeout = 5 * time.Second
)

// Dead-letter reasons, carried in HeaderDeadReason.
const (
	ReasonSystemError    = "system_error"    // kept failing until max_deliver
	ReasonInvalidPayload = "invalid_payload" // not a valid trace; never retried
)

// Headers on a dead-lettered message. The body is the original payload, so
// a message can be replayed by publishing it back to the trace subject.
const (
	HeaderDeadReason     = "Observer-Dead-Reason"
	HeaderDeadError      = "Observer-Dead-Error"
	HeaderDeadAt         = "Observer-Dead-At"
	HeaderDeliveries     = "Observer-Deliveries"
	HeaderSourceStream   = "Observer-Source-Stream"
	HeaderSourceSequence = "Observer-Source-Sequence"
)

// Handler processes one trace. Returning an error means a system error: the
// trace was not processed and should be tried again later. A handler that
// reaches a decision, by whatever route, returns nil.
type Handler func(ctx context.Context, t trace.Trace) error

// Workers is a running pool of queue workers. Stop stops pulling new
// traces and waits for the ones in flight; if its ctx ends first, the
// in-flight work is cancelled and those traces go back to the queue.
type Workers struct {
	*common.Pool
	q       *Queue
	handler Handler
}

// StartWorkers starts n workers pulling from the queue's durable consumer.
// Every engine process that uses the same consumer name shares the work.
func (q *Queue) StartWorkers(n int, handler Handler) Stopper {
	w := &Workers{Pool: common.NewPool(), q: q, handler: handler}
	w.Start(n, func() {
		common.Pull(w.Pool, q.cons, q.cfg.PollWait, w.handle, func(err error) {
			q.report(fault.Unavailable(fault.Queue, "pull trace", err))
		})
	})
	return w
}

// delivery is one message as a worker received it.
type delivery struct {
	msg      jetstream.Msg
	traceID  string
	count    int    // how many times it has been delivered, this one included
	sequence uint64 // its place in the trace stream
}

func newDelivery(msg jetstream.Msg) delivery {
	d := delivery{msg: msg, traceID: msg.Headers().Get(HeaderTraceID), count: 1}
	if meta, err := msg.Metadata(); err == nil {
		d.count = int(meta.NumDelivered)
		d.sequence = meta.Sequence.Stream
	}
	return d
}

func (w *Workers) handle(msg jetstream.Msg) {
	d := newDelivery(msg)
	t, err := trace.Decode(bytes.NewReader(msg.Data()))
	if err != nil {
		w.deadLetter(d, ReasonInvalidPayload, err) // nothing a retry could fix
		return
	}
	if t.TraceID == "" {
		t.TraceID = sequenceID(w.q.cfg.Stream, d.sequence) // published straight to the subject, without the API's ID
	}
	d.traceID = t.TraceID
	w.settle(d, w.handler(w.WorkCtx(), t))
}

// settle acknowledges, returns or dead-letters a delivery once its trace has been handled.
func (w *Workers) settle(d delivery, err error) {
	switch {
	case err == nil:
		w.ack(d)
	case w.WorkCtx().Err() != nil:
		w.returnOnShutdown(d)
	case w.q.cfg.retry().Exhausted(d.count):
		w.deadLetter(d, ReasonSystemError, err)
	default:
		w.redeliver(d, err)
	}
}

// returnOnShutdown hands a trace straight back when shutdown ran out of time mid-trace.
func (w *Workers) returnOnShutdown(d delivery) {
	if err := d.msg.Nak(); err != nil {
		w.q.report(fault.Unavailable(fault.Queue, "return trace on shutdown", err).WithTrace(d.traceID))
	}
	w.q.logger.Warn("trace returned to the queue on shutdown", "trace_id", d.traceID)
}

// redeliver schedules another delivery after a system error, with the configured backoff.
func (w *Workers) redeliver(d delivery, cause error) {
	delay := w.redeliveryDelay(d.count)
	w.q.report(fault.New(fault.KindOf(cause), fault.Queue, "process trace, will redeliver", cause).
		WithTrace(d.traceID).With("delivery", d.count).With("max_deliver", w.q.cfg.MaxDeliver).With("retry_in", delay.String()))
	if err := d.msg.NakWithDelay(delay); err != nil {
		w.q.report(fault.Unavailable(fault.Queue, "return trace to queue", err).WithTrace(d.traceID))
	}
}

// deadLetter copies the message to the dead-letter stream and, only once
// that is confirmed, removes it from the work queue. If the copy fails the
// trace stays queued and is tried again later.
func (w *Workers) deadLetter(d delivery, reason string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	if _, err := w.q.js.PublishMsg(ctx, w.deadLetterMsg(d, reason, cause)); err != nil {
		w.retryDeadLetter(d, err)
		return
	}
	w.q.report(fault.New(fault.KindOf(cause), fault.Queue, "dead-letter trace", cause).AsSevere().
		WithTrace(d.traceID).With("reason", reason).With("deliveries", d.count).With("dlq_stream", w.q.cfg.DLQStream))
	w.ack(d)
}

// deadLetterMsg is the original payload with headers saying why and when it failed, so it can be replayed.
func (w *Workers) deadLetterMsg(d delivery, reason string, cause error) *nats.Msg {
	dead := nats.NewMsg(w.q.cfg.DLQSubject)
	dead.Data = d.msg.Data()
	dead.Header.Set(HeaderTraceID, d.traceID)
	dead.Header.Set(HeaderDeadReason, reason)
	dead.Header.Set(HeaderDeadError, common.HeaderValue(cause.Error()))
	dead.Header.Set(HeaderDeadAt, time.Now().UTC().Format(time.RFC3339))
	dead.Header.Set(HeaderDeliveries, strconv.Itoa(d.count))
	dead.Header.Set(HeaderSourceStream, w.q.cfg.Stream)
	dead.Header.Set(HeaderSourceSequence, strconv.FormatUint(d.sequence, 10))
	return dead
}

// retryDeadLetter leaves the trace queued when it could not be dead-lettered.
func (w *Workers) retryDeadLetter(d delivery, err error) {
	delay := w.redeliveryDelay(d.count)
	w.q.report(fault.Unavailable(fault.Queue, "dead-letter trace, it stays queued", err).AsSevere().
		WithTrace(d.traceID).With("retry_in", delay.String()))
	if nakErr := d.msg.NakWithDelay(delay); nakErr != nil {
		w.q.report(fault.Unavailable(fault.Queue, "return trace to queue", nakErr).WithTrace(d.traceID))
	}
}

// ack confirms the acknowledgement with the server, because an ack that
// goes missing means the trace is processed a second time.
func (w *Workers) ack(d delivery) {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if err := d.msg.DoubleAck(ctx); err != nil {
		w.q.report(fault.Unavailable(fault.Queue, "ack trace, it may be redelivered", err).AsSevere().WithTrace(d.traceID))
	}
}

// redeliveryDelay is the wait before the next delivery, given how many
// deliveries have happened so far.
func (w *Workers) redeliveryDelay(deliveries int) time.Duration {
	return w.q.cfg.retry().Delay(deliveries)
}

// sequenceID derives a 32-hex-character trace ID from a message's place in
// the stream, so a trace keeps the same ID however often it is redelivered.
func sequenceID(stream string, sequence uint64) string {
	sum := sha256.Sum256([]byte(stream + ":" + strconv.FormatUint(sequence, 10)))
	return hex.EncodeToString(sum[:16])
}
