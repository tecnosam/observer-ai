package queue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/fault"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	pullErrorPause = time.Second
	ackTimeout     = 5 * time.Second
	maxHeaderError = 1024
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

// Workers is a running pool of queue workers.
type Workers struct {
	q       *Queue
	handler Handler

	stop     chan struct{} // closed to stop pulling new traces
	stopOnce sync.Once
	wg       sync.WaitGroup

	// workCtx is the context traces are processed under. It is cancelled
	// only when Stop runs out of time.
	workCtx    context.Context
	cancelWork context.CancelFunc
}

// StartWorkers starts n workers pulling from the queue's durable consumer.
// Every engine process that uses the same consumer name shares the work.
func (q *Queue) StartWorkers(n int, handler Handler) Stopper {
	w := &Workers{q: q, handler: handler, stop: make(chan struct{})}
	w.workCtx, w.cancelWork = context.WithCancel(context.Background())
	for range n {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.run()
		}()
	}
	return w
}

// Stop stops pulling new traces and waits for the ones in flight. If ctx
// ends first, the in-flight work is cancelled and those traces go back to
// the queue for redelivery; Stop then returns ctx's error.
func (w *Workers) Stop(ctx context.Context) error {
	w.stopOnce.Do(func() { close(w.stop) })
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		w.cancelWork()
		<-done
	}
	w.cancelWork()
	return err
}

func (w *Workers) stopping() bool {
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

func (w *Workers) run() {
	for !w.stopping() {
		// One message per pull: a worker never holds a trace it is not
		// working on, so nothing waits out its ack deadline in a buffer.
		msg, err := w.q.cons.Next(jetstream.FetchMaxWait(w.q.cfg.PollWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) {
				continue // nothing queued
			}
			if !w.stopping() {
				w.q.report(fault.Unavailable(fault.Queue, "pull trace", err))
				select {
				case <-w.stop:
				case <-time.After(pullErrorPause):
				}
			}
			continue
		}
		w.handle(msg)
	}
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
	w.settle(d, w.handler(w.workCtx, t))
}

// settle acknowledges, returns or dead-letters a delivery once its trace has been handled.
func (w *Workers) settle(d delivery, err error) {
	switch {
	case err == nil:
		w.ack(d)
	case w.workCtx.Err() != nil:
		w.returnOnShutdown(d)
	case d.count >= w.q.cfg.MaxDeliver:
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
	dead.Header.Set(HeaderDeadError, headerValue(cause.Error()))
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
	return redeliveryDelay(w.q.cfg.RedeliveryBackoff, deliveries)
}

// sequenceID derives a 32-hex-character trace ID from a message's place in
// the stream, so a trace keeps the same ID however often it is redelivered.
func sequenceID(stream string, sequence uint64) string {
	sum := sha256.Sum256([]byte(stream + ":" + strconv.FormatUint(sequence, 10)))
	return hex.EncodeToString(sum[:16])
}

// headerValue makes s safe to carry in a NATS header: one line, bounded.
func headerValue(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxHeaderError {
		s = s[:maxHeaderError] + "..."
	}
	return s
}
