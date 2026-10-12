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
func (q *Queue) StartWorkers(n int, handler Handler) *Workers {
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
				w.q.logger.Warn("queue pull failed", "error", err.Error())
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

func (w *Workers) handle(msg jetstream.Msg) {
	logger := w.q.logger
	deliveries := 1
	var sequence uint64
	if meta, err := msg.Metadata(); err == nil {
		deliveries = int(meta.NumDelivered)
		sequence = meta.Sequence.Stream
	}

	t, err := trace.Decode(bytes.NewReader(msg.Data()))
	if err != nil {
		// Nothing a retry could fix.
		w.deadLetter(msg, ReasonInvalidPayload, msg.Headers().Get(HeaderTraceID), err, deliveries, sequence)
		return
	}

	if t.TraceID == "" {
		// Published straight to the subject, without the API's ID.
		t.TraceID = sequenceID(w.q.cfg.Stream, sequence)
	}

	err = w.handler(w.workCtx, t)
	switch {
	case err == nil:
		w.ack(msg, t.TraceID)

	case w.workCtx.Err() != nil:
		// Shutdown ran out of time mid-trace. Hand it straight back.
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Error("queue nak failed", "trace_id", t.TraceID, "error", nakErr.Error())
		}
		logger.Warn("trace returned to the queue on shutdown", "trace_id", t.TraceID)

	case deliveries >= w.q.cfg.MaxDeliver:
		w.deadLetter(msg, ReasonSystemError, t.TraceID, err, deliveries, sequence)

	default:
		delay := w.redeliveryDelay(deliveries)
		logger.Warn("system error, trace will be redelivered",
			"trace_id", t.TraceID, "delivery", deliveries, "max_deliver", w.q.cfg.MaxDeliver,
			"retry_in", delay.String(), "error", err.Error())
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			logger.Error("queue nak failed", "trace_id", t.TraceID, "error", nakErr.Error())
		}
	}
}

// deadLetter copies the message to the dead-letter stream and, only once
// that is confirmed, removes it from the work queue. If the copy fails the
// trace stays queued and is tried again later.
func (w *Workers) deadLetter(msg jetstream.Msg, reason, traceID string, cause error, deliveries int, sequence uint64) {
	logger := w.q.logger
	dead := nats.NewMsg(w.q.cfg.DLQSubject)
	dead.Data = msg.Data()
	dead.Header.Set(HeaderTraceID, traceID)
	dead.Header.Set(HeaderDeadReason, reason)
	dead.Header.Set(HeaderDeadError, headerValue(cause.Error()))
	dead.Header.Set(HeaderDeadAt, time.Now().UTC().Format(time.RFC3339))
	dead.Header.Set(HeaderDeliveries, strconv.Itoa(deliveries))
	dead.Header.Set(HeaderSourceStream, w.q.cfg.Stream)
	dead.Header.Set(HeaderSourceSequence, strconv.FormatUint(sequence, 10))

	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	if _, err := w.q.js.PublishMsg(ctx, dead); err != nil {
		delay := w.redeliveryDelay(deliveries)
		logger.Error("dead-letter publish failed, trace stays queued",
			"trace_id", traceID, "retry_in", delay.String(), "error", err.Error())
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			logger.Error("queue nak failed", "trace_id", traceID, "error", nakErr.Error())
		}
		return
	}
	logger.Error("trace dead-lettered",
		"trace_id", traceID, "reason", reason, "deliveries", deliveries,
		"dlq_stream", w.q.cfg.DLQStream, "error", cause.Error())
	w.ack(msg, traceID)
}

// ack confirms the acknowledgement with the server, because an ack that
// goes missing means the trace is processed a second time.
func (w *Workers) ack(msg jetstream.Msg, traceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if err := msg.DoubleAck(ctx); err != nil {
		w.q.logger.Error("queue ack failed, trace may be redelivered", "trace_id", traceID, "error", err.Error())
	}
}

// redeliveryDelay is the wait before the next delivery, given how many
// deliveries have happened so far.
func (w *Workers) redeliveryDelay(deliveries int) time.Duration {
	backoff := w.q.cfg.RedeliveryBackoff
	if len(backoff) == 0 {
		return 0
	}
	return backoff[min(max(deliveries-1, 0), len(backoff)-1)]
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
