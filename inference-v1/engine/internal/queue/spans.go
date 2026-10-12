package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/observer-ai/engine/internal/trace"
)

const (
	assemblerAckWait     = 30 * time.Second
	assemblerOpTimeout   = 10 * time.Second
	assemblerRetryDelay  = 5 * time.Second
	assemblerMinRecheck  = 50 * time.Millisecond
	maxPendingAssemblies = 100_000
)

// SpanConfig configures how OTLP spans are stored and assembled into traces.
type SpanConfig struct {
	// Stream holds spans, one subject per trace: <Subject>.<trace id>.
	Stream  string
	Subject string
	// MaxAge removes spans that were never assembled.
	MaxAge time.Duration

	// PendingStream holds one marker per trace that has spans waiting.
	PendingStream  string
	PendingSubject string

	// IdleTimeout is how long a trace must receive no spans before it is
	// considered complete.
	IdleTimeout time.Duration
	// MaxWait assembles a trace this long after its first span even if
	// spans are still arriving.
	MaxWait time.Duration
}

func (c SpanConfig) enabled() bool { return c.Stream != "" }

func (q *Queue) setupSpans(ctx context.Context) error {
	sc := q.cfg.Spans

	// Spans stay here until their trace is assembled, then are purged.
	// MaxAge is the backstop for spans whose trace never completes.
	spans, err := q.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        sc.Stream,
		Description: "OTLP spans waiting to be assembled into traces, one subject per trace ID",
		Subjects:    []string{sc.Subject + ".*"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      sc.MaxAge,
	})
	if err != nil {
		return fmt.Errorf("stream %s: %w", sc.Stream, err)
	}
	q.spans = spans

	// One marker per trace: a second marker for a trace that already has
	// one is refused, so however many span batches arrive, a trace is
	// assembled by one assembler at a time.
	if _, err := q.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 sc.PendingStream,
		Description:          "Traces with spans waiting to be assembled, one marker per trace ID",
		Subjects:             []string{sc.PendingSubject + ".*"},
		Retention:            jetstream.WorkQueuePolicy,
		Storage:              jetstream.FileStorage,
		MaxMsgsPerSubject:    1,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", sc.PendingStream, err)
	}

	// A marker stays pending (delayed, unacknowledged) while its trace is
	// still receiving spans, so the limit on pending markers is the limit
	// on traces in flight.
	cons, err := q.js.CreateOrUpdateConsumer(ctx, sc.PendingStream, jetstream.ConsumerConfig{
		Durable:       q.cfg.Consumer + "-assemblers",
		Description:   "Observer engine trace assemblers",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       assemblerAckWait,
		MaxDeliver:    -1,
		MaxAckPending: maxPendingAssemblies,
	})
	if err != nil {
		return fmt.Errorf("consumer %s-assemblers: %w", q.cfg.Consumer, err)
	}
	q.pendingCons = cons
	return nil
}

// PublishSpans stores span batches, keyed by trace ID, and marks each trace
// as pending assembly. It returns once NATS has confirmed every batch.
// byTrace maps a trace ID to a serialized batch of that trace's spans.
func (q *Queue) PublishSpans(ctx context.Context, byTrace map[string][]byte) error {
	if !q.cfg.Spans.enabled() {
		return errors.New("queue: span storage is not configured")
	}
	sc := q.cfg.Spans
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	maxPayload := int(q.nc.MaxPayload())
	var stored []string
	var acks []jetstream.PubAckFuture
	for traceID, data := range byTrace {
		if maxPayload > 0 && len(data) > maxPayload {
			// Retrying cannot help, so do not make the exporter retry.
			q.logger.Error("span batch exceeds the NATS payload limit and was dropped",
				"trace_id", traceID, "bytes", len(data), "max_payload", maxPayload)
			continue
		}
		ack, err := q.js.PublishMsgAsync(&nats.Msg{Subject: sc.Subject + "." + traceID, Data: data})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		stored = append(stored, traceID)
		acks = append(acks, ack)
	}
	if err := waitAcks(ctx, acks, false); err != nil {
		return err
	}

	// Markers go out only after the spans are stored, so an assembler never
	// finds a marker whose spans have not landed yet.
	acks = acks[:0]
	for _, traceID := range stored {
		ack, err := q.js.PublishMsgAsync(&nats.Msg{Subject: sc.PendingSubject + "." + traceID})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		acks = append(acks, ack)
	}
	return waitAcks(ctx, acks, true)
}

// waitAcks waits for every publish to be confirmed. With markers set, a
// refusal because the trace already has a marker counts as success.
func waitAcks(ctx context.Context, acks []jetstream.PubAckFuture, markers bool) error {
	for _, ack := range acks {
		select {
		case <-ack.Ok():
		case err := <-ack.Err():
			if markers && isMarkerPresent(err) {
				continue
			}
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrUnavailable, ctx.Err())
		}
	}
	return nil
}

func isMarkerPresent(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && strings.Contains(apiErr.Description, "maximum messages per subject exceeded")
}

// AssembleFunc builds a trace from the span batches collected for it. An
// error means the spans do not make a trace worth scoring; they are then
// discarded.
type AssembleFunc func(traceID string, batches [][]byte) (trace.Trace, error)

// Assemblers is a running pool that turns completed span sets into traces
// on the work queue.
type Assemblers struct {
	q        *Queue
	assemble AssembleFunc
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// StartAssemblers starts n assemblers. Each takes a pending-trace marker,
// waits until the trace has been quiet for IdleTimeout (or MaxWait has
// passed), assembles its spans and publishes the trace to the work queue.
func (q *Queue) StartAssemblers(n int, assemble AssembleFunc) *Assemblers {
	a := &Assemblers{q: q, assemble: assemble, stop: make(chan struct{})}
	for range n {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.run()
		}()
	}
	return a
}

// Stop stops the assemblers after the trace each is working on. Unassembled
// spans stay in the stream for the next start.
func (a *Assemblers) Stop(ctx context.Context) error {
	a.stopOnce.Do(func() { close(a.stop) })
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Assemblers) stopping() bool {
	select {
	case <-a.stop:
		return true
	default:
		return false
	}
}

func (a *Assemblers) run() {
	for !a.stopping() {
		msg, err := a.q.pendingCons.Next(jetstream.FetchMaxWait(a.q.cfg.PollWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) {
				continue
			}
			if !a.stopping() {
				a.q.logger.Warn("assembler pull failed", "error", err.Error())
				select {
				case <-a.stop:
				case <-time.After(pullErrorPause):
				}
			}
			continue
		}
		a.handle(msg)
	}
}

func (a *Assemblers) handle(marker jetstream.Msg) {
	q, sc, logger := a.q, a.q.cfg.Spans, a.q.logger
	traceID := marker.Subject()[strings.LastIndexByte(marker.Subject(), '.')+1:]
	spanSubject := sc.Subject + "." + traceID
	ctx, cancel := context.WithTimeout(context.Background(), assemblerOpTimeout)
	defer cancel()

	retry := func(delay time.Duration) {
		if err := marker.NakWithDelay(delay); err != nil {
			logger.Error("assembler nak failed", "trace_id", traceID, "error", err.Error())
		}
	}

	last, err := q.spans.GetLastMsgForSubject(ctx, spanSubject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		// Already assembled, or the spans aged out.
		a.ack(marker, traceID)
		return
	}
	if err != nil {
		logger.Warn("assembler could not read spans", "trace_id", traceID, "error", err.Error())
		retry(assemblerRetryDelay)
		return
	}

	// Not quiet yet: look again when it could be. The marker's own
	// timestamp is when the trace's first span arrived.
	now := time.Now()
	quietFor := now.Sub(last.Time)
	age := sc.MaxWait // without metadata, do not wait
	if meta, err := marker.Metadata(); err == nil {
		age = now.Sub(meta.Timestamp)
	}
	if quietFor < sc.IdleTimeout && age < sc.MaxWait {
		retry(max(min(sc.IdleTimeout-quietFor, sc.MaxWait-age), assemblerMinRecheck))
		return
	}

	batches, err := a.readSpans(ctx, spanSubject, last.Sequence)
	if err != nil {
		logger.Warn("assembler could not read spans", "trace_id", traceID, "error", err.Error())
		retry(assemblerRetryDelay)
		return
	}

	t, err := a.assemble(traceID, batches)
	if err != nil {
		logger.Info("spans discarded", "trace_id", traceID, "batches", len(batches), "reason", err.Error())
	} else if err := a.publish(ctx, t, last.Sequence); err != nil {
		// The spans stay put; the marker brings us back.
		logger.Warn("assembled trace could not be queued, will retry", "trace_id", traceID, "error", err.Error())
		retry(assemblerRetryDelay)
		return
	} else {
		logger.Info("trace assembled from spans",
			"trace_id", traceID, "batches", len(batches), "tool_calls", len(t.ToolCalls), "evidence", len(t.Evidence))
	}

	// Remove exactly the spans that were read; later arrivals stay.
	if err := q.spans.Purge(ctx, jetstream.WithPurgeSubject(spanSubject), jetstream.WithPurgeSequence(last.Sequence+1)); err != nil {
		logger.Warn("assembler could not purge spans", "trace_id", traceID, "error", err.Error())
		retry(assemblerRetryDelay)
		return
	}
	a.ack(marker, traceID)

	// A span that arrived while we worked found the marker still in place
	// and could not leave its own. Leave one for it now.
	if _, err := q.spans.GetLastMsgForSubject(ctx, spanSubject); err == nil {
		if _, err := q.js.Publish(ctx, sc.PendingSubject+"."+traceID, nil); err != nil && !isMarkerPresent(err) {
			logger.Warn("assembler could not re-mark trace", "trace_id", traceID, "error", err.Error())
		}
	}
}

// readSpans returns the trace's span batches up to and including upTo.
func (a *Assemblers) readSpans(ctx context.Context, subject string, upTo uint64) ([][]byte, error) {
	var batches [][]byte
	for seq := uint64(1); seq <= upTo; {
		msg, err := a.q.spans.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if msg.Sequence > upTo {
			break
		}
		batches = append(batches, msg.Data)
		seq = msg.Sequence + 1
	}
	return batches, nil
}

// publish queues the assembled trace. The message ID makes a repeat of the
// same assembly (after a crash between queueing and purging) a no-op.
func (a *Assemblers) publish(ctx context.Context, t trace.Trace, lastSeq uint64) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if limit := int(a.q.nc.MaxPayload()); limit > 0 && len(data) > limit && len(t.OTLP) > 0 {
		// Too big to queue with its spans. Score it anyway; if kept it is
		// exported as rebuilt spans instead of the originals.
		a.q.logger.Warn("trace exceeds the NATS payload limit, queued without its original spans",
			"trace_id", t.TraceID, "bytes", len(data), "max_payload", limit)
		t.OTLP = nil
		if data, err = json.Marshal(t); err != nil {
			return err
		}
	}
	return a.q.publishTrace(ctx, t.TraceID, data, jetstream.WithMsgID(fmt.Sprintf("%s:%d", t.TraceID, lastSeq)))
}

func (a *Assemblers) ack(marker jetstream.Msg, traceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if err := marker.DoubleAck(ctx); err != nil {
		a.q.logger.Error("assembler ack failed", "trace_id", traceID, "error", err.Error())
	}
}
