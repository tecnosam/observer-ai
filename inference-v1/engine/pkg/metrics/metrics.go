// Package metrics records the engine's own OpenTelemetry metrics:
// latencies and error rates of the servers, the model calls and the
// exporters, plus decisions and reported errors. Recorder implements every
// metrics hook the other packages declare (pipeline.Metrics,
// observe.ErrorCounter, and the request and send hooks).
//
// Instruments, all durations in seconds:
//
//	http.server.request.duration  histogram  JSON API and OTLP/HTTP requests
//	rpc.server.call.duration      histogram  OTLP/gRPC calls
//	observer.scorer.duration      histogram  model calls, retries included
//	observer.export.duration      histogram  sends to a trace back end
//	observer.decisions            counter    decided traces
//	observer.errors               counter    errors reported to the error pipeline
//
// A failed call carries error.type (its fault kind, or the status code), so
// an error rate is the count with error.type over the total count.
package metrics

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
)

// MeterName is the instrumentation scope of every instrument.
const MeterName = "github.com/observer-ai/engine"

// Protocols a server speaks, in Request.Protocol.
const (
	ProtocolHTTP = "http"
	ProtocolGRPC = "grpc"
)

// latencyBuckets suit calls from a millisecond to a minute.
var latencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Request is one request a server answered.
type Request struct {
	Server   string // "api" or "otlp"
	Protocol string // ProtocolHTTP or ProtocolGRPC
	Method   string // the HTTP method, or the gRPC full method name
	Route    string // the HTTP route pattern; empty for gRPC
	Status   int    // the HTTP status, or the gRPC status code
	Duration time.Duration
}

// Send is one attempt to send a kept trace to a back end.
type Send struct {
	Exporter string
	Duration time.Duration
	Err      error // nil on success
}

// Recorder records on one meter. Build it with New, or Start to also send
// what it records.
type Recorder struct {
	httpDuration   metric.Float64Histogram
	rpcDuration    metric.Float64Histogram
	scoreDuration  metric.Float64Histogram
	exportDuration metric.Float64Histogram
	decisions      metric.Int64Counter
	errors         metric.Int64Counter
}

// New creates the instruments on meter.
func New(meter metric.Meter) (*Recorder, error) {
	b := builder{meter: meter}
	r := &Recorder{
		httpDuration:   b.histogram("http.server.request.duration", "Duration of HTTP requests the engine answered."),
		rpcDuration:    b.histogram("rpc.server.call.duration", "Duration of gRPC calls the engine answered."),
		scoreDuration:  b.histogram("observer.scorer.duration", "Duration of calls to a decision model, retries included."),
		exportDuration: b.histogram("observer.export.duration", "Duration of sending a kept trace to a back end."),
		decisions:      b.counter("observer.decisions", "{trace}", "Traces decided, by decision and reason."),
		errors:         b.counter("observer.errors", "{error}", "Errors reported to the error pipeline."),
	}
	return r, b.err
}

func (r *Recorder) RequestFinished(ctx context.Context, req Request) {
	if req.Protocol == ProtocolGRPC {
		r.rpcDuration.Record(ctx, req.Duration.Seconds(), metric.WithAttributes(grpcAttrs(req)...))
		return
	}
	r.httpDuration.Record(ctx, req.Duration.Seconds(), metric.WithAttributes(httpAttrs(req)...))
}

func (r *Recorder) ScoreFinished(ctx context.Context, res pipeline.ScoreResult) {
	attrs := append(modelAttrs(string(res.Agent), string(res.Model)), errorAttrs(res.Err)...)
	r.scoreDuration.Record(ctx, res.Duration.Seconds(), metric.WithAttributes(attrs...))
}

func (r *Recorder) TraceDecided(ctx context.Context, rec pipeline.Record) {
	attrs := append(modelAttrs(string(rec.Trace.Agent), string(rec.Model)),
		attribute.String("observer.decision", rec.Decision.Label()),
		attribute.String("observer.reason", string(rec.Decision.Reason)))
	r.decisions.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (r *Recorder) SendFinished(ctx context.Context, s Send) {
	attrs := append([]attribute.KeyValue{attribute.String("observer.exporter", s.Exporter)}, errorAttrs(s.Err)...)
	r.exportDuration.Record(ctx, s.Duration.Seconds(), metric.WithAttributes(attrs...))
}

func (r *Recorder) ErrorReported(kind fault.Kind, component fault.Component) {
	r.errors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("observer.component", string(component)),
		attribute.String("error.type", kind.String())))
}

func httpAttrs(req Request) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("observer.server", req.Server),
		attribute.String("http.request.method", req.Method),
		attribute.String("http.route", req.Route),
		attribute.Int("http.response.status_code", req.Status),
	}
	if req.Status >= 500 {
		attrs = append(attrs, attribute.String("error.type", strconv.Itoa(req.Status)))
	}
	return attrs
}

// grpcAttrs: status 0 is OK; anything else is an error.
func grpcAttrs(req Request) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("observer.server", req.Server),
		attribute.String("rpc.system", "grpc"),
		attribute.String("rpc.method", req.Method),
		attribute.Int("rpc.grpc.status_code", req.Status),
	}
	if req.Status != 0 {
		attrs = append(attrs, attribute.String("error.type", strconv.Itoa(req.Status)))
	}
	return attrs
}

func modelAttrs(agent, model string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("observer.agent", agent), attribute.String("observer.model", model)}
}

// errorAttrs is error.type set to the fault kind, or nothing on success.
func errorAttrs(err error) []attribute.KeyValue {
	if err == nil {
		return nil
	}
	return []attribute.KeyValue{attribute.String("error.type", fault.KindOf(err).String())}
}

// builder creates instruments and keeps the first error.
type builder struct {
	meter metric.Meter
	err   error
}

func (b *builder) histogram(name, desc string) metric.Float64Histogram {
	h, err := b.meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	b.keep(err)
	return h
}

func (b *builder) counter(name, unit, desc string) metric.Int64Counter {
	c, err := b.meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
	b.keep(err)
	return c
}

func (b *builder) keep(err error) {
	if b.err == nil {
		b.err = err
	}
}
