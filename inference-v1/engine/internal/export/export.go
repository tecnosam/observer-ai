// Package export sends kept traces to Arize Phoenix as OpenInference spans
// over OTLP/HTTP. A trace that arrived as OTLP spans is forwarded as those
// spans; a trace from the JSON API is rebuilt as an agent_run span with one
// child per tool call.
package export

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/observer-ai/engine/internal/otlp"
	"github.com/observer-ai/engine/internal/policy"
	"github.com/observer-ai/engine/internal/trace"
)

const (
	serviceName    = "observer-engine"
	tracerName     = "github.com/observer-ai/engine"
	rootSpanName   = "agent_run"
	spanKindKey    = "openinference.span.kind"
	spanKindAgent  = "AGENT"
	spanKindTool   = "TOOL"
	decisionKeep   = "keep"
	evidenceKeyFmt = "observer.evidence.%d"

	// A forward runs inside a queue worker, so it has to give up well
	// within the queue's ack_wait.
	forwardTimeout    = 10 * time.Second
	forwardMaxElapsed = 15 * time.Second
)

// Uploader sends OTLP spans to a backend. The OTLP/HTTP client implements it.
type Uploader interface {
	UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error
}

// Record is one decided trace.
type Record struct {
	Trace    trace.Trace
	Decision policy.Decision
	// Model is the model that scored the trace.
	Model string
	// LatencyMS is the scoring latency.
	LatencyMS float64
}

// Exporter writes kept traces as spans. It is safe for concurrent use.
type Exporter struct {
	tracer   oteltrace.Tracer
	flush    func(context.Context) error
	shutdown func(context.Context) error

	// forwarder and project are set when original spans can be forwarded.
	forwarder Uploader
	project   string
	logger    *slog.Logger
}

// Option configures an Exporter.
type Option func(*Exporter)

// WithForwarder makes the Exporter forward traces that carry their original
// OTLP spans through u, filed under the given Phoenix project.
func WithForwarder(u Uploader, project string) Option {
	return func(e *Exporter) { e.forwarder, e.project = u, project }
}

// WithLogger sets where forwarding failures are logged.
func WithLogger(l *slog.Logger) Option {
	return func(e *Exporter) { e.logger = l }
}

// New returns an Exporter that creates spans on tp. If tp can flush and shut
// down (as the SDK's TracerProvider can), Flush and Shutdown use it.
func New(tp oteltrace.TracerProvider, opts ...Option) *Exporter {
	e := &Exporter{tracer: tp.Tracer(tracerName), logger: slog.Default()}
	for _, opt := range opts {
		opt(e)
	}
	if f, ok := tp.(interface{ ForceFlush(context.Context) error }); ok {
		e.flush = f.ForceFlush
	}
	if s, ok := tp.(interface{ Shutdown(context.Context) error }); ok {
		e.shutdown = s.Shutdown
	}
	return e
}

// NewPhoenix returns an Exporter that batches spans and sends them to the
// Phoenix OTLP/HTTP endpoint (a full URL, e.g. http://localhost:6006/v1/traces)
// under the given Phoenix project. It does not connect until the first
// export, so Phoenix may be down at start-up.
func NewPhoenix(ctx context.Context, endpoint, project string, logger *slog.Logger) (*Exporter, error) {
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("export: create OTLP exporter: %w", err)
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", serviceName),
		attribute.String("openinference.project.name", project),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	// A second client forwards spans that arrived over OTLP. The SDK
	// pipeline above cannot: it only exports spans it created itself.
	forwarder := otlptracehttp.NewClient(
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(forwardTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: time.Second,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  forwardMaxElapsed,
		}),
	)
	if err := forwarder.Start(ctx); err != nil {
		return nil, fmt.Errorf("export: start OTLP forwarder: %w", err)
	}

	e := New(tp, WithForwarder(forwarder, project), WithLogger(logger))
	stopProvider := e.shutdown
	e.shutdown = func(ctx context.Context) error {
		return errors.Join(stopProvider(ctx), forwarder.Stop(ctx))
	}
	return e, nil
}

// LogErrors routes OpenTelemetry's asynchronous errors, which is where
// failed batch exports surface, to logger. Phoenix being unreachable then
// shows up as a log line instead of going unnoticed.
func LogErrors(logger *slog.Logger) {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("trace export failed", "error", err.Error())
	}))
}

// Export sends a kept trace to Phoenix and reports whether it did. Dropped
// traces send nothing.
//
// A trace carrying its original OTLP spans is forwarded as those spans,
// synchronously, so the result reflects whether Phoenix accepted them. Any
// other trace is rebuilt as spans and handed to the provider's span
// processor; with the batching provider, delivery happens in the background.
func (e *Exporter) Export(ctx context.Context, rec Record) bool {
	if !rec.Decision.Keep {
		return false
	}
	if len(rec.Trace.OTLP) > 0 && e.forwarder != nil {
		return e.forward(ctx, rec)
	}
	t, d := rec.Trace, rec.Decision

	attrs := []attribute.KeyValue{
		attribute.String(spanKindKey, spanKindAgent),
		attribute.String("input.value", t.Question),
		attribute.String("output.value", t.FinalAnswer),
		attribute.String("observer.source_trace_id", t.TraceID),
		attribute.String("eval.decision", decisionKeep),
		attribute.String("eval.reason", d.Reason),
		attribute.String("eval.severity", d.Severity),
		attribute.String("eval.model", rec.Model),
		attribute.Float64("eval.latency_ms", rec.LatencyMS),
	}
	if d.Scored {
		attrs = append(attrs,
			attribute.Float64("eval.p_keep", d.PKeep),
			attribute.Float64("eval.p_keep_raw", d.PKeepRaw),
		)
	}
	for i, ev := range t.Evidence {
		attrs = append(attrs, attribute.String(fmt.Sprintf(evidenceKeyFmt, i), ev))
	}

	// The source context is deliberately not used as parent: each kept
	// trace is its own trace in Phoenix.
	ctx, root := e.tracer.Start(ctx, rootSpanName, oteltrace.WithNewRoot(), oteltrace.WithAttributes(attrs...))
	for _, c := range t.ToolCalls {
		toolAttrs := []attribute.KeyValue{
			attribute.String(spanKindKey, spanKindTool),
			attribute.String("tool.name", c.Name),
			attribute.String("output.value", c.StatusText()),
		}
		if args := c.CompactArgs(); args != "" {
			toolAttrs = append(toolAttrs, attribute.String("input.value", args))
		}
		_, span := e.tracer.Start(ctx, c.Name, oteltrace.WithAttributes(toolAttrs...))
		span.End()
	}
	root.End()
	return true
}

// forward sends the trace's own spans on, unchanged except for the eval.*
// attributes on the root span and the Phoenix project on the resource. The
// trace keeps its trace and span IDs.
func (e *Exporter) forward(ctx context.Context, rec Record) bool {
	d := rec.Decision
	attrs := []*commonpb.KeyValue{
		otlp.StringAttr("eval.decision", decisionKeep),
		otlp.StringAttr("eval.reason", d.Reason),
		otlp.StringAttr("eval.severity", d.Severity),
		otlp.StringAttr("eval.model", rec.Model),
		otlp.FloatAttr("eval.latency_ms", rec.LatencyMS),
	}
	if d.Scored {
		attrs = append(attrs,
			otlp.FloatAttr("eval.p_keep", d.PKeep),
			otlp.FloatAttr("eval.p_keep_raw", d.PKeepRaw),
		)
	}
	spans, err := otlp.Annotate(rec.Trace.OTLP, e.project, attrs)
	if err == nil {
		err = e.forwarder.UploadTraces(ctx, spans)
	}
	if err != nil {
		e.logger.Error("trace export failed", "trace_id", rec.Trace.TraceID, "error", err.Error())
		return false
	}
	return true
}

// Flush sends any buffered spans now.
func (e *Exporter) Flush(ctx context.Context) error {
	if e.flush == nil {
		return nil
	}
	return e.flush(ctx)
}

// Shutdown flushes buffered spans and releases the exporter.
func (e *Exporter) Shutdown(ctx context.Context) error {
	if e.shutdown == nil {
		return nil
	}
	return e.shutdown(ctx)
}
