// Package export sends kept traces to Arize Phoenix as OpenInference spans
// over OTLP/HTTP.
package export

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

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
)

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
}

// New returns an Exporter that creates spans on tp. If tp can flush and shut
// down (as the SDK's TracerProvider can), Flush and Shutdown use it.
func New(tp oteltrace.TracerProvider) *Exporter {
	e := &Exporter{tracer: tp.Tracer(tracerName)}
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
func NewPhoenix(ctx context.Context, endpoint, project string) (*Exporter, error) {
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
	return New(tp), nil
}

// LogErrors routes OpenTelemetry's asynchronous errors, which is where
// failed batch exports surface, to logger. Phoenix being unreachable then
// shows up as a log line instead of going unnoticed.
func LogErrors(logger *slog.Logger) {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("trace export failed", "error", err.Error())
	}))
}

// Export emits the spans for a kept trace and reports whether it did.
// Dropped traces emit nothing. Spans are handed to the provider's span
// processor; with the batching provider, delivery happens in the background.
func (e *Exporter) Export(ctx context.Context, rec Record) bool {
	if !rec.Decision.Keep {
		return false
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
