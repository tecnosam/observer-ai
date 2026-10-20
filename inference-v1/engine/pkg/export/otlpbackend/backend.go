// Package otlpbackend is the export.AbstractBackend for any OTLP/HTTP
// endpoint: Phoenix, Langfuse, an OpenTelemetry Collector.
package otlpbackend

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/observer-ai/engine/pkg/export"
	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
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

// Backend sends kept traces to one OTLP/HTTP endpoint. A trace that
// arrived as OTLP spans is forwarded as those spans; any other trace is
// rebuilt as an agent span with one child per tool call. It is safe for
// concurrent use.
type Backend struct {
	tracer   oteltrace.Tracer
	flush    func(context.Context) error
	shutdown func(context.Context) error

	// forwarder and resource are set when original spans can be forwarded.
	forwarder Uploader
	resource  []*commonpb.KeyValue
}

// Option configures an Backend.
type Option func(*Backend)

// WithForwarder makes the backend forward traces that carry their original
// OTLP spans through u, with resourceAttrs set on their resources.
func WithForwarder(u Uploader, resourceAttrs map[string]string) Option {
	return func(b *Backend) { b.forwarder, b.resource = u, protoAttrs(resourceAttrs) }
}

// New returns a backend that creates spans on tp. If tp can flush and shut
// down (as the SDK's TracerProvider can), Flush and Shutdown use it.
func New(tp oteltrace.TracerProvider, opts ...Option) *Backend {
	b := &Backend{tracer: tp.Tracer(tracerName)}
	for _, opt := range opts {
		opt(b)
	}
	if f, ok := tp.(interface{ ForceFlush(context.Context) error }); ok {
		b.flush = f.ForceFlush
	}
	if s, ok := tp.(interface{ Shutdown(context.Context) error }); ok {
		b.shutdown = s.Shutdown
	}
	return b
}

// Open returns a backend that batches spans and sends them to
// cfg.Endpoint, a full OTLP/HTTP traces URL such as
// http://localhost:6006/v1/traces. It does not connect until the first
// export, so the backend may be down at start-up.
//
// Headers come from cfg only: the OTEL_EXPORTER_OTLP_HEADERS variable is
// not used, so one backend's credentials never reach another.
func Open(ctx context.Context, cfg export.BackendConfig) (*Backend, error) {
	exp, err := otlptracehttp.New(ctx, endpointOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("export: create OTLP exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(resourceOf(cfg)))

	// A second client forwards spans that arrived over OTLP. The SDK
	// pipeline above cannot: it only exports spans it created itself.
	forwarder := otlptracehttp.NewClient(append(endpointOptions(cfg),
		otlptracehttp.WithTimeout(forwardTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: time.Second,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  forwardMaxElapsed,
		}),
	)...)
	if err := forwarder.Start(ctx); err != nil {
		return nil, fmt.Errorf("export: start OTLP forwarder: %w", err)
	}

	b := New(tp, WithForwarder(forwarder, cfg.ResourceAttributes))
	stopProvider := b.shutdown
	b.shutdown = func(ctx context.Context) error {
		return errors.Join(stopProvider(ctx), forwarder.Stop(ctx))
	}
	return b, nil
}

// endpointOptions are the options both OTLP clients share. Headers are always
// set, even when empty, which replaces any from the environment.
func endpointOptions(cfg export.BackendConfig) []otlptracehttp.Option {
	headers := map[string]string{}
	for k, v := range cfg.Headers {
		headers[k] = v
	}
	return []otlptracehttp.Option{otlptracehttp.WithEndpointURL(cfg.Endpoint), otlptracehttp.WithHeaders(headers)}
}

// resourceOf is the resource of rebuilt spans: the engine's service name plus
// the configured attributes.
func resourceOf(cfg export.BackendConfig) *resource.Resource {
	attrs := []attribute.KeyValue{attribute.String("service.name", serviceName)}
	for k, v := range cfg.ResourceAttributes {
		attrs = append(attrs, attribute.String(k, v))
	}
	return resource.NewSchemaless(attrs...)
}

func protoAttrs(attrs map[string]string) []*commonpb.KeyValue {
	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, otlp.StringAttr(k, v))
	}
	return out
}

// ReportErrors routes OpenTelemetry's asynchronous errors, which is where
// failed batch exports surface, to reporter. A backend being unreachable then
// shows up as a reported error instead of going unnoticed.
func ReportErrors(reporter observe.Reporter) {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		reporter.Report(context.Background(), fault.Unavailable(fault.Export, "export spans", err))
	}))
}

// Send sends one kept trace. A trace carrying its original OTLP spans is
// forwarded as those spans, synchronously, so an error means the backend did
// not accept them. Any other trace is rebuilt as spans and handed to the
// provider's batch processor, which delivers it in the background.
func (b *Backend) Send(ctx context.Context, rec pipeline.Record) error {
	if len(rec.Trace.OTLP) > 0 && b.forwarder != nil {
		return b.forward(ctx, rec)
	}
	b.rebuild(ctx, rec)
	return nil
}

// rebuild creates an agent span with one child per tool call.
func (b *Backend) rebuild(ctx context.Context, rec pipeline.Record) {
	t, d := rec.Trace, rec.Decision

	attrs := []attribute.KeyValue{
		attribute.String(spanKindKey, spanKindAgent),
		attribute.String("input.value", t.Question),
		attribute.String("output.value", t.FinalAnswer),
		attribute.String("observer.source_trace_id", t.TraceID),
		attribute.String("eval.agent", string(t.Agent)),
		attribute.String("eval.decision", decisionKeep),
		attribute.String("eval.reason", string(d.Reason)),
		attribute.String("eval.severity", string(d.Severity)),
		attribute.String("eval.model", string(rec.Model)),
		attribute.Float64("eval.latency_ms", pipeline.Milliseconds(rec.Latency)),
	}
	if d.Scored {
		attrs = append(attrs,
			attribute.Float64("eval.p_keep", float64(d.PKeep)),
			attribute.Float64("eval.p_keep_raw", float64(d.PKeepRaw)),
		)
	}
	for i, ev := range t.Evidence {
		attrs = append(attrs, attribute.String(fmt.Sprintf(evidenceKeyFmt, i), ev))
	}

	// The source context is deliberately not used as parent: each kept
	// trace is its own trace in Phoenix.
	ctx, root := b.tracer.Start(ctx, rootSpanName, oteltrace.WithNewRoot(), oteltrace.WithAttributes(attrs...))
	for _, c := range t.ToolCalls {
		toolAttrs := []attribute.KeyValue{
			attribute.String(spanKindKey, spanKindTool),
			attribute.String("tool.name", c.Name),
			attribute.String("output.value", c.StatusText()),
		}
		if args := c.CompactArgs(); args != "" {
			toolAttrs = append(toolAttrs, attribute.String("input.value", args))
		}
		_, span := b.tracer.Start(ctx, c.Name, oteltrace.WithAttributes(toolAttrs...))
		span.End()
	}
	root.End()
}

// forward sends the trace's own spans on, unchanged except for the eval.*
// attributes on the root span and the configured resource attributes. The
// trace keeps its trace and span IDs.
func (b *Backend) forward(ctx context.Context, rec pipeline.Record) error {
	d := rec.Decision
	attrs := []*commonpb.KeyValue{
		otlp.StringAttr("eval.agent", string(rec.Trace.Agent)),
		otlp.StringAttr("eval.decision", decisionKeep),
		otlp.StringAttr("eval.reason", string(d.Reason)),
		otlp.StringAttr("eval.severity", string(d.Severity)),
		otlp.StringAttr("eval.model", string(rec.Model)),
		otlp.FloatAttr("eval.latency_ms", pipeline.Milliseconds(rec.Latency)),
	}
	if d.Scored {
		attrs = append(attrs,
			otlp.FloatAttr("eval.p_keep", float64(d.PKeep)),
			otlp.FloatAttr("eval.p_keep_raw", float64(d.PKeepRaw)),
		)
	}
	spans, err := otlp.Annotate(rec.Trace.OTLP, otlp.Annotation{Resource: b.resource, Root: attrs})
	if err != nil {
		return fault.Invalid(fault.Export, "annotate stored spans", err)
	}
	if err := b.forwarder.UploadTraces(ctx, spans); err != nil {
		return fault.Unavailable(fault.Export, "forward spans", err)
	}
	return nil
}

// Flush sends any buffered spans now.
func (b *Backend) Flush(ctx context.Context) error {
	if b.flush == nil {
		return nil
	}
	return b.flush(ctx)
}

// Shutdown flushes buffered spans and releases the backend.
func (b *Backend) Shutdown(ctx context.Context) error {
	if b.shutdown == nil {
		return nil
	}
	return b.shutdown(ctx)
}

// Factory builds an OTLP/HTTP back end for each configured exporter.
type Factory struct{}

func (Factory) Build(ctx context.Context, cfg export.BackendConfig) (export.AbstractBackend, error) {
	return Open(ctx, cfg)
}

var _ export.AbstractBackend = (*Backend)(nil)
