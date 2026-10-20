package metrics

import (
	"context"
	"maps"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const serviceName = "observer-engine"

// Config is where metrics are sent. An empty Endpoint records nothing.
type Config struct {
	Endpoint           string            // the full OTLP/HTTP metrics URL
	Headers            map[string]string // sent with every request; the OTEL_* header variables are not read
	Interval           time.Duration
	ResourceAttributes map[string]string // besides service.name=observer-engine
}

// Pipeline is a Recorder and the provider that sends what it records.
type Pipeline struct {
	*Recorder
	shutdown func(context.Context) error
}

// Start builds a Recorder that sends its metrics every cfg.Interval over
// OTLP/HTTP, or, with no endpoint, one that records nothing.
func Start(ctx context.Context, cfg Config) (*Pipeline, error) {
	if cfg.Endpoint == "" {
		r, err := New(noop.NewMeterProvider().Meter(MeterName))
		return &Pipeline{Recorder: r}, err
	}
	exp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.Endpoint),
		otlpmetrichttp.WithHeaders(maps.Clone(cfg.Headers)))
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.Interval))),
		sdkmetric.WithResource(resourceOf(cfg.ResourceAttributes)))
	r, err := New(provider.Meter(MeterName))
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	return &Pipeline{Recorder: r, shutdown: provider.Shutdown}, nil
}

// Shutdown sends what is recorded and stops.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	if p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

func resourceOf(extra map[string]string) *resource.Resource {
	attrs := []attribute.KeyValue{attribute.String("service.name", serviceName)}
	for k, v := range extra {
		attrs = append(attrs, attribute.String(k, v))
	}
	return resource.NewSchemaless(attrs...)
}
