package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

func newRecorder(t *testing.T) (*Recorder, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	r, err := New(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter(MeterName))
	if err != nil {
		t.Fatal(err)
	}
	return r, reader
}

// collect returns every data point's attributes by instrument name, with
// histogram counts and counter values as the point's value.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string][]point {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]point{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], point{dp.Attributes, int64(dp.Count)})
				}
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], point{dp.Attributes, dp.Value})
				}
			}
		}
	}
	return out
}

type point struct {
	attrs attribute.Set
	value int64
}

func (p point) get(key string) string {
	v, _ := p.attrs.Value(attribute.Key(key))
	return v.Emit()
}

func TestRecorderInstruments(t *testing.T) {
	r, reader := newRecorder(t)
	ctx := context.Background()
	r.RequestFinished(ctx, Request{Server: "api", Protocol: ProtocolHTTP, Method: "POST", Route: "/v1/traces", Status: 202, Duration: time.Millisecond})
	r.RequestFinished(ctx, Request{Server: "api", Protocol: ProtocolHTTP, Method: "POST", Route: "/v1/traces", Status: 503, Duration: time.Millisecond})
	r.RequestFinished(ctx, Request{Server: "otlp", Protocol: ProtocolGRPC, Method: "/x.TraceService/Export", Status: 14, Duration: time.Millisecond})
	r.ScoreFinished(ctx, pipeline.ScoreResult{Agent: "support-bot", Model: "kev-support", Duration: time.Second})
	r.ScoreFinished(ctx, pipeline.ScoreResult{Agent: "support-bot", Model: "kev-support", Err: fault.Timeout(fault.Scorer, "score", errors.New("slow"))})
	r.TraceDecided(ctx, pipeline.Record{Trace: trace.Trace{Agent: "support-bot"}, Model: "kev-support",
		Decision: pipeline.Decision{Keep: true, Reason: pipeline.ReasonModel}})
	r.SendFinished(ctx, Send{Exporter: "langfuse", Err: fault.Unavailable(fault.Export, "forward", errors.New("503"))})
	r.ErrorReported(fault.KindFull, fault.Queue)

	got := collect(t, reader)

	http := got["http.server.request.duration"]
	if len(http) != 2 {
		t.Fatalf("http points = %d, want one per status", len(http))
	}
	for _, p := range http {
		failed := p.get("http.response.status_code") == "503"
		if (p.get("error.type") == "503") != failed || p.get("http.route") != "/v1/traces" || p.get("observer.server") != "api" {
			t.Errorf("http point %v", p.attrs.ToSlice())
		}
	}
	if p := got["rpc.server.call.duration"]; len(p) != 1 || p[0].get("error.type") != "14" || p[0].get("rpc.method") != "/x.TraceService/Export" {
		t.Errorf("rpc points %+v", p)
	}

	scores := got["observer.scorer.duration"]
	var ok, failed int64
	for _, p := range scores {
		if p.get("observer.agent") != "support-bot" || p.get("observer.model") != "kev-support" {
			t.Errorf("score point %v", p.attrs.ToSlice())
		}
		if p.get("error.type") == "timeout" {
			failed += p.value
		} else {
			ok += p.value
		}
	}
	if ok != 1 || failed != 1 {
		t.Errorf("scores: %d ok, %d failed; want 1 and 1", ok, failed)
	}

	if p := got["observer.decisions"]; len(p) != 1 || p[0].value != 1 || p[0].get("observer.decision") != "keep" || p[0].get("observer.reason") != "kev" {
		t.Errorf("decision points %+v", p)
	}
	if p := got["observer.export.duration"]; len(p) != 1 || p[0].get("observer.exporter") != "langfuse" || p[0].get("error.type") != "unavailable" {
		t.Errorf("export points %+v", p)
	}
	if p := got["observer.errors"]; len(p) != 1 || p[0].get("observer.component") != "queue" || p[0].get("error.type") != "full" {
		t.Errorf("error points %+v", p)
	}
}

func TestStartWithoutEndpointRecordsNothing(t *testing.T) {
	p, err := Start(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	p.ScoreFinished(context.Background(), pipeline.ScoreResult{Duration: time.Second})
	if err := p.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

// Start sends OTLP metrics to the endpoint, with its own headers only.
func TestStartSendsOTLP(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-global=1")
	var mu sync.Mutex
	var paths []string
	var auth, global string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		paths = append(paths, r.URL.Path)
		auth, global = r.Header.Get("Authorization"), r.Header.Get("x-global")
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer srv.Close()

	p, err := Start(context.Background(), Config{
		Endpoint: srv.URL + "/v1/metrics", Headers: map[string]string{"Authorization": "Bearer abc"}, Interval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.ScoreFinished(context.Background(), pipeline.ScoreResult{Duration: time.Second})
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 || paths[0] != "/v1/metrics" {
		t.Fatalf("requests to %v, want /v1/metrics on shutdown", paths)
	}
	if auth != "Bearer abc" || global != "" {
		t.Errorf("headers: Authorization %q, x-global %q", auth, global)
	}
}
