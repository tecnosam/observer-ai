package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/observer-ai/engine/internal/fault"
)

var fixed = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

type counter struct{ seen []string }

func (c *counter) ErrorReported(k fault.Kind, comp fault.Component) {
	c.seen = append(c.seen, string(comp)+"/"+k.String())
}

type failingSink struct{}

func (failingSink) Write(Entry) error { return errors.New("disk full") }
func (failingSink) Close() error      { return nil }

func TestPipelineWritesCountsAndSurvivesABrokenSink(t *testing.T) {
	var out, fallback bytes.Buffer
	stdout, _ := DefaultSinkFactory{Stdout: &out}.Build(SinkConfig{Kind: SinkStdout})
	c := &counter{}
	p := NewPipeline(PipelineConfig{Sinks: []AbstractSink{failingSink{}, stdout}, Counter: c, Fallback: &fallback,
		Now: func() time.Time { return fixed }})

	p.Report(context.Background(), fault.Unavailable(fault.Queue, "nak trace", errors.New("nats: timeout")).WithTrace("t1").With("delivery", 2))

	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("not one JSON line: %q", out.String())
	}
	want := map[string]any{"time": "2026-10-20T12:00:00Z", "level": "WARN", "msg": "nak trace", "kind": "unavailable",
		"component": "queue", "trace_id": "t1", "error": "nats: timeout", "delivery": float64(2)}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if len(c.seen) != 1 || c.seen[0] != "queue/unavailable" {
		t.Errorf("counted %v", c.seen)
	}
	if !strings.Contains(fallback.String(), "disk full") {
		t.Errorf("the failing sink was not noticed: %q", fallback.String())
	}
}

func TestLevels(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want Level
	}{
		{fault.Rejected(fault.Scorer, "score", nil), LevelWarn},
		{fault.Unavailable(fault.Queue, "publish", nil).AsSevere(), LevelError},
		{fault.Internal(fault.Pipeline, "decide", nil), LevelError},
		{errors.New("not ours"), LevelError},
	} {
		if got := entryFor(tt.err, fixed).Level; got != tt.want {
			t.Errorf("%v: level %s, want %s", tt.err, got, tt.want)
		}
	}
}

func TestFileSinkAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "errors.jsonl")
	p, err := BuildPipeline(DefaultSinkFactory{}, []SinkConfig{{Kind: SinkFile, Path: path}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Report(context.Background(), fault.Invalid(fault.OTLP, "store spans", nil))
	p.Report(context.Background(), fault.Invalid(fault.OTLP, "store spans", nil))
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if n := strings.Count(string(data), "\n"); n != 2 {
		t.Errorf("%d lines, want 2: %s", n, data)
	}
}

func TestUnknownSinkIsAConfigError(t *testing.T) {
	_, err := BuildPipeline(DefaultSinkFactory{}, []SinkConfig{{Kind: "datadog"}}, nil)
	if fault.KindOf(err) != fault.KindInvalid {
		t.Errorf("error = %v", err)
	}
}
