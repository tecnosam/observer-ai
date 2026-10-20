package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/trace"
)

// stubScorer answers every trace with raw, or fails with err.
type stubScorer struct {
	raw      Probability
	err      error
	readyErr error
	mu       sync.Mutex
	calls    int
}

func (s *stubScorer) Score(context.Context, trace.State) (Score, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	if s.err != nil {
		return Score{}, s.err
	}
	return Score{Raw: s.raw, Model: "reported"}, nil
}

func (s *stubScorer) Ready(context.Context) error { return s.readyErr }

// threshold keeps at 0.5 and records the calibrated probability it saw.
type threshold struct{ seen Probability }

func (p *threshold) Decide(raw, calibrated Probability) Decision {
	p.seen = calibrated
	return Decision{Keep: calibrated >= 0.5, Scored: true, PKeep: calibrated, PKeepRaw: raw, Reason: ReasonModel}
}
func (p *threshold) Fallback() Decision { return Decision{Reason: ReasonFallback} }

type nowhere struct{}

func (nowhere) Export(context.Context, Record) bool { return true }
func (nowhere) Flush(context.Context) error         { return nil }

type recordingMetrics struct {
	mu      sync.Mutex
	scores  []ScoreResult
	records []Record
}

func (m *recordingMetrics) ScoreFinished(_ context.Context, r ScoreResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scores = append(m.scores, r)
}

func (m *recordingMetrics) TraceDecided(_ context.Context, rec Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, rec)
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func sampleTrace(agent trace.AgentName) trace.Trace {
	return trace.Trace{TraceID: "t1", Agent: agent, Question: "q", FinalAnswer: "a"}
}

func TestAgentRouterRoutes(t *testing.T) {
	def := Model{Name: "default"}
	support := Model{Name: "support"}
	r := NewAgentRouter(def, map[trace.AgentName]Model{"support-bot": support})
	for agent, want := range map[trace.AgentName]ModelName{"support-bot": "support", "other": "default", "": "default"} {
		if got := r.Route(agent).Name; got != want {
			t.Errorf("Route(%q) = %s, want %s", agent, got, want)
		}
	}
}

// Readiness follows the default model only: one agent's model being down
// must not take the whole engine out of a load balancer.
func TestAgentRouterReadyChecksTheDefaultOnly(t *testing.T) {
	agentDown := NewAgentRouter(Model{Name: "default", Scorer: &stubScorer{}},
		map[trace.AgentName]Model{"support-bot": {Name: "support", Scorer: &stubScorer{readyErr: errors.New("refused")}}})
	if err := agentDown.Ready(context.Background()); err != nil {
		t.Errorf("an agent's model down: Ready = %v, want nil", err)
	}

	defaultDown := NewAgentRouter(Model{Name: "default", Scorer: &stubScorer{readyErr: errors.New("refused")}}, nil)
	err := defaultDown.Ready(context.Background())
	if err == nil || !strings.Contains(err.Error(), "model default (default): refused") {
		t.Errorf("default down: Ready = %v", err)
	}
}

// Each trace is scored by its agent's model and calibrated with that
// model's temperature.
func TestEngineScoresWithTheAgentsModel(t *testing.T) {
	def := &stubScorer{raw: 0.6}
	support := &stubScorer{raw: 0.6}
	policy := &threshold{}
	m := &recordingMetrics{}
	e := NewEngine(Deps{
		Models: NewAgentRouter(Model{Name: "default", Scorer: def, Temperature: 1},
			map[trace.AgentName]Model{"support-bot": {Name: "support", Scorer: support, Temperature: 4}}),
		Policy: policy, Exporter: nowhere{}, Metrics: m, Logger: quiet,
	})

	e.Process(context.Background(), sampleTrace("support-bot"))
	if support.calls != 1 || def.calls != 0 {
		t.Fatalf("calls: support %d, default %d", support.calls, def.calls)
	}
	if want := Calibrate(0.6, 4); policy.seen != want {
		t.Errorf("calibrated with %v, want the agent model's temperature (%v)", policy.seen, want)
	}

	e.Process(context.Background(), sampleTrace("unknown"))
	if def.calls != 1 || policy.seen != 0.6 {
		t.Errorf("unknown agent: default calls %d, calibrated %v", def.calls, policy.seen)
	}

	if len(m.scores) != 2 || m.scores[0].Agent != "support-bot" || m.scores[0].Model != "support" || m.scores[0].Err != nil {
		t.Errorf("score metrics = %+v", m.scores)
	}
	if len(m.records) != 2 || m.records[0].Trace.Agent != "support-bot" {
		t.Errorf("decision metrics = %+v", m.records)
	}
}

// A fallback is labelled with the model that failed, and the failure is
// recorded with its error.
func TestEngineFallbackNamesTheAgentsModel(t *testing.T) {
	down := &stubScorer{err: fault.Unavailable(fault.Scorer, "score trace", errors.New("refused"))}
	m := &recordingMetrics{}
	e := NewEngine(Deps{
		Models: NewAgentRouter(Model{Name: "default", Scorer: &stubScorer{}},
			map[trace.AgentName]Model{"support-bot": {Name: "support", Scorer: down}}),
		Policy: &threshold{}, Exporter: nowhere{}, Metrics: m, Logger: quiet,
	})
	out := e.Process(context.Background(), sampleTrace("support-bot"))
	if out.Record.Model != "support" || out.Record.Decision.Reason != ReasonFallback {
		t.Errorf("record = %+v", out.Record)
	}
	if len(m.scores) != 1 || fault.KindOf(m.scores[0].Err) != fault.KindUnavailable {
		t.Errorf("score metrics = %+v", m.scores)
	}
}
