package scorer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const questions = `{"answer_wrong":{"type":"noul","instructions":"Is the final answer wrong?"},"answers_question":{"type":"score","instructions":"How good?","criteria":["Wrong","Fully correct"]}}`

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../../testdata/kev", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// hang blocks a fake-Kev handler until the client gives up. The body must be
// read first: net/http only notices a dropped connection after that.
func hang(r *http.Request) {
	io.Copy(io.Discard, r.Body)
	select {
	case <-r.Context().Done():
	case <-time.After(10 * time.Second):
	}
}

// sleeps records backoff delays instead of waiting.
type sleeps struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delays = append(s.delays, d)
	return nil
}

func newClient(t *testing.T, endpoint string, mutate func(*Config)) (*Client, *sleeps) {
	t.Helper()
	rec := &sleeps{}
	cfg := Config{
		Endpoint:         endpoint,
		Model:            "kev-latest",
		Questions:        json.RawMessage(questions),
		DecisionQuestion: "answer_wrong",
		Timeout:          2 * time.Second,
		MaxRetries:       3,
		Backoff:          []time.Duration{500 * time.Millisecond, 2 * time.Second, 5 * time.Second},
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Sleep:            rec.sleep,
		Jitter:           func() float64 { return 0.5 },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

func TestScoreSuccess(t *testing.T) {
	var got struct {
		Model     string          `json:"model"`
		State     string          `json:"state"`
		Questions json.RawMessage `json:"questions"`
	}
	var method, path, contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, contentType = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture(t, "success.json"))
	}))
	defer srv.Close()

	c, rec := newClient(t, srv.URL+"/v1/systemone", nil)
	res, err := c.Score(context.Background(), "USER QUESTION:\nq")
	if err != nil {
		t.Fatal(err)
	}
	if res.PRaw != 0.93 || res.Model != "kev-latest" || res.LatencyMS != 93 || res.Attempts != 1 {
		t.Errorf("result = %+v", res)
	}
	if method != http.MethodPost || path != "/v1/systemone" || contentType != "application/json" {
		t.Errorf("request = %s %s (%s)", method, path, contentType)
	}
	if got.Model != "kev-latest" || got.State != "USER QUESTION:\nq" {
		t.Errorf("request body = %+v", got)
	}
	// Questions go out exactly as configured, in the same key order.
	if string(got.Questions) != questions {
		t.Errorf("questions = %s\nwant %s", got.Questions, questions)
	}
	if len(rec.delays) != 0 {
		t.Errorf("slept %v on a first-try success", rec.delays)
	}
}

func TestScoreJevCompatibleLatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "success_jev.json"))
	}))
	defer srv.Close()

	c, _ := newClient(t, srv.URL, nil)
	res, err := c.Score(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if res.PRaw != 0.71 || res.LatencyMS != 57.25 || res.Model != "jev-latest" {
		t.Errorf("result = %+v", res)
	}
}

func TestScoreMeasuresLatencyWhenServerReportsNone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"answers":{"answer_wrong":{"type":"noul","noul":0.4}}}`))
	}))
	defer srv.Close()

	c, _ := newClient(t, srv.URL, nil)
	res, err := c.Score(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if res.LatencyMS <= 0 {
		t.Errorf("latency = %v, want a measured value", res.LatencyMS)
	}
	if res.Model != "kev-latest" {
		t.Errorf("model = %q, want the configured model as fallback", res.Model)
	}
}

func TestScoreTimeoutIsRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			hang(r)
			return
		}
		w.Write(fixture(t, "success.json"))
	}))
	defer srv.Close()

	c, rec := newClient(t, srv.URL, func(cfg *Config) { cfg.Timeout = 100 * time.Millisecond })
	res, err := c.Score(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if res.Attempts != 2 || res.PRaw != 0.93 {
		t.Errorf("result = %+v, want success on attempt 2", res)
	}
	if len(rec.delays) != 1 {
		t.Errorf("delays = %v, want one backoff", rec.delays)
	}
}

func TestScoreTimeoutEveryAttempt(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		hang(r)
	}))
	defer srv.Close()

	c, _ := newClient(t, srv.URL, func(cfg *Config) {
		cfg.Timeout = 50 * time.Millisecond
		cfg.MaxRetries = 1
	})
	_, err := c.Score(context.Background(), "s")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want a deadline error", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestScore5xxThenSuccess(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "model loading", http.StatusServiceUnavailable)
			return
		}
		w.Write(fixture(t, "success.json"))
	}))
	defer srv.Close()

	c, rec := newClient(t, srv.URL, nil)
	res, err := c.Score(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if res.Attempts != 3 || calls.Load() != 3 {
		t.Errorf("attempts = %d, calls = %d, want 3", res.Attempts, calls.Load())
	}
	// Jitter 0.5 gives 75% of each base delay: 500ms and 2s.
	want := []time.Duration{375 * time.Millisecond, 1500 * time.Millisecond}
	if len(rec.delays) != 2 || rec.delays[0] != want[0] || rec.delays[1] != want[1] {
		t.Errorf("delays = %v, want %v", rec.delays, want)
	}
}

func TestScore4xxIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write(fixture(t, "error_422.json"))
	}))
	defer srv.Close()

	c, rec := newClient(t, srv.URL, nil)
	res, err := c.Score(context.Background(), "s")
	var status *StatusError
	if !errors.As(err, &status) || status.Code != http.StatusUnprocessableEntity {
		t.Fatalf("error = %v, want a 422 StatusError", err)
	}
	if !strings.Contains(status.Body, "Field required") {
		t.Errorf("error body = %q, want the server's message", status.Body)
	}
	if calls.Load() != 1 || res.Attempts != 1 || len(rec.delays) != 0 {
		t.Errorf("calls = %d, attempts = %d, delays = %v; want a single attempt", calls.Load(), res.Attempts, rec.delays)
	}
}

func TestScoreAllRetriesFail(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, rec := newClient(t, srv.URL, func(cfg *Config) { cfg.MaxRetries = 4 })
	res, err := c.Score(context.Background(), "s")
	if err == nil {
		t.Fatal("expected an error")
	}
	var status *StatusError
	if !errors.As(err, &status) || status.Code != 500 {
		t.Errorf("error = %v, want it to wrap the last 500", err)
	}
	if !strings.Contains(err.Error(), "giving up after 5 attempts") {
		t.Errorf("error = %v", err)
	}
	if calls.Load() != 5 || res.Attempts != 5 {
		t.Errorf("calls = %d, attempts = %d, want 5 (1 + 4 retries)", calls.Load(), res.Attempts)
	}
	// The last backoff entry repeats once the list runs out.
	want := []time.Duration{375 * time.Millisecond, 1500 * time.Millisecond, 3750 * time.Millisecond, 3750 * time.Millisecond}
	if len(rec.delays) != len(want) {
		t.Fatalf("delays = %v, want %v", rec.delays, want)
	}
	for i := range want {
		if rec.delays[i] != want[i] {
			t.Errorf("delay[%d] = %v, want %v", i, rec.delays[i], want[i])
		}
	}
}

func TestScoreConnectionErrorIsRetried(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens here any more

	c, rec := newClient(t, url, func(cfg *Config) { cfg.MaxRetries = 2 })
	res, err := c.Score(context.Background(), "s")
	if err == nil {
		t.Fatal("expected an error")
	}
	if res.Attempts != 3 || len(rec.delays) != 2 {
		t.Errorf("attempts = %d, delays = %v, want 3 attempts", res.Attempts, rec.delays)
	}
}

func TestScoreMalformedResponsesAreNotRetried(t *testing.T) {
	tests := []struct{ name, body, wantErr string }{
		{"not JSON", `<html>`, "decode kev response"},
		{"missing answer", string(fixture(t, "missing_answer.json")), `no answer for "answer_wrong"`},
		{"no noul value", `{"answers":{"answer_wrong":{"type":"score","score":1.2}}}`, "no noul value"},
		{"out of range", `{"answers":{"answer_wrong":{"type":"noul","noul":1.7}}}`, "outside [0, 1]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			c, _ := newClient(t, srv.URL, nil)
			_, err := c.Score(context.Background(), "s")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if calls.Load() != 1 {
				t.Errorf("calls = %d, want 1", calls.Load())
			}
		})
	}
}

func TestScoreStopsWhenCallerCancels(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		cancel()
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, _ := newClient(t, srv.URL, nil)
	_, err := c.Score(ctx, "s")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want no retries after cancellation", calls.Load())
	}
}

func TestBackoffJitterRange(t *testing.T) {
	for _, j := range []float64{0, 0.999999} {
		c, _ := newClient(t, "http://kev.invalid", func(cfg *Config) { cfg.Jitter = func() float64 { return j } })
		for retry, base := range []time.Duration{500 * time.Millisecond, 2 * time.Second, 5 * time.Second} {
			if d := c.backoff(retry); d < base/2 || d > base {
				t.Errorf("jitter %v retry %d: delay %v outside [%v, %v]", j, retry, d, base/2, base)
			}
		}
	}
	// Without a configured list the delay doubles from 500ms.
	c, _ := newClient(t, "http://kev.invalid", func(cfg *Config) {
		cfg.Backoff = nil
		cfg.Jitter = func() float64 { return 0.999999 }
	})
	if a, b := c.backoff(0), c.backoff(2); b < 3*a {
		t.Errorf("default backoff does not grow: %v then %v", a, b)
	}
}

func TestReady(t *testing.T) {
	var path string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(status)
	}))
	c, _ := newClient(t, srv.URL+"/v1/systemone", nil)

	if err := c.Ready(context.Background()); err != nil {
		t.Errorf("Ready = %v, want nil", err)
	}
	if path != "/v1/models" {
		t.Errorf("probe path = %q, want /v1/models", path)
	}

	status = http.StatusNotFound // up, just no such route
	if err := c.Ready(context.Background()); err != nil {
		t.Errorf("Ready on 404 = %v, want nil", err)
	}

	status = http.StatusBadGateway
	if err := c.Ready(context.Background()); err == nil {
		t.Error("Ready on 502 = nil, want an error")
	}

	srv.Close()
	if err := c.Ready(context.Background()); err == nil {
		t.Error("Ready with the server down = nil, want an error")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	base := Config{Endpoint: "http://kev:1/v1/systemone", Questions: json.RawMessage(questions), DecisionQuestion: "answer_wrong"}

	bad := base
	bad.Endpoint = "not a url"
	if _, err := New(bad); err == nil {
		t.Error("expected an error for a bad endpoint")
	}
	bad = base
	bad.Questions = json.RawMessage(`{`)
	if _, err := New(bad); err == nil {
		t.Error("expected an error for invalid questions")
	}
	bad = base
	bad.DecisionQuestion = ""
	if _, err := New(bad); err == nil {
		t.Error("expected an error for a missing decision question")
	}
}
