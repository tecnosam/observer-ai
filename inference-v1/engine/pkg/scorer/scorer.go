// Package scorer calls the Kev decision model over HTTP.
package scorer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"

	"github.com/observer-ai/engine/pkg/fault"
	"github.com/observer-ai/engine/pkg/observe"
	"github.com/observer-ai/engine/pkg/pipeline"
	"github.com/observer-ai/engine/pkg/trace"
)

const (
	maxResponseBytes = 1 << 20
	maxErrorBody     = 300
	probePath        = "/v1/models"
)

// Config configures a Client.
type Config struct {
	Endpoint string
	Model    string
	// Questions is the JSON object of questions sent with every request.
	Questions        json.RawMessage
	DecisionQuestion string
	// APIKey, when set, is sent as a bearer token with every request.
	APIKey string
	// Timeout applies to each attempt, not to the whole call.
	Timeout time.Duration
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries int
	// Backoff[i] is the base delay before retry i+1; the last entry repeats.
	Backoff []time.Duration

	HTTPClient *http.Client     // defaults to a new client
	Reporter   observe.Reporter // receives each failed attempt that is retried; defaults to observe.Discard

	// Sleep and Jitter exist so tests can run without waiting. Jitter
	// returns a value in [0, 1).
	Sleep  func(ctx context.Context, d time.Duration) error
	Jitter func() float64
}

// StatusError is a non-2xx response from Kev.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("kev returned HTTP %d: %s", e.Code, e.Body)
}

// Client scores states against Kev's /v1/systemone endpoint.
type Client struct {
	cfg      Config
	probeURL string
}

func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("scorer: invalid endpoint %q", cfg.Endpoint)
	}
	if !json.Valid(cfg.Questions) {
		return nil, errors.New("scorer: questions must be valid JSON")
	}
	if cfg.DecisionQuestion == "" {
		return nil, errors.New("scorer: decision question is required")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	if cfg.Reporter == nil {
		cfg.Reporter = observe.Discard
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleep
	}
	if cfg.Jitter == nil {
		cfg.Jitter = rand.Float64
	}
	probe := url.URL{Scheme: u.Scheme, Host: u.Host, Path: probePath}
	return &Client{cfg: cfg, probeURL: probe.String()}, nil
}

type request struct {
	Model     string          `json:"model"`
	State     string          `json:"state"`
	Questions json.RawMessage `json:"questions"`
}

type response struct {
	Model   string                     `json:"model"`
	Answers map[string]json.RawMessage `json:"answers"`
	// Kev reports latency_ms; Jev-compatible servers report evaluation_time_ms.
	LatencyMS        *float64 `json:"latency_ms"`
	EvaluationTimeMS *float64 `json:"evaluation_time_ms"`
}

type noulAnswer struct {
	Type string   `json:"type"`
	Noul *float64 `json:"noul"`
}

const opScore fault.Op = "score trace"

// Score asks the model the configured questions about state and returns its
// answer to the decision question. Timeouts, connection errors and 5xx
// responses are retried with jittered backoff; 4xx and malformed responses
// are not. The returned error describes the last attempt (see classify).
func (c *Client) Score(ctx context.Context, state trace.State) (pipeline.Score, error) {
	body, err := json.Marshal(request{Model: c.cfg.Model, State: string(state), Questions: c.cfg.Questions})
	if err != nil {
		return pipeline.Score{}, fault.Internal(fault.Scorer, "encode request", err)
	}
	attempts := c.cfg.MaxRetries + 1
	for attempt := 1; ; attempt++ {
		score, err := c.attempt(ctx, body)
		if err == nil {
			score.Attempts = attempt
			return score, nil
		}
		if attempt == attempts || ctx.Err() != nil || !retryable(err) {
			return pipeline.Score{Attempts: attempt}, classify(err).With("attempts", attempt)
		}
		if err := c.waitBeforeRetry(ctx, attempt, attempts, err); err != nil {
			return pipeline.Score{Attempts: attempt}, classify(err).With("attempts", attempt)
		}
	}
}

// waitBeforeRetry reports the failed attempt and sleeps out its backoff.
func (c *Client) waitBeforeRetry(ctx context.Context, attempt, attempts int, cause error) error {
	delay := c.backoff(attempt - 1)
	c.cfg.Reporter.Report(ctx, classify(cause).With("attempt", attempt).With("max_attempts", attempts).
		With("retry_in", delay.String()))
	return c.cfg.Sleep(ctx, delay)
}

// attempt makes one call to the model, bounded by the per-attempt timeout.
func (c *Client) attempt(ctx context.Context, body []byte) (pipeline.Score, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := c.newRequest(ctx, body)
	if err != nil {
		return pipeline.Score{}, permanent(err)
	}
	start := time.Now()
	raw, err := c.send(req)
	if err != nil {
		return pipeline.Score{}, err
	}
	return c.parse(raw, time.Since(start))
}

func (c *Client) newRequest(ctx context.Context, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	c.authorize(req)
	return req, nil
}

// send returns the body of a 2xx response. A timeout or connection error is
// returned as is; any other status as a *StatusError.
func (c *Client) send(req *http.Request) ([]byte, error) {
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &StatusError{Code: resp.StatusCode, Body: truncate(string(raw), maxErrorBody)}
	}
	return raw, nil
}

// parse reads the decision question's probability from a response. Anything
// malformed is permanent: asking again gets the same answer.
func (c *Client) parse(raw []byte, elapsed time.Duration) (pipeline.Score, error) {
	var out response
	if err := json.Unmarshal(raw, &out); err != nil {
		return pipeline.Score{}, permanent(fmt.Errorf("decode kev response: %w", err))
	}
	p, err := c.decisionAnswer(out)
	if err != nil {
		return pipeline.Score{}, permanent(err)
	}
	return pipeline.Score{Raw: p, Model: c.modelName(out), Latency: reportedLatency(out, elapsed)}, nil
}

// decisionAnswer is the noul of the decision question, checked to be a probability.
func (c *Client) decisionAnswer(out response) (pipeline.Probability, error) {
	q := c.cfg.DecisionQuestion
	rawAnswer, ok := out.Answers[q]
	if !ok {
		return 0, fmt.Errorf("kev response has no answer for %q", q)
	}
	var ans noulAnswer
	if err := json.Unmarshal(rawAnswer, &ans); err != nil {
		return 0, fmt.Errorf("decode answer %q: %w", q, err)
	}
	if ans.Noul == nil {
		return 0, fmt.Errorf("answer %q has no noul value", q)
	}
	if p := *ans.Noul; math.IsNaN(p) || p < 0 || p > 1 {
		return 0, fmt.Errorf("answer %q has noul %v outside [0, 1]", q, p)
	}
	return pipeline.Probability(*ans.Noul), nil
}

// modelName is the model the server says answered, else the one asked for.
func (c *Client) modelName(out response) pipeline.ModelName {
	if out.Model != "" {
		return pipeline.ModelName(out.Model)
	}
	return pipeline.ModelName(c.cfg.Model)
}

// reportedLatency is the server's own evaluation time when it reports one
// (Kev: latency_ms, Jev-compatible servers: evaluation_time_ms), else the
// measured round trip.
func reportedLatency(out response, measured time.Duration) time.Duration {
	for _, ms := range []*float64{out.LatencyMS, out.EvaluationTimeMS} {
		if ms != nil {
			return time.Duration(*ms * float64(time.Millisecond))
		}
	}
	return measured
}

// Ready reports whether the Kev server answers a cheap GET on /v1/models.
// Any response below 500 counts: the server is up and routing requests.
func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.probeURL, nil)
	if err != nil {
		return err
	}
	c.authorize(req)
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode >= 500 {
		return &StatusError{Code: resp.StatusCode, Body: "probe " + c.probeURL}
	}
	return nil
}

func (c *Client) authorize(req *http.Request) {
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
}

// backoff returns the jittered delay before retry number retry (0-based):
// between half and all of the base delay.
func (c *Client) backoff(retry int) time.Duration {
	var base time.Duration
	if n := len(c.cfg.Backoff); n > 0 {
		base = c.cfg.Backoff[min(retry, n-1)]
	} else {
		base = min(500*time.Millisecond<<min(retry, 6), 30*time.Second)
	}
	return base/2 + time.Duration(c.cfg.Jitter()*float64(base/2))
}

// permanentError marks a failure that a retry cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return &permanentError{err} }

func retryable(err error) bool {
	var perm *permanentError
	if errors.As(err, &perm) {
		return false
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.Code >= 500
	}
	return true // timeout or connection error
}

// classify turns a failed attempt into the engine's error kinds. Only a
// 400, 413 or 422, where the model refuses this particular request, is
// rejected; everything else (unreachable, timing out, 5xx, refusing the
// caller with 401, 404 or 429, an unparseable answer) is the scoring
// system's fault and worth retrying later.
func classify(err error) *fault.Error {
	var status *StatusError
	switch {
	case errors.As(err, &status) && rejectedStatus(status.Code):
		return fault.Rejected(fault.Scorer, opScore, err)
	case errors.Is(err, context.DeadlineExceeded):
		return fault.Timeout(fault.Scorer, opScore, err)
	}
	return fault.Unavailable(fault.Scorer, opScore, err)
}

func rejectedStatus(code int) bool {
	switch code {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
