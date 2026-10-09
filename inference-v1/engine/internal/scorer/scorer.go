// Package scorer calls the Kev decision model over HTTP.
package scorer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"
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
	// Timeout applies to each attempt, not to the whole call.
	Timeout time.Duration
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries int
	// Backoff[i] is the base delay before retry i+1; the last entry repeats.
	Backoff []time.Duration

	HTTPClient *http.Client // defaults to a new client
	Logger     *slog.Logger // defaults to slog.Default()

	// Sleep and Jitter exist so tests can run without waiting. Jitter
	// returns a value in [0, 1).
	Sleep  func(ctx context.Context, d time.Duration) error
	Jitter func() float64
}

// Result is the model's answer to the decision question.
type Result struct {
	// PRaw is the uncalibrated probability from the noul answer.
	PRaw float64
	// Model is the model name the server reported.
	Model string
	// LatencyMS is the server-reported evaluation time, or the measured
	// round trip when the server reports none.
	LatencyMS float64
	// Attempts is how many requests were made.
	Attempts int
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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
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

// Score asks Kev the configured questions about state and returns the answer
// to the decision question. Timeouts, connection errors and 5xx responses
// are retried with jittered backoff; 4xx and malformed responses are not.
// The returned error describes the last attempt.
func (c *Client) Score(ctx context.Context, state string) (Result, error) {
	body, err := json.Marshal(request{Model: c.cfg.Model, State: state, Questions: c.cfg.Questions})
	if err != nil {
		return Result{}, fmt.Errorf("scorer: encode request: %w", err)
	}

	attempts := c.cfg.MaxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		res, err := c.attempt(ctx, body)
		if err == nil {
			res.Attempts = attempt
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return Result{Attempts: attempt}, fmt.Errorf("scorer: %w", ctx.Err())
		}
		if !retryable(err) {
			return Result{Attempts: attempt}, fmt.Errorf("scorer: %w", err)
		}
		if attempt == attempts {
			break
		}
		delay := c.backoff(attempt - 1)
		c.cfg.Logger.Warn("kev request failed, retrying",
			"attempt", attempt, "max_attempts", attempts, "retry_in", delay.String(), "error", err.Error())
		if err := c.cfg.Sleep(ctx, delay); err != nil {
			return Result{Attempts: attempt}, fmt.Errorf("scorer: %w", err)
		}
	}
	return Result{Attempts: attempts}, fmt.Errorf("scorer: giving up after %d attempts: %w", attempts, lastErr)
}

func (c *Client) attempt(ctx context.Context, body []byte) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{}, permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return Result{}, err // timeout or connection error
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return Result{}, err
	}
	elapsed := time.Since(start)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Result{}, &StatusError{Code: resp.StatusCode, Body: truncate(string(raw), maxErrorBody)}
	}

	var out response
	if err := json.Unmarshal(raw, &out); err != nil {
		return Result{}, permanent(fmt.Errorf("decode kev response: %w", err))
	}
	rawAnswer, ok := out.Answers[c.cfg.DecisionQuestion]
	if !ok {
		return Result{}, permanent(fmt.Errorf("kev response has no answer for %q", c.cfg.DecisionQuestion))
	}
	var ans noulAnswer
	if err := json.Unmarshal(rawAnswer, &ans); err != nil {
		return Result{}, permanent(fmt.Errorf("decode answer %q: %w", c.cfg.DecisionQuestion, err))
	}
	if ans.Noul == nil {
		return Result{}, permanent(fmt.Errorf("answer %q has no noul value", c.cfg.DecisionQuestion))
	}
	if p := *ans.Noul; math.IsNaN(p) || p < 0 || p > 1 {
		return Result{}, permanent(fmt.Errorf("answer %q has noul %v outside [0, 1]", c.cfg.DecisionQuestion, p))
	}

	res := Result{PRaw: *ans.Noul, Model: out.Model, LatencyMS: float64(elapsed.Microseconds()) / 1000}
	if res.Model == "" {
		res.Model = c.cfg.Model
	}
	switch {
	case out.LatencyMS != nil:
		res.LatencyMS = *out.LatencyMS
	case out.EvaluationTimeMS != nil:
		res.LatencyMS = *out.EvaluationTimeMS
	}
	return res, nil
}

// Ready reports whether the Kev server answers a cheap GET on /v1/models.
// Any response below 500 counts: the server is up and routing requests.
func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.probeURL, nil)
	if err != nil {
		return err
	}
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
