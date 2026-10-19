package observe

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// AbstractSink writes entries somewhere. Implementations must be safe for
// concurrent use.
type AbstractSink interface {
	Write(Entry) error
	Close() error
}

// SinkKind names a sink implementation.
type SinkKind string

const (
	SinkStdout SinkKind = "stdout"
	SinkFile   SinkKind = "file"
)

// SinkConfig selects and configures one sink.
type SinkConfig struct {
	Kind SinkKind
	Path string // file: where to append
}

// AbstractSinkFactory builds sinks from their configuration.
type AbstractSinkFactory interface {
	Build(SinkConfig) (AbstractSink, error)
}

// DefaultSinkFactory builds the sinks the engine ships with.
type DefaultSinkFactory struct {
	Stdout io.Writer // defaults to os.Stdout
}

func (f DefaultSinkFactory) Build(cfg SinkConfig) (AbstractSink, error) {
	switch cfg.Kind {
	case SinkStdout:
		w := f.Stdout
		if w == nil {
			w = os.Stdout
		}
		return newJSONLines(w, nil), nil
	case SinkFile:
		file, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open error log %s: %w", cfg.Path, err)
		}
		return newJSONLines(file, file), nil
	}
	return nil, fmt.Errorf("unknown sink kind %q", cfg.Kind)
}

// jsonLines writes one JSON object per line.
type jsonLines struct {
	mu     sync.Mutex
	enc    *json.Encoder
	closer io.Closer // nil when the writer is not ours to close
}

func newJSONLines(w io.Writer, closer io.Closer) *jsonLines {
	return &jsonLines{enc: json.NewEncoder(w), closer: closer}
}

func (s *jsonLines) Write(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(e)
}

func (s *jsonLines) Close() error {
	if s.closer == nil {
		return nil
	}
	return s.closer.Close()
}
