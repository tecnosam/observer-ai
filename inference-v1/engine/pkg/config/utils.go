package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// durations converts config durations to time.Duration for the components.
func durations(in []Duration) []time.Duration {
	out := make([]time.Duration, len(in))
	for i, d := range in {
		out[i] = d.Std()
	}
	return out
}

// resolveRelative makes path absolute against dir, leaving an absolute path as is.
func resolveRelative(path, dir string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}

func checkURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an http(s) URL", s)
	}
	return nil
}

// parseHeaders reads headers in the OpenTelemetry environment format:
// "key=value,key2=value2", with values percent-encoded where needed
// (a space may be written as is or as %20).
func parseHeaders(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		if strings.TrimSpace(pair) == "" {
			continue
		}
		key, value, ok := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, errors.New(`expected "key=value" pairs separated by commas`)
		}
		decoded, err := url.PathUnescape(strings.TrimSpace(value))
		if err != nil {
			// The cause quotes the bad escape, which is part of a secret.
			return nil, fmt.Errorf("header %q: value is not valid percent-encoding", key)
		}
		out[key] = decoded
	}
	return out, nil
}
