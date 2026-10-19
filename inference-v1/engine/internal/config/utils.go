package config

import (
	"fmt"
	"net/url"
	"path/filepath"
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
