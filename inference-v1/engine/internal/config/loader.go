package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// AbstractConfigLoader produces the engine's settings with every default
// filled in. Environment overrides and validation come later, in Compiler.
type AbstractConfigLoader interface {
	LoadConfig() (Config, error)
}

// AbstractManifestLoader produces the model manifest, checked for the
// decision question.
type AbstractManifestLoader interface {
	LoadManifest(decisionQuestion string) (*Manifest, error)
}

// AbstractLoaderFactory builds the loaders for one way of configuring the
// engine, so the two always agree on where things come from: a YAML file and
// the manifest file it names, or values built in code.
type AbstractLoaderFactory interface {
	ConfigLoader() AbstractConfigLoader
	// ManifestLoader is built from the loaded settings, which may say where
	// the manifest is.
	ManifestLoader(cfg Config) AbstractManifestLoader
	// Describe names the source for error messages, e.g. the file path.
	Describe() string
}

// ---- YAML file

// YAMLFileFactory reads observer.yaml from disk. A relative scorer.manifest
// resolves against the YAML file's directory.
type YAMLFileFactory struct {
	Path string
}

func NewYAMLFileFactory(path string) *YAMLFileFactory {
	return &YAMLFileFactory{Path: path}
}

func (f *YAMLFileFactory) ConfigLoader() AbstractConfigLoader {
	return &yamlFileLoader{path: f.Path}
}

func (f *YAMLFileFactory) ManifestLoader(cfg Config) AbstractManifestLoader {
	path := cfg.Scorer.Manifest
	if path != "" {
		path = resolveRelative(path, filepath.Dir(f.Path))
	}
	return &fileManifestLoader{path: path}
}

func (f *YAMLFileFactory) Describe() string { return f.Path }

type yamlFileLoader struct {
	path string
}

// LoadConfig decodes the file over Default(). Decoding is strict: an unknown
// key is an error with its line number, not silently ignored.
func (l *yamlFileLoader) LoadConfig() (Config, error) {
	raw, err := os.ReadFile(l.path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config %s: %w", l.path, err)
	}
	return cfg, nil
}

type fileManifestLoader struct {
	path string
}

func (l *fileManifestLoader) LoadManifest(decisionQuestion string) (*Manifest, error) {
	if l.path == "" {
		return nil, errors.New("scorer.manifest is required")
	}
	return LoadManifest(l.path, decisionQuestion)
}

// ---- values built in code

// StaticFactory configures the engine from values built in code, for a
// program that embeds the engine instead of shipping a YAML file. Start from
// Default() and change what you need:
//
//	cfg := config.Default()
//	cfg.Scorer.Endpoint = "http://kev:8010/v1/systemone"
//	compiled, err := config.NewCompiler(config.NewStaticFactory(cfg, manifest)).Compile()
//
// With a nil manifest, it is read from the file scorer.manifest names.
type StaticFactory struct {
	Config   Config
	Manifest *Manifest
}

func NewStaticFactory(cfg Config, manifest *Manifest) *StaticFactory {
	return &StaticFactory{Config: cfg, Manifest: manifest}
}

func (f *StaticFactory) ConfigLoader() AbstractConfigLoader {
	return &staticConfigLoader{cfg: f.Config}
}

func (f *StaticFactory) ManifestLoader(cfg Config) AbstractManifestLoader {
	if f.Manifest != nil {
		return &staticManifestLoader{manifest: *f.Manifest}
	}
	return &fileManifestLoader{path: cfg.Scorer.Manifest}
}

func (f *StaticFactory) Describe() string { return "config built in code" }

type staticConfigLoader struct {
	cfg Config
}

func (l *staticConfigLoader) LoadConfig() (Config, error) { return l.cfg, nil }

type staticManifestLoader struct {
	manifest Manifest // a copy, so preparing it leaves the caller's value alone
}

func (l *staticManifestLoader) LoadManifest(decisionQuestion string) (*Manifest, error) {
	m := l.manifest
	if err := m.prepare(decisionQuestion); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	return &m, nil
}
