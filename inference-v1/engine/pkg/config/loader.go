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

// AbstractManifestLoader produces a model's manifest, checked for the
// model's decision question.
type AbstractManifestLoader interface {
	// LoadManifest loads the manifest of the default model (agent "") or
	// of an agent's model, configured by model.
	LoadManifest(agent string, model Scorer) (*Manifest, error)
}

// AbstractLoaderFactory builds the loaders for one way of configuring the
// engine, so the two always agree on where things come from: a YAML file and
// the manifest file it names, or values built in code.
type AbstractLoaderFactory interface {
	ConfigLoader() AbstractConfigLoader
	ManifestLoader() AbstractManifestLoader
	// Describe names the source for error messages, e.g. the file path.
	Describe() string
}

// ---- YAML file

// YAMLFileFactory reads observer.yaml from disk. A relative manifest path
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

func (f *YAMLFileFactory) ManifestLoader() AbstractManifestLoader {
	return &fileManifestLoader{dir: filepath.Dir(f.Path)}
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
	if err := applyModelDefaults(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("config %s: %w", l.path, err)
	}
	resolveQueueFiles(&cfg.Queue, filepath.Dir(l.path))
	return cfg, nil
}

// resolveQueueFiles makes the NATS credential and certificate paths
// absolute against the config file's directory, as manifest paths are.
func resolveQueueFiles(q *Queue, dir string) {
	for _, path := range []*string{
		&q.Auth.CredentialsFile, &q.Auth.NkeySeedFile, &q.TLS.CAFile, &q.TLS.CertFile, &q.TLS.KeyFile,
	} {
		if *path != "" {
			*path = resolveRelative(*path, dir)
		}
	}
}

// applyModelDefaults decodes each agents.models entry again, this time over
// DefaultModel(), so a key it leaves out gets the engine's built-in default,
// as in the scorer section. Nothing comes from the scorer section itself.
// The strict first decode has already checked the keys.
func applyModelDefaults(raw []byte, cfg *Config) error {
	var doc struct {
		Agents struct {
			Models map[string]yaml.Node `yaml:"models"`
		} `yaml:"agents"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for agent, node := range doc.Agents.Models {
		model := DefaultModel()
		if err := node.Decode(&model); err != nil {
			return fmt.Errorf("agents.models.%s: %w", agent, err)
		}
		cfg.Agents.Models[agent] = model
	}
	return nil
}

// fileManifestLoader reads a model's manifest file; a relative path
// resolves against dir (empty: the working directory).
type fileManifestLoader struct {
	dir string
}

func (l *fileManifestLoader) LoadManifest(agent string, model Scorer) (*Manifest, error) {
	if model.Manifest == "" {
		return nil, fmt.Errorf("%s.manifest is required", modelKey(agent))
	}
	path := model.Manifest
	if l.dir != "" {
		path = resolveRelative(path, l.dir)
	}
	return LoadManifest(path, model.DecisionQuestion)
}

// modelKey is where a model is configured, for messages: "scorer" for the
// default, agents.models.<agent> for an agent's.
func modelKey(agent string) string {
	if agent == "" {
		return "scorer"
	}
	return "agents.models." + agent
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
// With a nil manifest, it is read from the file scorer.manifest names. Build
// each agent's model (cfg.Agents.Models) from DefaultModel(); its manifest
// comes from AgentManifests, else from its manifest file.
type StaticFactory struct {
	Config         Config
	Manifest       *Manifest
	AgentManifests map[string]*Manifest
}

func NewStaticFactory(cfg Config, manifest *Manifest) *StaticFactory {
	return &StaticFactory{Config: cfg, Manifest: manifest}
}

func (f *StaticFactory) ConfigLoader() AbstractConfigLoader {
	return &staticConfigLoader{cfg: f.Config}
}

func (f *StaticFactory) ManifestLoader() AbstractManifestLoader {
	return &staticManifestLoader{def: f.Manifest, byAgent: f.AgentManifests}
}

func (f *StaticFactory) Describe() string { return "config built in code" }

type staticConfigLoader struct {
	cfg Config
}

func (l *staticConfigLoader) LoadConfig() (Config, error) { return l.cfg, nil }

type staticManifestLoader struct {
	def     *Manifest
	byAgent map[string]*Manifest
}

func (l *staticManifestLoader) LoadManifest(agent string, model Scorer) (*Manifest, error) {
	given := l.def
	if agent != "" {
		given = l.byAgent[agent]
	}
	if given == nil {
		return (&fileManifestLoader{}).LoadManifest(agent, model)
	}
	m := *given // a copy, so preparing it leaves the caller's value alone
	if err := m.prepare(model.DecisionQuestion); err != nil {
		return nil, fmt.Errorf("%s manifest: %w", modelKey(agent), err)
	}
	return &m, nil
}
