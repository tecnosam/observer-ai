package config

import (
	"fmt"
	"os"
)

// Compiler turns the settings an AbstractLoaderFactory provides into a
// validated Config, in a fixed order:
//
//  1. load the settings, every default filled in
//  2. apply the environment overrides (OBSERVER_*)
//  3. read the scorer's API key from the variable scorer.api_key_env names
//  4. validate every value, reporting all problems at once
//  5. load the manifest and check it for the decision question
type Compiler struct {
	factory AbstractLoaderFactory
	getenv  func(string) string
}

// Option configures a Compiler.
type Option func(*Compiler)

// WithEnv sets where environment variables are read from: os.Getenv by
// default. nil turns the overrides and the API key lookup off.
func WithEnv(getenv func(string) string) Option {
	return func(c *Compiler) { c.getenv = getenv }
}

func NewCompiler(factory AbstractLoaderFactory, opts ...Option) *Compiler {
	c := &Compiler{factory: factory, getenv: os.Getenv}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Load compiles the YAML file at path. It is what the engine binary runs.
func Load(path string) (*Config, error) {
	return NewCompiler(NewYAMLFileFactory(path)).Compile()
}

func (c *Compiler) Compile() (*Config, error) {
	cfg, err := c.factory.ConfigLoader().LoadConfig()
	if err != nil {
		return nil, err
	}
	if c.getenv != nil {
		applyEnv(&cfg, c.getenv)
	}
	if cfg.Scorer.Kind == ScorerJev && cfg.Scorer.APIKeyEnv == "" {
		cfg.Scorer.APIKeyEnv = DefaultJevAPIKeyEnv
	}
	if cfg.Scorer.APIKeyEnv != "" && c.getenv != nil {
		cfg.Scorer.APIKey = c.getenv(cfg.Scorer.APIKeyEnv)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", c.factory.Describe(), err)
	}

	m, err := c.factory.ManifestLoader(cfg).LoadManifest(cfg.Scorer.DecisionQuestion)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", c.factory.Describe(), err)
	}
	cfg.Manifest = *m
	return &cfg, nil
}

// applyEnv replaces the keys that differ between a laptop and a container
// with their OBSERVER_* variables, when set.
func applyEnv(cfg *Config, getenv func(string) string) {
	for env, field := range map[string]*string{
		EnvScorerEndpoint:  &cfg.Scorer.Endpoint,
		EnvPhoenixEndpoint: &cfg.Export.PhoenixEndpoint,
		EnvNATSURL:         &cfg.Queue.URL,
		EnvManifest:        &cfg.Scorer.Manifest,
		EnvOTLPGRPCAddr:    &cfg.OTLP.GRPCAddr,
		EnvOTLPHTTPAddr:    &cfg.OTLP.HTTPAddr,
	} {
		if v := getenv(env); v != "" {
			*field = v
		}
	}
}
