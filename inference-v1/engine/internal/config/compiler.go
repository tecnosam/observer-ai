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
	c.resolveAPIKey(&cfg)
	if err := c.resolveExportHeaders(&cfg); err != nil {
		return nil, fmt.Errorf("config %s: %w", c.factory.Describe(), err)
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
	fields := map[string]*string{
		EnvScorerEndpoint: &cfg.Scorer.Endpoint,
		EnvNATSURL:        &cfg.Queue.URL,
		EnvManifest:       &cfg.Scorer.Manifest,
		EnvOTLPGRPCAddr:   &cfg.OTLP.GRPCAddr,
		EnvOTLPHTTPAddr:   &cfg.OTLP.HTTPAddr,
	}
	if len(cfg.Exporters) > 0 {
		fields[EnvExportEndpoint] = &cfg.Exporters[0].Endpoint
	}
	for env, field := range fields {
		if v := getenv(env); v != "" {
			*field = v
		}
	}
}

// resolveAPIKey reads the scorer's API key from the variable it names
// (JEV_API_KEY by default for Jev).
func (c *Compiler) resolveAPIKey(cfg *Config) {
	if cfg.Scorer.Kind == ScorerJev && cfg.Scorer.APIKeyEnv == "" {
		cfg.Scorer.APIKeyEnv = DefaultJevAPIKeyEnv
	}
	if cfg.Scorer.APIKeyEnv != "" && c.getenv != nil {
		cfg.Scorer.APIKey = c.getenv(cfg.Scorer.APIKeyEnv)
	}
}

// resolveExportHeaders gives every exporter its final headers: the ones
// written in the file, plus those in the variable headers_env names.
func (c *Compiler) resolveExportHeaders(cfg *Config) error {
	for i := range cfg.Exporters {
		e := &cfg.Exporters[i]
		resolved, err := c.exportHeaders(*e)
		if err != nil {
			return fmt.Errorf("exporters[%d] (%s): %w", i, e.Name, err)
		}
		e.ResolvedHeaders = resolved
	}
	return nil
}

func (c *Compiler) exportHeaders(e Exporter) (map[string]string, error) {
	out := make(map[string]string, len(e.Headers))
	for k, v := range e.Headers {
		out[k] = v
	}
	if e.HeadersEnv == "" || c.getenv == nil {
		return out, nil
	}
	raw := c.getenv(e.HeadersEnv)
	if raw == "" {
		return nil, fmt.Errorf("headers_env: $%s is not set", e.HeadersEnv)
	}
	fromEnv, err := parseHeaders(raw)
	if err != nil {
		return nil, fmt.Errorf("headers_env: $%s: %w", e.HeadersEnv, err)
	}
	for k, v := range fromEnv {
		out[k] = v
	}
	return out, nil
}
