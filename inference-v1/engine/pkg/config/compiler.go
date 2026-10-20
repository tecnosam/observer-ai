package config

import (
	"fmt"
	"maps"
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
	if err := c.compile(&cfg); err != nil {
		return nil, fmt.Errorf("config %s: %w", c.factory.Describe(), err)
	}
	return &cfg, nil
}

// compile runs every step after loading: environment, secrets, validation,
// then the manifests the settings point at.
func (c *Compiler) compile(cfg *Config) error {
	if c.getenv != nil {
		applyEnv(cfg, c.getenv)
	}
	c.resolveAPIKeys(cfg)
	if err := c.resolveHeaders(cfg); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	return c.loadManifests(cfg)
}

// loadManifests loads the default model's manifest and every agent model's.
func (c *Compiler) loadManifests(cfg *Config) error {
	loader := c.factory.ManifestLoader()
	m, err := loader.LoadManifest("", cfg.Scorer)
	if err != nil {
		return err
	}
	cfg.Manifest = *m
	cfg.AgentManifests = make(map[string]Manifest, len(cfg.Agents.Models))
	for agent, model := range cfg.Agents.Models {
		m, err := loader.LoadManifest(agent, model)
		if err != nil {
			return err
		}
		cfg.AgentManifests[agent] = *m
	}
	return nil
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

// resolveAPIKeys reads each model's API key from the variable it names
// (JEV_API_KEY by default for Jev).
func (c *Compiler) resolveAPIKeys(cfg *Config) {
	c.resolveAPIKey(&cfg.Scorer)
	for agent, model := range cfg.Agents.Models {
		c.resolveAPIKey(&model)
		cfg.Agents.Models[agent] = model
	}
}

func (c *Compiler) resolveAPIKey(s *Scorer) {
	if s.Kind == ScorerJev && s.APIKeyEnv == "" {
		s.APIKeyEnv = DefaultJevAPIKeyEnv
	}
	if s.APIKeyEnv != "" && c.getenv != nil {
		s.APIKey = c.getenv(s.APIKeyEnv)
	}
}

// resolveHeaders gives every exporter, and the metrics exporter, its final
// headers: the ones written in the file, plus those in the variable
// headers_env names.
func (c *Compiler) resolveHeaders(cfg *Config) error {
	for i := range cfg.Exporters {
		e := &cfg.Exporters[i]
		resolved, err := c.headers(e.Headers, e.HeadersEnv)
		if err != nil {
			return fmt.Errorf("exporters[%d] (%s): %w", i, e.Name, err)
		}
		e.ResolvedHeaders = resolved
	}
	m := &cfg.Observability.Metrics
	resolved, err := c.headers(m.Headers, m.HeadersEnv)
	if err != nil {
		return fmt.Errorf("observability.metrics: %w", err)
	}
	m.ResolvedHeaders = resolved
	return nil
}

func (c *Compiler) headers(inline map[string]string, env string) (map[string]string, error) {
	out := maps.Clone(inline)
	if out == nil {
		out = map[string]string{}
	}
	if env == "" || c.getenv == nil {
		return out, nil
	}
	raw := c.getenv(env)
	if raw == "" {
		return nil, fmt.Errorf("headers_env: $%s is not set", env)
	}
	fromEnv, err := parseHeaders(raw)
	if err != nil {
		return nil, fmt.Errorf("headers_env: $%s: %w", env, err)
	}
	maps.Copy(out, fromEnv)
	return out, nil
}
