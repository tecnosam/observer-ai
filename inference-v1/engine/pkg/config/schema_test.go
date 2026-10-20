package config

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const schemaPath = "../../observer.schema.yaml"

// observer.schema.yaml documents the config for people and editors. This
// keeps it honest: every key the engine reads is in the schema and the other
// way round, with the default the engine actually uses.
func TestSchemaMatchesConfig(t *testing.T) {
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]schemaSection `yaml:"properties"`
	}
	if err := yaml.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}

	defaults := reflect.ValueOf(Default())
	var sections, envs []string
	for i := 0; i < defaults.NumField(); i++ {
		section := defaults.Type().Field(i).Tag.Get("yaml")
		if section == "-" {
			continue
		}
		sections = append(sections, section)
		if defaults.Field(i).Kind() == reflect.Slice {
			envs = append(envs, checkListSection(t, section, schema.Properties[section], defaults.Field(i))...)
			continue
		}
		props := schema.Properties[section].Properties
		if props == nil {
			t.Errorf("section %q is missing from the schema", section)
			continue
		}

		value := defaults.Field(i)
		var keys []string
		for j := 0; j < value.NumField(); j++ {
			key := strings.Split(value.Type().Field(j).Tag.Get("yaml"), ",")[0]
			if key == "-" {
				continue // filled in at load time, not read from the file
			}
			keys = append(keys, key)
			prop, ok := props[key]
			if !ok {
				t.Errorf("%s.%s is missing from the schema", section, key)
				continue
			}
			if env, ok := prop["x-env"].(string); ok {
				envs = append(envs, env)
			}
			want := value.Field(j).Interface()
			got, hasDefault := prop["default"]
			if reflect.ValueOf(want).IsZero() {
				if hasDefault {
					t.Errorf("%s.%s: schema default %v, but the engine has none", section, key, got)
				}
				continue
			}
			if !hasDefault || !sameDefault(t, got, want) {
				t.Errorf("%s.%s: schema default %v, engine default %v", section, key, got, want)
			}
		}
		for key := range props {
			if !contains(keys, key) {
				t.Errorf("%s.%s is in the schema but the engine does not read it", section, key)
			}
		}
	}
	for section := range schema.Properties {
		if !contains(sections, section) {
			t.Errorf("section %q is in the schema but the engine does not read it", section)
		}
	}

	wantEnvs := []string{EnvExportEndpoint, EnvManifest, EnvNATSURL, EnvOTLPGRPCAddr, EnvOTLPHTTPAddr, EnvScorerEndpoint}
	sort.Strings(envs)
	if !reflect.DeepEqual(envs, wantEnvs) {
		t.Errorf("x-env in the schema = %v, want %v", envs, wantEnvs)
	}
}

type schemaSection struct {
	Properties map[string]map[string]any `yaml:"properties"`
	Default    any                       `yaml:"default"`
	Env        string                    `yaml:"x-env"`
	Items      struct {
		Properties map[string]map[string]any `yaml:"properties"`
	} `yaml:"items"`
}

// checkListSection compares a section that is a list (exporters): the keys
// of its items, and the default list as a whole. It returns its x-env.
func checkListSection(t *testing.T, section string, schema schemaSection, value reflect.Value) []string {
	t.Helper()
	props := schema.Items.Properties
	if props == nil {
		t.Errorf("section %q is missing its items from the schema", section)
		return nil
	}
	item := value.Type().Elem()
	var keys []string
	for j := 0; j < item.NumField(); j++ {
		key := strings.Split(item.Field(j).Tag.Get("yaml"), ",")[0]
		if key == "-" {
			continue
		}
		keys = append(keys, key)
		if _, ok := props[key]; !ok {
			t.Errorf("%s[].%s is missing from the schema", section, key)
		}
	}
	for key := range props {
		if !contains(keys, key) {
			t.Errorf("%s[].%s is in the schema but the engine does not read it", section, key)
		}
	}
	if !sameDefault(t, schema.Default, value.Interface()) {
		t.Errorf("%s: schema default %v, engine default %v", section, schema.Default, value.Interface())
	}
	if schema.Env == "" {
		return nil
	}
	return []string{schema.Env}
}

// sameDefault compares a default read from the schema with the engine's.
// Durations are written as strings in the schema.
func sameDefault(t *testing.T, got, want any) bool {
	t.Helper()
	duration := func(v any) (Duration, bool) {
		s, ok := v.(string)
		if !ok {
			return 0, false
		}
		d, err := time.ParseDuration(s)
		return Duration(d), err == nil
	}
	switch want := want.(type) {
	case Duration:
		d, ok := duration(got)
		return ok && d == want
	case []Duration:
		list, ok := got.([]any)
		if !ok || len(list) != len(want) {
			return false
		}
		for i, item := range list {
			if d, ok := duration(item); !ok || d != want[i] {
				return false
			}
		}
		return true
	case int:
		n, ok := got.(int)
		return ok && n == want
	case int64:
		n, ok := got.(int)
		return ok && int64(n) == want
	case float64:
		switch n := got.(type) {
		case float64:
			return n == want
		case int:
			return float64(n) == want
		}
		return false
	default:
		// Anything else (lists of structs, ...) compares in its YAML form.
		raw, err := yaml.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var asYAML any
		if err := yaml.Unmarshal(raw, &asYAML); err != nil {
			t.Fatal(err)
		}
		return reflect.DeepEqual(got, asYAML)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
