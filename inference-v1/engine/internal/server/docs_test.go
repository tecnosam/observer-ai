package server

import (
	"net/http"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type openAPIDoc struct {
	OpenAPI string `yaml:"openapi"`
	Info    struct {
		Title   string `yaml:"title"`
		Version string `yaml:"version"`
	} `yaml:"info"`
	Paths      map[string]map[string]openAPIOperation `yaml:"paths"`
	Components struct {
		Schemas map[string]struct {
			Required   []string             `yaml:"required"`
			Properties map[string]yaml.Node `yaml:"properties"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

type openAPIOperation struct {
	OperationID string               `yaml:"operationId"`
	Responses   map[string]yaml.Node `yaml:"responses"`
}

func parseSpec(t *testing.T, raw []byte) openAPIDoc {
	t.Helper()
	var doc openAPIDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("openapi.yaml does not parse: %v", err)
	}
	return doc
}

func TestOpenAPIServed(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	w := h.do("GET", "/openapi.yaml", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("Content-Type = %q, want application/yaml", ct)
	}
	doc := parseSpec(t, w.Body.Bytes())
	if !strings.HasPrefix(doc.OpenAPI, "3.") || doc.Info.Title == "" || doc.Info.Version == "" {
		t.Errorf("spec header = %q %q %q", doc.OpenAPI, doc.Info.Title, doc.Info.Version)
	}
}

// Every API route is documented with the status codes the handlers return.
func TestOpenAPICoversRoutes(t *testing.T) {
	doc := parseSpec(t, openAPISpec)
	want := []struct {
		method, path string
		statuses     []string
	}{
		{"post", "/v1/traces", []string{"202", "400", "413", "503"}},
		{"post", "/v1/traces:score", []string{"200", "400", "413"}},
		{"get", "/healthz", []string{"200"}},
		{"get", "/readyz", []string{"200", "503"}},
	}
	for _, tt := range want {
		op, ok := doc.Paths[tt.path][tt.method]
		if !ok {
			t.Errorf("%s %s is not in the spec", tt.method, tt.path)
			continue
		}
		if op.OperationID == "" {
			t.Errorf("%s %s has no operationId", tt.method, tt.path)
		}
		for _, status := range tt.statuses {
			if _, ok := op.Responses[status]; !ok {
				t.Errorf("%s %s does not document status %s", tt.method, tt.path, status)
			}
		}
	}
	if len(doc.Paths) != len(want) {
		t.Errorf("spec documents %d paths, want %d", len(doc.Paths), len(want))
	}
}

// The Decision schema lists exactly the fields the sync path returns.
func TestOpenAPIDecisionMatchesResult(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	w := h.do("POST", "/v1/traces:score", `{"question": "q", "final_answer": "a"}`)
	got := decodeBody[map[string]any](t, w)

	schema := parseSpec(t, openAPISpec).Components.Schemas["Decision"]
	if len(schema.Properties) != len(got) {
		t.Errorf("Decision schema has %d properties, the response has %d", len(schema.Properties), len(got))
	}
	for field := range got {
		if _, ok := schema.Properties[field]; !ok {
			t.Errorf("response field %q is not in the Decision schema", field)
		}
	}
	for _, field := range schema.Required {
		if _, ok := got[field]; !ok {
			t.Errorf("Decision schema requires %q but the response lacks it", field)
		}
	}
}

func TestDocsPage(t *testing.T) {
	h := newHarness(t, harnessOpts{scorer: &fakeScorer{score: fixedScore(0.9)}})
	w := h.do("GET", "/docs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	for _, want := range []string{"swagger-ui", `url: "/openapi.yaml"`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("docs page does not contain %q", want)
		}
	}
}
