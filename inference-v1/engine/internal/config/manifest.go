package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// LoadManifest reads manifest.json and checks that decisionQuestion exists
// and is a noul question.
func LoadManifest(path, decisionQuestion string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	m, err := ParseManifest(raw, decisionQuestion)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	return m, nil
}

// ParseManifest decodes a manifest held in memory and checks it the same way.
func ParseManifest(raw []byte, decisionQuestion string) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if err := m.prepare(decisionQuestion); err != nil {
		return nil, err
	}
	return &m, nil
}

// prepare checks the manifest for decisionQuestion, compacts its questions
// (keys and order untouched) and sets Temperature.
func (m *Manifest) prepare(decisionQuestion string) error {
	var questions map[string]json.RawMessage
	if err := json.Unmarshal(m.Questions, &questions); err != nil || len(questions) == 0 {
		return fmt.Errorf("\"questions\" must be a non-empty object")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, m.Questions); err != nil {
		return err
	}
	m.Questions = compact.Bytes()

	q, ok := questions[decisionQuestion]
	if !ok {
		return fmt.Errorf("decision question %q not found in questions", decisionQuestion)
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(q, &head); err != nil {
		return fmt.Errorf("question %q: %w", decisionQuestion, err)
	}
	if head.Type != questionTypeNoul {
		return fmt.Errorf("decision question %q has type %q, want %q", decisionQuestion, head.Type, questionTypeNoul)
	}

	m.Temperature = 1.0
	if t, ok := m.Temperatures[decisionQuestion]; ok {
		if !(t > 0) {
			return fmt.Errorf("temperature for %q must be positive, got %v", decisionQuestion, t)
		}
		m.Temperature = t
	}
	return nil
}
