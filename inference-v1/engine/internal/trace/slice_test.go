package trace

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

const testdata = "../../testdata"

// TestBuildStateGolden checks BuildState against the output of the Python
// training code for every fixture. Regenerate with `make golden`.
func TestBuildStateGolden(t *testing.T) {
	traces, err := filepath.Glob(filepath.Join(testdata, "traces", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	goldens, err := filepath.Glob(filepath.Join(testdata, "golden", "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) == 0 {
		t.Fatal("no trace fixtures found")
	}
	if len(traces) != len(goldens) {
		t.Fatalf("%d traces but %d golden files; run `make golden`", len(traces), len(goldens))
	}

	for _, path := range traces {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			tr, err := Decode(f)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			want, err := os.ReadFile(filepath.Join(testdata, "golden", name+".txt"))
			if err != nil {
				t.Fatalf("%v; run `make golden`", err)
			}
			if got := BuildState(tr); got != string(want) {
				t.Errorf("BuildState differs from Python at byte %d\n--- got\n%s\n--- want\n%s",
					firstDiff(got, string(want)), got, want)
			}
		})
	}
}

// TestBuildStateFuzzGolden compares against a larger Python-generated corpus
// when OBSERVER_FUZZ_GOLDEN points at one (see `make fuzz-golden`). Each line
// is {"trace": {...}, "state": "..."}.
func TestBuildStateFuzzGolden(t *testing.T) {
	path := os.Getenv("OBSERVER_FUZZ_GOLDEN")
	if path == "" {
		t.Skip("OBSERVER_FUZZ_GOLDEN not set")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	n, failed := 0, 0
	for sc.Scan() {
		n++
		var row struct {
			Trace json.RawMessage `json:"trace"`
			State string          `json:"state"`
		}
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("line %d: %v", n, err)
		}
		tr, err := Decode(strings.NewReader(string(row.Trace)))
		if err != nil {
			t.Fatalf("line %d: decode: %v", n, err)
		}
		if got := BuildState(tr); got != row.State {
			failed++
			if failed <= 5 {
				d := firstDiff(got, row.State)
				t.Errorf("line %d differs at byte %d:\n got  %q\n want %q", n, d, window(got, d), window(row.State, d))
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d cases, %d mismatches", n, failed)
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		in   string
		n    int
		want string
	}{
		{"", 3, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"日本語です", 3, "日本語"},
		{"a😀b😀c", 2, "a😀"},
		{"éé", 5, "éé"},
	}
	for _, tt := range tests {
		got := truncateRunes(tt.in, tt.n)
		if got != tt.want {
			t.Errorf("truncateRunes(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateRunes(%q, %d) split a code point", tt.in, tt.n)
		}
	}
}

func TestPyRepr(t *testing.T) {
	tests := []struct{ in, want string }{
		{`"plain"`, `'plain'`},
		{`"it's"`, `"it's"`},
		{`"say \"hi\""`, `'say "hi"'`},
		{`"it's \"both\""`, `'it\'s "both"'`},
		{`"back\\slash"`, `'back\\slash'`},
		{`"a\nb\tc\r"`, `'a\nb\tc\r'`},
		{`"\u0000\u001f\u007f"`, `'\x00\x1f\x7f'`},
		{`"\u00a0\u200b"`, `'\xa0\u200b'`},
		{`"é日😀"`, `'é日😀'`},
		{`""`, `''`},
		{`1`, `1`},
		{`-0`, `0`},
		{`1.0`, `1.0`},
		{`1e16`, `1e+16`},
		{`1e15`, `1000000000000000.0`},
		{`0.00001`, `1e-05`},
		{`-0.0`, `-0.0`},
		{`1e999`, `inf`},
		{`true`, `True`},
		{`false`, `False`},
		{`null`, `None`},
		{`[]`, `[]`},
		{`{}`, `{}`},
		{`[1, "a", null]`, `[1, 'a', None]`},
		{`{"b": 1, "a": {"c": [true]}}`, `{'b': 1, 'a': {'c': [True]}}`},
		{`{"a": 1, "b": 2, "a": 3}`, `{'a': 3, 'b': 2}`},
	}
	for _, tt := range tests {
		got, err := pyRepr([]byte(tt.in))
		if err != nil {
			t.Errorf("pyRepr(%s): %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("pyRepr(%s) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestPyReprDepthLimit(t *testing.T) {
	deep := strings.Repeat("[", maxReprDepth+2) + strings.Repeat("]", maxReprDepth+2)
	if _, err := pyRepr([]byte(deep)); err == nil {
		t.Error("expected an error for args nested past the depth limit")
	}
}

func firstDiff(a, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func window(s string, at int) string {
	return s[max(0, at-40):min(len(s), at+40)]
}
