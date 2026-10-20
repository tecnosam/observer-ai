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
			if got := string(BuildState(tr)); got != string(want) {
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
		if got := string(BuildState(tr)); got != row.State {
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

// compactJSON prints what Python's json.dumps(v, ensure_ascii=False,
// separators=(",", ":")) prints.
func TestCompactJSON(t *testing.T) {
	tests := []struct{ in, want string }{
		{`{"query": "capital of France", "limit": 3}`, `{"query":"capital of France","limit":3}`},
		{`{"b": 1, "a": 2}`, `{"b":1,"a":2}`},         // key order kept
		{`{"a": 1, "b": 2, "a": 3}`, `{"a":3,"b":2}`}, // a repeated key: first place, last value
		{`"Seine"`, `"Seine"`},
		{`[1, [2, {"x": null}], true, false]`, `[1,[2,{"x":null}],true,false]`},
		{`{}`, `{}`},
		{`[]`, `[]`},
		{`{"a": {}, "b": []}`, `{"a":{},"b":[]}`},
		{`1.5e3`, `1.5e3`},                           // numbers as written
		{`"caf\u00e9 \ud83d\ude80"`, `"café 🚀"`},     // escapes decoded
		{`"tab\tnl\nq\"bs\\"`, `"tab\tnl\nq\"bs\\"`}, // required escapes kept
		{`"nul:\u0000 esc:\u001b del:\u007f"`, `"nul:\u0000 esc:\u001b del:` + "\x7f" + `"`},
		{`"ls:\u2028 ps:\u2029"`, "\"ls:\u2028 ps:\u2029\""},
		{`"<b>&</b>"`, `"<b>&</b>"`}, // no HTML escaping
	}
	for _, tt := range tests {
		got, err := compactJSON([]byte(tt.in))
		if err != nil {
			t.Errorf("compactJSON(%s): %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("compactJSON(%s) = %s, want %s", tt.in, got, tt.want)
		}
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
