package trace

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// maxReprDepth bounds nesting in tool-call args. Python's own repr gives up
// on deeply nested containers, so nothing the model was trained on is deeper.
const maxReprDepth = 200

// pyRepr renders a JSON value the way Python's repr() renders the result of
// json.loads on it: single-quoted strings with Python escaping, True/False/
// None, and dicts in insertion order.
func pyRepr(raw []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	s, err := reprValue(dec, 0)
	if err != nil {
		return "", err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return "", errors.New("unexpected data after JSON value")
	}
	return s, nil
}

// pyStr renders a JSON value the way Python's str() would: strings as
// themselves, everything else as repr.
func pyStr(raw []byte) (string, error) {
	var s string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	return pyRepr(raw)
}

func reprValue(dec *json.Decoder, depth int) (string, error) {
	if depth > maxReprDepth {
		return "", fmt.Errorf("nested deeper than %d levels", maxReprDepth)
	}
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	switch v := tok.(type) {
	case nil:
		return "None", nil
	case bool:
		if v {
			return "True", nil
		}
		return "False", nil
	case string:
		return reprString(v), nil
	case json.Number:
		return reprNumber(v.String()), nil
	case json.Delim:
		if v == '[' {
			return reprList(dec, depth)
		}
		return reprDict(dec, depth)
	}
	return "", fmt.Errorf("unexpected JSON token %v", tok)
}

func reprList(dec *json.Decoder, depth int) (string, error) {
	var items []string
	for dec.More() {
		s, err := reprValue(dec, depth+1)
		if err != nil {
			return "", err
		}
		items = append(items, s)
	}
	if _, err := dec.Token(); err != nil { // closing ]
		return "", err
	}
	return "[" + strings.Join(items, ", ") + "]", nil
}

func reprDict(dec *json.Decoder, depth int) (string, error) {
	// A repeated key keeps its first position and takes the last value, as
	// in a Python dict.
	var keys []string
	vals := map[string]string{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return "", err
		}
		key, ok := tok.(string)
		if !ok {
			return "", fmt.Errorf("unexpected object key %v", tok)
		}
		val, err := reprValue(dec, depth+1)
		if err != nil {
			return "", err
		}
		if _, seen := vals[key]; !seen {
			keys = append(keys, key)
		}
		vals[key] = val
	}
	if _, err := dec.Token(); err != nil { // closing }
		return "", err
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(reprString(k))
		b.WriteString(": ")
		b.WriteString(vals[k])
	}
	b.WriteByte('}')
	return b.String(), nil
}

// reprNumber renders a JSON number literal. Python's json module reads a
// literal with a fraction or exponent as float and anything else as int.
func reprNumber(lit string) string {
	if !strings.ContainsAny(lit, ".eE") {
		if lit == "-0" {
			return "0"
		}
		return lit
	}
	f, _ := strconv.ParseFloat(lit, 64) // out of range yields ±Inf, as in Python
	return reprFloat(f)
}

// reprFloat matches Python's float repr: shortest round-trip digits,
// scientific notation when the decimal exponent is < -4 or >= 16.
func reprFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	sci := strconv.FormatFloat(f, 'e', -1, 64)
	exp, _ := strconv.Atoi(sci[strings.IndexByte(sci, 'e')+1:])
	if exp < -4 || exp >= 16 {
		return sci
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// reprString matches Python's str repr: single quotes unless the string has
// a single quote and no double quote, with non-printable characters escaped.
func reprString(s string) string {
	quote := '\''
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteRune(quote)
	for _, r := range s {
		switch {
		case r == quote || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < ' ' || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x7f || unicode.IsPrint(r):
			b.WriteRune(r)
		case r < 0x100:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x10000:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteRune(quote)
	return b.String()
}
