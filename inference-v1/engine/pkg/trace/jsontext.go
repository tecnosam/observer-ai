package trace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// compactJSON writes a JSON value on one line: no spaces, strings escaped
// only where JSON requires it (quote, backslash, control characters) and
// every other character as itself, keys in their original order (a key
// given twice keeps its first place and its last value), and numbers as
// written. It is what the training code's json.dumps(value,
// ensure_ascii=False, separators=(",", ":")) prints for the same value.
func compactJSON(raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return readValue(dec)
}

func readValue(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	switch t := tok.(type) {
	case json.Delim:
		if t == '{' {
			return readObject(dec)
		}
		return readArray(dec)
	case string:
		return quoteJSON(t), nil
	case json.Number:
		return string(t), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "null", nil
	}
}

func readObject(dec *json.Decoder) (string, error) {
	var keys []string
	values := map[string]string{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return "", err
		}
		key := tok.(string) // the decoder only gives strings as keys
		value, err := readValue(dec)
		if err != nil {
			return "", err
		}
		if _, seen := values[key]; !seen {
			keys = append(keys, key)
		}
		values[key] = value
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return "", err
	}
	members := make([]string, len(keys))
	for i, k := range keys {
		members[i] = quoteJSON(k) + ":" + values[k]
	}
	return "{" + strings.Join(members, ",") + "}", nil
}

func readArray(dec *json.Decoder) (string, error) {
	var items []string
	for dec.More() {
		item, err := readValue(dec)
		if err != nil {
			return "", err
		}
		items = append(items, item)
	}
	if _, err := dec.Token(); err != nil { // the closing bracket
		return "", err
	}
	return "[" + strings.Join(items, ",") + "]", nil
}

// quoteJSON quotes s, escaping what JSON requires and nothing else.
func quoteJSON(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
