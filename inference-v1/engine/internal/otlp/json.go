package otlp

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// idFields are the byte fields OTLP/JSON writes as hex. Standard protobuf
// JSON expects base64 for bytes, which is the one place the two differ.
var idFields = map[string]bool{
	"traceId": true, "spanId": true, "parentSpanId": true,
	"trace_id": true, "span_id": true, "parent_span_id": true,
}

// unmarshalJSON decodes an OTLP/JSON export request.
func unmarshalJSON(data []byte, req *coltracepb.ExportTraceServiceRequest) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // keep 64-bit timestamps exact
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return err
	}
	if err := hexIDsToBase64(doc); err != nil {
		return err
	}
	fixed, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(fixed, req)
}

func hexIDsToBase64(node any) error {
	switch v := node.(type) {
	case map[string]any:
		for key, child := range v {
			if s, ok := child.(string); ok && idFields[key] {
				raw, err := hex.DecodeString(s)
				if err != nil {
					return fmt.Errorf("%s %q is not hex", key, s)
				}
				v[key] = base64.StdEncoding.EncodeToString(raw)
				continue
			}
			if err := hexIDsToBase64(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := hexIDsToBase64(child); err != nil {
				return err
			}
		}
	}
	return nil
}
