package vendor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"text/template"
)

// Data is rebuilt for each attempt. TaskID is the stable outbound idempotency key.
type Data struct {
	TaskID       string
	SourceSystem string
	EventType    string
	Payload      map[string]any
}

// Render uses Go text/template directly. Receivers do not call this function;
// the Worker will render the latest vendor configuration on every attempt.
func Render(headersTemplate, bodyTemplate string, data Data) (http.Header, []byte, error) {
	headerBytes, err := execute("headers", headersTemplate, data)
	if err != nil {
		return nil, nil, err
	}
	var values map[string]any
	if err := json.Unmarshal(headerBytes, &values); err != nil || values == nil {
		return nil, nil, fmt.Errorf("headers template must render a JSON object of strings")
	}
	headers := make(http.Header)
	for key, raw := range values {
		value, ok := raw.(string)
		if !ok {
			return nil, nil, fmt.Errorf("rendered HTTP header %q must be a string", key)
		}
		if !validHeaderName(key) || !validHeaderValue(value) {
			return nil, nil, fmt.Errorf("invalid rendered HTTP header %q", key)
		}
		canonical := http.CanonicalHeaderKey(key)
		if _, exists := headers[canonical]; exists {
			return nil, nil, fmt.Errorf("duplicate rendered HTTP header %q", canonical)
		}
		headers.Set(canonical, value)
	}
	body, err := execute("body", bodyTemplate, data)
	if err != nil {
		return nil, nil, err
	}
	if !json.Valid(body) {
		return nil, nil, fmt.Errorf("body template must render valid JSON")
	}
	return headers, body, nil
}

func execute(name, source string, data Data) ([]byte, error) {
	tpl, err := parse(name, source)
	if err != nil {
		return nil, fmt.Errorf("parse %s template: %w", name, err)
	}
	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return nil, fmt.Errorf("execute %s template: %w", name, err)
	}
	return out.Bytes(), nil
}

func parse(name, source string) (*template.Template, error) {
	return template.New(name).Option("missingkey=error").Funcs(template.FuncMap{
		"json": func(value any) (string, error) {
			encoded, err := json.Marshal(value)
			return string(encoded), err
		},
	}).Parse(source)
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, ch := range name {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", ch)) {
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] == 0x7f || (value[i] < 0x20 && value[i] != '\t') {
			return false
		}
	}
	return true
}
