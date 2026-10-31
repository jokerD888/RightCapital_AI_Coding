package vendor

import (
	"encoding/json"
	"testing"
)

func TestRenderEscapesAndPreservesNumbers(t *testing.T) {
	data := Data{TaskID: "stable-task-id", Payload: map[string]any{
		"name": "quoted \"value\"\n下一行", "quantity": json.Number("9007199254740993"),
	}}
	headers, body, err := Render(`{"Idempotency-Key":{{json .TaskID}}}`,
		`{"name":{{json .Payload.name}},"quantity":{{json .Payload.quantity}}}`, data)
	if err != nil {
		t.Fatal(err)
	}
	if headers.Get("Idempotency-Key") != data.TaskID || !json.Valid(body) {
		t.Fatalf("headers=%v body=%s", headers, body)
	}
	var rendered map[string]json.RawMessage
	if err := json.Unmarshal(body, &rendered); err != nil {
		t.Fatal(err)
	}
	if string(rendered["quantity"]) != "9007199254740993" {
		t.Fatalf("number rounded: %s", body)
	}
	var name string
	if err := json.Unmarshal(rendered["name"], &name); err != nil || name != data.Payload["name"] {
		t.Fatalf("text did not roundtrip: %s, %v", body, err)
	}
}

func TestRenderRejectsInvalidTemplates(t *testing.T) {
	for _, tt := range []struct{ name, headers, body string }{
		{"missing field", `{}`, `{"name":{{json .Payload.missing}}}`},
		{"syntax error", `{}`, `{{`},
		{"invalid JSON", `{}`, `not json`},
		{"null headers", `null`, `{}`},
		{"non-string header", `{"X-Value":1}`, `{}`},
		{"null header value", `{"X-Value":null}`, `{}`},
		{"newline header", `{"X-Value":"bad\r\nInjected: true"}`, `{}`},
		{"bad header name", `{"invalid name":"x"}`, `{}`},
		{"duplicate header casing", `{"X-Test":"1","x-test":"2"}`, `{}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Render(tt.headers, tt.body, Data{Payload: map[string]any{}})
			if err == nil {
				t.Fatal("expected render failure")
			}
		})
	}
}
