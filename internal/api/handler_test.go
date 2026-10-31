package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"rightcapital/notification/internal/task"
	"rightcapital/notification/internal/vendor"
)

const validRequest = `{"source_system":"shop","vendor_code":"inventory","event_type":"order.paid","payload":{"sku":"SKU-1","delta":-1,"order_id":9007199254740993}}`

type fakeStore struct {
	acceptCalls int
	key         string
	submission  task.Submission
	accepted    task.Accepted
	acceptErr   error
	getErr      error
	pingErr     error
}

func (s *fakeStore) Accept(_ context.Context, key string, submission task.Submission) (task.Accepted, error) {
	s.acceptCalls++
	s.key, s.submission = key, submission
	return s.accepted, s.acceptErr
}

func (s *fakeStore) Get(_ context.Context, id string) (task.Task, error) {
	return task.Task{ID: id, Status: "PENDING"}, s.getErr
}

func (s *fakeStore) Ping(context.Context) error { return s.pingErr }

func (s *fakeStore) List(context.Context, task.Filter) ([]task.Task, error) {
	return []task.Task{}, nil
}
func (s *fakeStore) Logs(context.Context, string, int, int) ([]task.DeliveryLog, error) {
	return []task.DeliveryLog{}, s.getErr
}
func (s *fakeStore) Retry(context.Context, string) (task.Accepted, error) {
	return s.accepted, s.acceptErr
}
func (s *fakeStore) Vendors(context.Context) ([]vendor.Config, error) { return []vendor.Config{}, nil }
func (s *fakeStore) SaveVendor(context.Context, vendor.Config) error  { return s.acceptErr }
func (s *fakeStore) Stats(context.Context) (task.Stats, error)        { return task.Stats{}, s.pingErr }

func testHandler(store *fakeStore) http.Handler {
	return New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestAcceptValidation(t *testing.T) {
	tests := []struct {
		name, body, contentType, key string
		want                         int
	}{
		{"missing key", validRequest, "application/json", "", 400},
		{"long key", validRequest, "application/json", strings.Repeat("x", 129), 400},
		{"wrong content type", validRequest, "text/plain", "key", 415},
		{"broken JSON", `{"source_system":`, "application/json", "key", 400},
		{"two documents", validRequest + `{}`, "application/json", "key", 400},
		{"null request", `null`, "application/json", "key", 400},
		{"missing payload", `{"source_system":"shop","vendor_code":"ads","event_type":"signup"}`, "application/json", "key", 400},
		{"array payload", `{"source_system":"shop","vendor_code":"ads","event_type":"signup","payload":[]}`, "application/json", "key", 400},
		{"null payload", `{"source_system":"shop","vendor_code":"ads","event_type":"signup","payload":null}`, "application/json", "key", 400},
		{"unknown field", strings.Replace(validRequest, `"source_system":`, `"unexpected":true,"source_system":`, 1), "application/json", "key", 400},
		{"invalid source", strings.Replace(validRequest, `"shop"`, `"shop space"`, 1), "application/json", "key", 400},
		{"blank event", strings.Replace(validRequest, `"order.paid"`, `" "`, 1), "application/json", "key", 400},
		{"oversize body", `{"source_system":"` + strings.Repeat("x", maxBodyBytes) + `"}`, "application/json", "key", 413},
		{"oversize trailing whitespace", validRequest + strings.Repeat(" ", maxBodyBytes), "application/json", "key", 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			r := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.contentType)
			if tt.key != "" {
				r.Header.Set("Idempotency-Key", tt.key)
			}
			w := httptest.NewRecorder()
			testHandler(store).ServeHTTP(w, r)
			if w.Code != tt.want || store.acceptCalls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, store.acceptCalls, w.Body.String())
			}
		})
	}
}

func TestAcceptResults(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{
		{"accepted", nil, 202},
		{"conflicting payload", task.ErrConflict, 409},
		{"unknown vendor", task.ErrUnknownVendor, 422},
		{"unsupported payload", task.ErrInvalidPayload, 400},
		{"database failure", errors.New("database offline"), 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{acceptErr: tt.err, accepted: task.Accepted{TaskID: "task-id", Status: "PENDING"}}
			r := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(validRequest))
			r.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.Header.Set("Idempotency-Key", "order-123")
			w := httptest.NewRecorder()
			testHandler(store).ServeHTTP(w, r)
			if w.Code != tt.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if store.key != "order-123" || store.submission.Payload["order_id"] != json.Number("9007199254740993") {
				t.Fatalf("request lost key or number precision: %+v", store)
			}
			if tt.want == 202 && w.Header().Get("Location") != "/v1/notifications/task-id" {
				t.Fatal("missing task Location")
			}
			if tt.want == 503 && w.Header().Get("Retry-After") == "" {
				t.Fatal("missing retry hint")
			}
		})
	}
}

func TestReadAndHealth(t *testing.T) {
	for _, tt := range []struct {
		name, path string
		store      fakeStore
		want       int
	}{
		{"task", "/v1/notifications/00000000-0000-0000-0000-000000000001", fakeStore{}, 200},
		{"invalid id", "/v1/notifications/not-a-uuid", fakeStore{}, 400},
		{"missing task", "/v1/notifications/00000000-0000-0000-0000-000000000001", fakeStore{getErr: task.ErrNotFound}, 404},
		{"get storage down", "/v1/notifications/00000000-0000-0000-0000-000000000001", fakeStore{getErr: errors.New("offline")}, 503},
		{"healthy", "/healthz", fakeStore{}, 200},
		{"unhealthy", "/healthz", fakeStore{pingErr: errors.New("offline")}, 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			testHandler(&tt.store).ServeHTTP(w, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if w.Code != tt.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
