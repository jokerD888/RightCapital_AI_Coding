package mockvendor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFailureThenIdempotentSuccess(t *testing.T) {
	h := New(1)
	for _, status := range []int{503, 200, 200} {
		r := httptest.NewRequest("POST", "/inventory/adjustments", strings.NewReader(`{"delta":-1}`))
		r.Header.Set("Idempotency-Key", "task-1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("status=%d want=%d", w.Code, status)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/inventory/adjustments", strings.NewReader(`{"delta":-2}`))
	r.Header.Set("Idempotency-Key", "task-1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal("changed payload accepted for same key")
	}
}
