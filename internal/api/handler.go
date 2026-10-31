package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"rightcapital/notification/internal/task"
)

const maxBodyBytes = 1 << 20

var (
	codePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
	uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type handler struct {
	store task.Store
	log   *slog.Logger
}

func New(store task.Store, log *slog.Logger) http.Handler {
	h := &handler{store: store, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/notifications", h.accept)
	mux.HandleFunc("GET /v1/notifications/{id}", h.get)
	mux.HandleFunc("GET /v1/notifications", h.list)
	mux.HandleFunc("GET /v1/notifications/{id}/logs", h.logs)
	mux.HandleFunc("POST /v1/notifications/{id}/retry", h.retry)
	mux.HandleFunc("GET /v1/vendors", h.vendors)
	mux.HandleFunc("GET /v1/vendors/{code}", h.getVendor)
	mux.HandleFunc("PUT /v1/vendors/{code}", h.saveVendor)
	mux.HandleFunc("GET /metrics", h.metrics)
	mux.HandleFunc("GET /healthz", h.health)
	return mux
}

func (h *handler) accept(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !validText(keys[0], 128) {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "one non-empty Idempotency-Key of at most 128 characters is required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var submission task.Submission
	if err := decoder.Decode(&submission); err != nil {
		h.decodeError(w, err)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		h.decodeError(w, err)
		return
	}
	if !codePattern.MatchString(submission.SourceSystem) ||
		!codePattern.MatchString(submission.VendorCode) ||
		!validText(submission.EventType, 128) || submission.Payload == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "source_system and vendor_code must be 1-64 letters, digits, dots, underscores or hyphens and start with a letter or digit; event_type must be 1-128 characters; payload must be an object")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := h.store.Accept(ctx, keys[0], submission)
	switch {
	case errors.Is(err, task.ErrConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used with different request content")
	case errors.Is(err, task.ErrUnknownVendor):
		writeError(w, http.StatusUnprocessableEntity, "unknown_vendor", "vendor_code is not configured")
	case errors.Is(err, task.ErrInvalidPayload):
		writeError(w, http.StatusBadRequest, "invalid_payload", "payload cannot contain NUL characters or numbers outside PostgreSQL JSONB limits")
	case err != nil:
		h.unavailable(w, r, err)
	default:
		w.Header().Set("Location", "/v1/notifications/"+result.TaskID)
		writeJSON(w, http.StatusAccepted, result)
	}
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid_task_id", "task_id must be a UUID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := h.store.Get(ctx, id)
	switch {
	case errors.Is(err, task.ErrNotFound):
		writeError(w, http.StatusNotFound, "task_not_found", "task was not found")
	case err != nil:
		h.unavailable(w, r, err)
	default:
		writeJSON(w, http.StatusOK, result)
	}
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.store.Ping(ctx); err != nil {
		h.unavailable(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) decodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body must not exceed 1 MiB")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_json", "body must contain exactly one valid JSON object with known fields")
}

func (h *handler) unavailable(w http.ResponseWriter, r *http.Request, err error) {
	h.log.Error("request storage operation failed", "method", r.Method, "path", r.URL.Path, "error", err)
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, "storage_unavailable", "retry with the same Idempotency-Key if submitting a notification")
}

func validText(value string, max int) bool {
	if value == "" || strings.TrimSpace(value) != value || utf8.RuneCountInString(value) > max {
		return false
	}
	for _, ch := range value {
		if ch < 0x20 || ch == 0x7f {
			return false
		}
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
