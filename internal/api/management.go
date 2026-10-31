package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"rightcapital/notification/internal/task"
	"rightcapital/notification/internal/vendor"
)

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := queryInt(r, "limit", 50, 1, 100)
	if err != nil {
		writeError(w, 400, "invalid_query", err.Error())
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, 1000000)
	if err != nil {
		writeError(w, 400, "invalid_query", err.Error())
		return
	}
	state := q.Get("status")
	if state != "" && state != "PENDING" && state != "PROCESSING" && state != "RETRY_WAIT" && state != "SUCCEEDED" && state != "DEAD" {
		writeError(w, 400, "invalid_query", "unknown status")
		return
	}
	for _, name := range []string{"source_system", "vendor_code"} {
		if q.Get(name) != "" && !codePattern.MatchString(q.Get(name)) {
			writeError(w, 400, "invalid_query", "invalid "+name)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := h.store.List(ctx, task.Filter{Status: state, VendorCode: q.Get("vendor_code"),
		SourceSystem: q.Get("source_system"), Limit: limit, Offset: offset})
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func (h *handler) logs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, 400, "invalid_task_id", "task_id must be a UUID")
		return
	}
	limit, err := queryInt(r, "limit", 100, 1, 100)
	if err != nil {
		writeError(w, 400, "invalid_query", err.Error())
		return
	}
	after, err := queryInt(r, "after_attempt", 0, 0, 2147483647)
	if err != nil {
		writeError(w, 400, "invalid_query", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := h.store.Logs(ctx, id, after, limit)
	if errors.Is(err, task.ErrNotFound) {
		writeError(w, 404, "task_not_found", "task was not found")
		return
	}
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "limit": limit, "after_attempt": after})
}

func (h *handler) retry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, 400, "invalid_task_id", "task_id must be a UUID")
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) > 0 {
		writeError(w, 400, "invalid_request", "retry expects an empty body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := h.store.Retry(ctx, id)
	switch {
	case errors.Is(err, task.ErrNotFound):
		writeError(w, 404, "task_not_found", "task was not found")
	case errors.Is(err, task.ErrNotDead):
		writeError(w, 409, "task_not_dead", "only DEAD tasks can be retried manually")
	case err != nil:
		h.unavailable(w, r, err)
	default:
		h.log.Info("task manually requeued", "task_id", id)
		w.Header().Set("Location", "/v1/notifications/"+id)
		writeJSON(w, 202, result)
	}
}

func (h *handler) vendors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := h.store.Vendors(ctx)
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (h *handler) getVendor(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !codePattern.MatchString(code) {
		writeError(w, 400, "invalid_vendor", "invalid vendor code")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := h.store.Vendors(ctx)
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	for _, item := range items {
		if item.Code == code {
			writeJSON(w, 200, item)
			return
		}
	}
	writeError(w, 404, "unknown_vendor", "vendor is not configured")
}

func (h *handler) saveVendor(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !codePattern.MatchString(code) {
		writeError(w, 400, "invalid_vendor", "invalid vendor code")
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, 415, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var config vendor.Config
	if err := decoder.Decode(&config); err != nil {
		h.decodeError(w, err)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		h.decodeError(w, err)
		return
	}
	if config.Code != "" && config.Code != code {
		writeError(w, 400, "invalid_vendor", "body code must match URL code")
		return
	}
	config.Code = code
	if err := config.Validate(); err != nil {
		writeError(w, 400, "invalid_vendor", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.store.SaveVendor(ctx, config); err != nil {
		h.unavailable(w, r, err)
		return
	}
	h.log.Info("vendor configuration saved", "vendor_code", code)
	writeJSON(w, 200, config)
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	stats, err := h.store.Stats(ctx)
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	var out strings.Builder
	out.WriteString("# HELP notification_tasks Current tasks by status.\n# TYPE notification_tasks gauge\n")
	for _, state := range []string{"PENDING", "PROCESSING", "RETRY_WAIT", "SUCCEEDED", "DEAD"} {
		fmt.Fprintf(&out, "notification_tasks{status=%q} %d\n", state, stats.Tasks[state])
	}
	out.WriteString("# HELP notification_oldest_pending_age_seconds Age of oldest waiting task.\n# TYPE notification_oldest_pending_age_seconds gauge\n")
	fmt.Fprintf(&out, "notification_oldest_pending_age_seconds %.3f\n", stats.OldestPendingAgeSeconds)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(200)
	_, _ = io.WriteString(w, out.String())
}

func queryInt(r *http.Request, name string, fallback, min, max int) (int, error) {
	text := r.URL.Query().Get(name)
	if text == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s must be %d-%d", name, min, max)
	}
	return value, nil
}
