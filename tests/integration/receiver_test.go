package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rightcapital/notification/internal/api"
	"rightcapital/notification/internal/task"
	"rightcapital/notification/internal/vendor"
)

// This test creates and drops only its own random schema, never public tables.
func TestReceiverPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, sql := newTestDatabase(t)
	store := task.NewPostgres(pool)
	server := httptest.NewServer(api.New(store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 10 * time.Second}

	post := func(key, body string) (int, task.Accepted, error) {
		r, err := http.NewRequest(http.MethodPost, server.URL+"/v1/notifications", strings.NewReader(body))
		if err != nil {
			return 0, task.Accepted{}, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		response, err := client.Do(r)
		if err != nil {
			return 0, task.Accepted{}, err
		}
		defer response.Body.Close()
		var accepted task.Accepted
		err = json.NewDecoder(response.Body).Decode(&accepted)
		return response.StatusCode, accepted, err
	}

	requests := map[string]string{
		"ads":       `{"source_system":"shop","vendor_code":"ads","event_type":"signup","payload":{"user_id":"u1","click_id":"c1"}}`,
		"crm":       `{"source_system":"shop","vendor_code":"crm","event_type":"subscription.paid","payload":{"contact_id":"c1","status":"paid"}}`,
		"inventory": `{"source_system":"shop","vendor_code":"inventory","event_type":"order.paid","payload":{"sku":"sku1","delta":-1,"order_id":9007199254740993}}`,
	}
	for code, body := range requests {
		t.Run("seed and accept "+code, func(t *testing.T) {
			status, accepted, err := post("seed-"+code, body)
			if err != nil || status != 202 || accepted.Duplicate || accepted.Status != "PENDING" {
				t.Fatalf("status=%d accepted=%+v err=%v", status, accepted, err)
			}
			var submission task.Submission
			decoder := json.NewDecoder(strings.NewReader(body))
			decoder.UseNumber()
			if err := decoder.Decode(&submission); err != nil {
				t.Fatal(err)
			}
			var headersTemplate, bodyTemplate string
			if err := pool.QueryRow(ctx, "SELECT headers_template, body_template FROM vendors WHERE code=$1", code).
				Scan(&headersTemplate, &bodyTemplate); err != nil {
				t.Fatal(err)
			}
			headers, rendered, err := vendor.Render(headersTemplate, bodyTemplate, vendor.Data{
				TaskID: accepted.TaskID, SourceSystem: submission.SourceSystem,
				EventType: submission.EventType, Payload: submission.Payload,
			})
			if err != nil || !json.Valid(rendered) || headers.Get("Idempotency-Key") != accepted.TaskID {
				t.Fatalf("headers=%v body=%s err=%v", headers, rendered, err)
			}
			if code == "inventory" && !strings.Contains(string(rendered), "9007199254740993") {
				t.Fatal("large order ID rounded")
			}
		})
	}

	t.Run("concurrent duplicate requests", func(t *testing.T) {
		type result struct {
			accepted task.Accepted
			err      error
		}
		const count = 24
		results := make(chan result, count)
		var wg sync.WaitGroup
		for range count {
			wg.Go(func() {
				status, accepted, err := post("concurrent", requests["ads"])
				if err == nil && status != 202 {
					err = fmt.Errorf("unexpected HTTP status %d", status)
				}
				results <- result{accepted, err}
			})
		}
		wg.Wait()
		close(results)
		id, created := "", 0
		for result := range results {
			if result.err != nil {
				t.Fatal(result.err)
			}
			if id == "" {
				id = result.accepted.TaskID
			}
			if result.accepted.TaskID != id {
				t.Fatal("same request created multiple tasks")
			}
			if !result.accepted.Duplicate {
				created++
			}
		}
		var rows int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM notification_tasks WHERE idempotency_key='concurrent'").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if created != 1 || rows != 1 {
			t.Fatalf("created=%d rows=%d", created, rows)
		}
		// A fresh connection pool represents a restarted API process.
		fresh, err := pgxpool.NewWithConfig(ctx, pool.Config().Copy())
		if err != nil {
			t.Fatal(err)
		}
		defer fresh.Close()
		saved, err := task.NewPostgres(fresh).Get(ctx, id)
		if err != nil || saved.ID != id || saved.Status != "PENDING" {
			t.Fatalf("persisted task=%+v err=%v", saved, err)
		}
		response, err := client.Get(server.URL + "/v1/notifications/" + id)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("GET status=%d", response.StatusCode)
		}
		_, _ = io.Copy(io.Discard, response.Body)
	})

	t.Run("conflict and JSON key reordering", func(t *testing.T) {
		_, original, err := post("compare", requests["ads"])
		if err != nil {
			t.Fatal(err)
		}
		reordered := `{"payload":{"click_id":"c1","user_id":"u1"},"event_type":"signup","vendor_code":"ads","source_system":"shop"}`
		status, duplicate, err := post("compare", reordered)
		if err != nil || status != 202 || !duplicate.Duplicate || duplicate.TaskID != original.TaskID {
			t.Fatalf("status=%d accepted=%+v err=%v", status, duplicate, err)
		}
		status, _, err = post("compare", strings.Replace(requests["ads"], `"u1"`, `"u2"`, 1))
		if err != nil || status != 409 {
			t.Fatalf("conflict status=%d err=%v", status, err)
		}
		status, separate, err := post("compare", strings.Replace(requests["ads"], `"shop"`, `"another-system"`, 1))
		if err != nil || status != 202 || separate.TaskID == original.TaskID {
			t.Fatalf("source isolation status=%d accepted=%+v err=%v", status, separate, err)
		}
	})

	t.Run("unknown vendor", func(t *testing.T) {
		status, _, err := post("unknown-vendor", strings.Replace(requests["ads"], `"ads"`, `"missing"`, 1))
		if err != nil || status != 422 {
			t.Fatalf("status=%d err=%v", status, err)
		}
	})

	t.Run("JSON outside PostgreSQL limits is a client error", func(t *testing.T) {
		for name, value := range map[string]string{
			"nul": `"bad\u0000value"`, "numeric-overflow": `1e1000000`,
		} {
			body := strings.Replace(requests["ads"], `"u1"`, value, 1)
			status, _, err := post("invalid-jsonb-"+name, body)
			if err != nil || status != 400 {
				t.Fatalf("%s status=%d err=%v", name, status, err)
			}
		}
	})

	t.Run("bad configuration does not block acceptance and seed preserves edits", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "UPDATE vendors SET body_template='{{broken' WHERE code='ads'"); err != nil {
			t.Fatal(err)
		}
		status, accepted, err := post("bad-config", requests["ads"])
		if err != nil || status != 202 || accepted.Status != "PENDING" {
			t.Fatalf("status=%d accepted=%+v err=%v", status, accepted, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
		var saved string
		if err := pool.QueryRow(ctx, "SELECT body_template FROM vendors WHERE code='ads'").Scan(&saved); err != nil || saved != "{{broken" {
			t.Fatalf("schema reset vendor edit: %q %v", saved, err)
		}
		var logs int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_logs").Scan(&logs); err != nil || logs != 0 {
			t.Fatalf("receiver must not perform delivery: logs=%d err=%v", logs, err)
		}
	})
}
