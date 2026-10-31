package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rightcapital/notification/internal/task"
	"rightcapital/notification/internal/vendor"
)

func requestJSON(t *testing.T, method, endpoint string, value any, want int, output any) {
	t.Helper()
	var input io.Reader
	if value != nil {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		input = strings.NewReader(string(data))
	}
	r, err := http.NewRequest(method, endpoint, input)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, endpoint, response.StatusCode, want, data)
	}
	if output != nil {
		if err := json.Unmarshal(data, output); err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
	}
}

func TestManagementFailureRepairAndManualRetry(t *testing.T) {
	var fixedCalls atomic.Int32
	var keysMu sync.Mutex
	var keys []string
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		keysMu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		keysMu.Unlock()
		if r.URL.Path != "/fixed" || fixedCalls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	f.exec(`UPDATE vendors SET max_attempts=2 WHERE code='inventory'`)
	id := f.submit("inventory")
	f.process()
	f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
	f.process()
	f.saved(id, "DEAD", 2)
	endpoint := f.receiver.URL
	var page struct {
		Items []task.Task `json:"items"`
	}
	requestJSON(t, "GET", endpoint+"/v1/notifications?status=DEAD&source_system=shop&vendor_code=inventory&limit=1", nil, 200, &page)
	if len(page.Items) != 1 || page.Items[0].ID != id {
		t.Fatalf("failed task not listed: %+v", page)
	}
	requestJSON(t, "GET", endpoint+"/v1/notifications?status=DEAD&limit=1&offset=1", nil, 200, &page)
	if len(page.Items) != 0 {
		t.Fatal("offset ignored")
	}
	var logPage struct {
		Items []task.DeliveryLog `json:"items"`
	}
	requestJSON(t, "GET", endpoint+"/v1/notifications/"+id+"/logs", nil, 200, &logPage)
	if len(logPage.Items) != 2 || logPage.Items[0].Outcome != "RETRYABLE_FAILURE" {
		t.Fatalf("failure logs=%+v", logPage)
	}
	var config vendor.Config
	requestJSON(t, "GET", endpoint+"/v1/vendors/inventory", nil, 200, &config)
	config.EndpointURL = f.vendor.URL + "/fixed"
	requestJSON(t, "PUT", endpoint+"/v1/vendors/inventory", config, 200, nil)
	var accepted task.Accepted
	requestJSON(t, "POST", endpoint+"/v1/notifications/"+id+"/retry", nil, 202, &accepted)
	if accepted.TaskID != id {
		t.Fatal("manual retry changed external idempotency identity")
	}
	requestJSON(t, "POST", endpoint+"/v1/notifications/"+id+"/retry", nil, 409, nil)
	f.process()
	value := f.saved(id, "RETRY_WAIT", 3)
	if value.RetryStartAttempt != 2 {
		t.Fatalf("new budget not started: %+v", value)
	}
	f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
	f.process()
	f.saved(id, "SUCCEEDED", 4)
	requestJSON(t, "POST", endpoint+"/v1/notifications/"+id+"/retry", nil, 409, nil)
	requestJSON(t, "GET", endpoint+"/v1/notifications/"+id+"/logs?after_attempt=2&limit=1", nil, 200, &logPage)
	if len(logPage.Items) != 1 || logPage.Items[0].AttemptNo != 3 || logPage.Items[0].VendorSnapshot.EndpointURL != config.EndpointURL {
		t.Fatalf("new configuration/log pagination failed: %+v", logPage)
	}
	logs := f.logs(id)
	if len(logs) != 4 || logs[0].Snapshot.EndpointURL == config.EndpointURL || logs[3].Outcome != "SUCCESS" {
		t.Fatalf("history overwritten: %+v", logs)
	}
	keysMu.Lock()
	defer keysMu.Unlock()
	if len(keys) != 4 {
		t.Fatalf("calls=%v", keys)
	}
	for _, key := range keys {
		if key != id {
			t.Fatalf("changed key: %v", keys)
		}
	}
	response, err := http.Get(endpoint + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(data), `notification_tasks{status="SUCCEEDED"} 1`) {
		t.Fatalf("metrics=%s err=%v", data, err)
	}
	missing := "00000000-0000-0000-0000-000000000001"
	requestJSON(t, "GET", endpoint+"/v1/notifications/"+missing+"/logs", nil, 404, nil)
	requestJSON(t, "POST", endpoint+"/v1/notifications/"+missing+"/retry", nil, 404, nil)
}

func TestConcurrentManualRetryOnlyRequeuesOnce(t *testing.T) {
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400) })
	id := f.submit("inventory")
	f.process()
	const count = 12
	statuses := make(chan int, count)
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			r, err := http.Post(f.receiver.URL+"/v1/notifications/"+id+"/retry", "", nil)
			if err != nil {
				statuses <- 0
				return
			}
			defer r.Body.Close()
			statuses <- r.StatusCode
		})
	}
	wg.Wait()
	close(statuses)
	accepted := 0
	for status := range statuses {
		if status == 202 {
			accepted++
		} else if status != 409 {
			t.Fatalf("status=%d", status)
		}
	}
	if accepted != 1 {
		t.Fatalf("requeued %d times", accepted)
	}
	f.saved(id, "PENDING", 1)
}

func TestVendorConfigurationCRUDAndValidation(t *testing.T) {
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	endpoint := f.receiver.URL
	var config vendor.Config
	requestJSON(t, "GET", endpoint+"/v1/vendors/ads", nil, 200, &config)
	config.Code = "custom"
	requestJSON(t, "PUT", endpoint+"/v1/vendors/custom", config, 200, nil)
	var saved vendor.Config
	requestJSON(t, "GET", endpoint+"/v1/vendors/custom", nil, 200, &saved)
	if saved != config {
		t.Fatalf("saved=%+v want=%+v", saved, config)
	}
	var list struct {
		Items []vendor.Config `json:"items"`
	}
	requestJSON(t, "GET", endpoint+"/v1/vendors", nil, 200, &list)
	if len(list.Items) != 4 {
		t.Fatalf("vendors=%+v", list)
	}
	for _, change := range []func(*vendor.Config){
		func(c *vendor.Config) { c.BodyTemplate = "{{broken" },
		func(c *vendor.Config) { c.EndpointURL = "file:///tmp/test" },
		func(c *vendor.Config) { c.HTTPMethod = "GET" },
		func(c *vendor.Config) { c.MaxAttempts = 0 },
		func(c *vendor.Config) { c.RetryMaxSeconds = 0 },
		func(c *vendor.Config) { c.Code = "different" },
	} {
		bad := config
		change(&bad)
		requestJSON(t, "PUT", endpoint+"/v1/vendors/custom", bad, 400, nil)
	}
	requestJSON(t, "GET", endpoint+"/v1/vendors/custom", nil, 200, &saved)
	if saved != config {
		t.Fatal("invalid update changed stored configuration")
	}
	requestJSON(t, "GET", endpoint+"/v1/vendors/missing", nil, 404, nil)
	for _, query := range []string{"limit=0", "limit=101", "limit=abc", "offset=-1", "status=BAD", "vendor_code=bad%20code"} {
		requestJSON(t, "GET", endpoint+"/v1/notifications?"+query, nil, 400, nil)
	}
}

func TestManualRetrySchemaUpgrade(t *testing.T) {
	pool, _ := newTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := task.NewPostgres(pool)
	accepted, err := store.Accept(ctx, "before-upgrade", task.Submission{SourceSystem: "shop", VendorCode: "ads", EventType: "signup", Payload: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the old Step 1/2 shape in this isolated schema, with data retained.
	if _, err := pool.Exec(ctx, `ALTER TABLE notification_tasks DROP COLUMN retry_start_attempt`); err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile("../../migrations/003_manual_retry.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	saved, err := store.Get(ctx, accepted.TaskID)
	if err != nil || saved.RetryStartAttempt != 0 || saved.Status != "PENDING" {
		t.Fatalf("upgrade lost data: %+v %v", saved, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE notification_tasks SET retry_start_attempt=-1 WHERE id=$1::uuid`, accepted.TaskID); err == nil {
		t.Fatal(fmt.Errorf("upgrade did not enforce retry budget constraint"))
	}
}
