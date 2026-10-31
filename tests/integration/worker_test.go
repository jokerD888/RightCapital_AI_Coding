package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"

	"rightcapital/notification/internal/api"
	"rightcapital/notification/internal/task"
	"rightcapital/notification/internal/vendor"
	"rightcapital/notification/internal/worker"
)

type workerFixture struct {
	t        *testing.T
	ctx      context.Context
	pool     *pgxpool.Pool
	store    *task.Postgres
	jobs     *worker.Worker
	receiver *httptest.Server
	vendor   *httptest.Server
	sequence int
}

type deliveryLog struct {
	Attempt    int
	Outcome    string
	HTTPStatus *int
	Excerpt    *string
	Error      *string
	Snapshot   vendor.Config
}

func newWorkerFixture(t *testing.T, endpoint http.HandlerFunc) *workerFixture {
	t.Helper()
	pool, _ := newTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	f := &workerFixture{t: t, ctx: ctx, pool: pool, store: task.NewPostgres(pool)}
	f.vendor = httptest.NewServer(endpoint)
	t.Cleanup(f.vendor.Close)
	f.exec(`UPDATE vendors SET endpoint_url=$1 || '/' || code,
		retry_base_seconds=1, retry_max_seconds=2, max_attempts=3, timeout_ms=2000`, f.vendor.URL)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.receiver = httptest.NewServer(api.New(f.store, logger))
	t.Cleanup(f.receiver.Close)
	var err error
	f.jobs, err = worker.New(pool, logger, worker.Options{
		Concurrency: 4, PollInterval: 10 * time.Millisecond, RecoveryInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.jobs.CloseIdleConnections)
	return f
}

func (f *workerFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *workerFixture) submit(code string) string {
	f.t.Helper()
	f.sequence++
	payloads := map[string]string{
		"ads":       `{"user_id":"u1","click_id":"c1"}`,
		"crm":       `{"contact_id":"c1","status":"paid"}`,
		"inventory": `{"sku":"sku1","delta":-2,"order_id":9007199254740993}`,
	}
	body := fmt.Sprintf(`{"source_system":"shop","vendor_code":%q,"event_type":"order.paid","payload":%s}`, code, payloads[code])
	r, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.receiver.URL+"/v1/notifications", strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", fmt.Sprintf("notification-%d", f.sequence))
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	var accepted task.Accepted
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil || response.StatusCode != 202 {
		f.t.Fatalf("submit status=%d accepted=%+v err=%v", response.StatusCode, accepted, err)
	}
	return accepted.TaskID
}

func (f *workerFixture) process() {
	f.t.Helper()
	worked, err := f.jobs.ProcessOne(f.ctx)
	if err != nil || !worked {
		f.t.Fatalf("process worked=%v err=%v", worked, err)
	}
}

func (f *workerFixture) saved(id, status string, attempts int) task.Task {
	f.t.Helper()
	value, err := f.store.Get(f.ctx, id)
	if err != nil || value.Status != status || value.AttemptCount != attempts {
		f.t.Fatalf("task=%+v want status=%s attempts=%d err=%v", value, status, attempts, err)
	}
	var noLease bool
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_token IS NULL AND lease_until IS NULL FROM notification_tasks WHERE id=$1::uuid`, id).Scan(&noLease); err != nil {
		f.t.Fatal(err)
	}
	if status != "PROCESSING" && !noLease {
		f.t.Fatal("completed attempt retained lease")
	}
	return value
}

func (f *workerFixture) logs(id string) []deliveryLog {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `SELECT attempt_no, outcome, http_status,
		response_excerpt, error_message, vendor_snapshot
		FROM delivery_logs WHERE task_id=$1::uuid ORDER BY attempt_no`, id)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var logs []deliveryLog
	for rows.Next() {
		var entry deliveryLog
		var snapshot []byte
		if err := rows.Scan(&entry.Attempt, &entry.Outcome, &entry.HTTPStatus, &entry.Excerpt, &entry.Error, &snapshot); err != nil {
			f.t.Fatal(err)
		}
		if err := json.Unmarshal(snapshot, &entry.Snapshot); err != nil {
			f.t.Fatal(err)
		}
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return logs
}

func (f *workerFixture) start(jobs *worker.Worker) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() { defer close(done); jobs.Run(ctx) }()
	f.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			f.t.Error("worker did not stop after cancellation")
		}
	})
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestWorkerDeliversAllVendorsOutsideTransaction(t *testing.T) {
	type received struct {
		method, path, key string
		body              string
	}
	requests := make(chan received, 3)
	var f *workerFixture
	f = newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			t.Error(err)
		}
		id := r.Header.Get("Idempotency-Key")
		// The STARTED log must already be committed, and the task row must be
		// lockable by another transaction while the HTTP request is in flight.
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		defer tx.Rollback(ctx)
		var state string
		err = tx.QueryRow(ctx, `SELECT status FROM notification_tasks WHERE id=$1::uuid FOR UPDATE NOWAIT`, id).Scan(&state)
		if err != nil || state != "PROCESSING" {
			t.Errorf("task locked during HTTP or uncommitted claim: state=%s err=%v", state, err)
		}
		var started int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM delivery_logs WHERE task_id=$1::uuid AND outcome='STARTED'`, id).Scan(&started); err != nil || started != 1 {
			t.Errorf("attempt log not committed before HTTP: count=%d err=%v", started, err)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing rendered Content-Type")
		}
		requests <- received{r.Method, r.URL.Path, id, string(body)}
		w.WriteHeader(http.StatusNoContent)
	})
	for _, code := range []string{"ads", "crm", "inventory"} {
		id := f.submit(code)
		f.process()
		f.saved(id, "SUCCEEDED", 1)
		logs := f.logs(id)
		if len(logs) != 1 || logs[0].Outcome != "SUCCESS" || *logs[0].HTTPStatus != 204 || logs[0].Snapshot.EndpointURL != f.vendor.URL+"/"+code {
			t.Fatalf("invalid delivery log: %+v", logs)
		}
		request := <-requests
		wantMethod := "POST"
		if code == "crm" {
			wantMethod = "PATCH"
		}
		if request.key != id || request.method != wantMethod || request.path != "/"+code || !json.Valid([]byte(request.body)) {
			t.Fatalf("invalid request: %+v", request)
		}
		if code == "inventory" && !strings.Contains(request.body, "9007199254740993") {
			t.Fatal("large order ID rounded during delivery")
		}
	}
}

func TestWorkerPollingRetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var keys []string
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		if calls.Add(1) < 3 {
			http.Error(w, "temporary outage", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	id := f.submit("inventory")
	f.start(f.jobs)
	eventually(t, func() bool {
		value, err := f.store.Get(f.ctx, id)
		return err == nil && value.Status == "SUCCEEDED"
	})
	f.saved(id, "SUCCEEDED", 3)
	logs := f.logs(id)
	if len(logs) != 3 || logs[0].Outcome != "RETRYABLE_FAILURE" || logs[1].Outcome != "RETRYABLE_FAILURE" || logs[2].Outcome != "SUCCESS" {
		t.Fatalf("logs=%+v", logs)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls.Load() != 3 || len(keys) != 3 || keys[0] != id || keys[1] != id || keys[2] != id {
		t.Fatalf("unstable idempotency key or extra sends: %v", keys)
	}
	// Verify real polling respected the persisted schedule, without changing time.
	var tooEarly int
	err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM (
		SELECT started_at, lag(finished_at) OVER (ORDER BY attempt_no) AS previous_end,
		       attempt_no FROM delivery_logs WHERE task_id=$1::uuid
	) x WHERE attempt_no > 1 AND started_at < previous_end +
	    CASE WHEN attempt_no=2 THEN interval '490 milliseconds' ELSE interval '990 milliseconds' END`, id).Scan(&tooEarly)
	if err != nil || tooEarly != 0 {
		t.Fatalf("retry ignored backoff: early=%d err=%v", tooEarly, err)
	}
}

func TestWorkerUsesFixedLatestConfigurationOnRetry(t *testing.T) {
	var calls atomic.Int32
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/fixed" || r.Header.Get("X-Fixed") != "yes" {
			t.Error("retry used stale endpoint or headers")
		}
		w.WriteHeader(200)
	})
	f.exec(`UPDATE vendors SET body_template='{{broken' WHERE code='inventory'`)
	id := f.submit("inventory")
	f.process()
	f.saved(id, "RETRY_WAIT", 1)
	if calls.Load() != 0 {
		t.Fatal("invalid template sent an HTTP request")
	}
	if worked, err := f.jobs.ProcessOne(f.ctx); err != nil || worked {
		t.Fatalf("future retry was claimed early: worked=%v err=%v", worked, err)
	}
	f.exec(`UPDATE vendors SET endpoint_url=$1, body_template=$2, headers_template=$3 WHERE code='inventory'`,
		f.vendor.URL+"/fixed", `{"sku":{{json .Payload.sku}}}`, `{"X-Fixed":"yes","Idempotency-Key":{{json .TaskID}}}`)
	f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
	f.process()
	f.saved(id, "SUCCEEDED", 2)
	logs := f.logs(id)
	if len(logs) != 2 || logs[0].Snapshot.BodyTemplate != "{{broken" || logs[1].Snapshot.EndpointURL != f.vendor.URL+"/fixed" || logs[1].Snapshot.BodyTemplate == "{{broken" {
		t.Fatalf("configuration snapshots lost history or stayed stale: %+v", logs)
	}
}

func TestWorkerFailureClassificationAndExhaustion(t *testing.T) {
	for _, status := range []int{400, 302, 429, 503} {
		t.Run(fmt.Sprintf("HTTP %d", status), func(t *testing.T) {
			var calls atomic.Int32
			f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path == "/redirected" {
					t.Error("unexpected automatic redirect")
				}
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(status)
			})
			f.exec(`UPDATE vendors SET max_attempts=2 WHERE code='inventory'`)
			id := f.submit("inventory")
			f.process()
			attempts, outcome := 1, "PERMANENT_FAILURE"
			if status == 429 || status == 503 {
				f.saved(id, "RETRY_WAIT", 1)
				f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
				f.process()
				attempts, outcome = 2, "RETRYABLE_FAILURE"
			}
			value := f.saved(id, "DEAD", attempts)
			if value.CompletedAt == nil || value.LastError == nil || calls.Load() != int32(attempts) {
				t.Fatalf("terminal task=%+v calls=%d", value, calls.Load())
			}
			logs := f.logs(id)
			if len(logs) != attempts || logs[attempts-1].Outcome != outcome {
				t.Fatalf("logs=%+v", logs)
			}
			if worked, err := f.jobs.ProcessOne(f.ctx); err != nil || worked {
				t.Fatalf("dead task was retried: %v %v", worked, err)
			}
		})
	}
}

func TestWorkerRespectsRetryAfter(t *testing.T) {
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(429)
	})
	id := f.submit("inventory")
	f.process()
	f.saved(id, "RETRY_WAIT", 1)
	var seconds float64
	if err := f.pool.QueryRow(f.ctx, `SELECT extract(epoch FROM t.next_attempt_at-l.finished_at)
		FROM notification_tasks t JOIN delivery_logs l ON l.task_id=t.id WHERE t.id=$1::uuid`, id).Scan(&seconds); err != nil || seconds < 4.9 {
		t.Fatalf("Retry-After ignored: seconds=%f err=%v", seconds, err)
	}
}

func TestWorkerHTTPTimeoutThenRetry(t *testing.T) {
	var calls atomic.Int32
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			return
		}
		w.WriteHeader(200)
	})
	f.exec(`UPDATE vendors SET timeout_ms=100 WHERE code='inventory'`)
	id := f.submit("inventory")
	f.process()
	f.saved(id, "RETRY_WAIT", 1)
	logs := f.logs(id)
	if len(logs) != 1 || logs[0].HTTPStatus != nil || logs[0].Error == nil || !strings.Contains(*logs[0].Error, "deadline exceeded") {
		t.Fatalf("timeout not recorded: %+v", logs)
	}
	f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
	f.process()
	f.saved(id, "SUCCEEDED", 2)
}

func TestWorkerBoundsHugeAndBinaryResponse(t *testing.T) {
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		chunk := bytes.Repeat([]byte{0xff, 0, 'x', 'y'}, 4096)
		for range 128 {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	})
	id := f.submit("inventory")
	f.process()
	f.saved(id, "SUCCEEDED", 1)
	logs := f.logs(id)
	if len(logs) != 1 || logs[0].Excerpt == nil {
		t.Fatalf("missing response excerpt: %+v", logs)
	}
	excerpt := *logs[0].Excerpt
	if len(excerpt) > (4<<10)+len(" [truncated]") || !utf8.ValidString(excerpt) || strings.ContainsRune(excerpt, '\x00') || !strings.HasSuffix(excerpt, " [truncated]") {
		t.Fatalf("excerpt not bounded or database-safe: length=%d", len(excerpt))
	}
}

func TestWorkerSkipsLockedTask(t *testing.T) {
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	first, second := f.submit("inventory"), f.submit("inventory")
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err := tx.Exec(f.ctx, `SELECT id FROM notification_tasks WHERE id=$1::uuid FOR UPDATE`, first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	if worked, err := f.jobs.ProcessOne(ctx); err != nil || !worked {
		t.Fatalf("worker blocked instead of skipping locked row: %v %v", worked, err)
	}
	f.saved(second, "SUCCEEDED", 1)
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.process()
	f.saved(first, "SUCCEEDED", 1)
}

func TestConcurrentWorkersDoNotClaimSameTask(t *testing.T) {
	var mu sync.Mutex
	counts := make(map[string]int)
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.Header.Get("Idempotency-Key")]++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(200)
	})
	const count = 20
	ids := make([]string, count)
	for i := range ids {
		ids[i] = f.submit("inventory")
	}
	other, err := worker.New(f.pool, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Options{PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.CloseIdleConnections)
	f.start(f.jobs)
	f.start(other)
	eventually(t, func() bool {
		var succeeded int
		err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM notification_tasks WHERE status='SUCCEEDED'`).Scan(&succeeded)
		return err == nil && succeeded == count
	})
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if counts[id] != 1 {
			t.Fatalf("task %s sent %d times", id, counts[id])
		}
		f.saved(id, "SUCCEEDED", 1)
	}
}

func TestLeaseRecoveryAndLateResultFencing(t *testing.T) {
	for _, maxAttempts := range []int{1, 3} {
		t.Run(fmt.Sprintf("max_attempts=%d", maxAttempts), func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			var calls atomic.Int32
			f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					started <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
					}
				}
				w.WriteHeader(200)
			})
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			f.exec(`UPDATE vendors SET max_attempts=$1 WHERE code='inventory'`, maxAttempts)
			id := f.submit("inventory")
			finished := make(chan error, 1)
			go func() { _, err := f.jobs.ProcessOne(f.ctx); finished <- err }()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("first delivery did not start")
			}
			// Expire the persisted lease rather than waiting for its 30s margin.
			f.exec(`UPDATE notification_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1::uuid`, id)
			if recovered, err := f.jobs.RecoverExpired(f.ctx); err != nil || recovered != 1 {
				t.Fatalf("recovered=%d err=%v", recovered, err)
			}
			if recovered, err := f.jobs.RecoverExpired(f.ctx); err != nil || recovered != 0 {
				t.Fatalf("lease recovered twice: recovered=%d err=%v", recovered, err)
			}
			if maxAttempts == 1 {
				f.saved(id, "DEAD", 1)
			} else {
				f.saved(id, "RETRY_WAIT", 1)
				f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
				f.process()
				f.saved(id, "SUCCEEDED", 2)
			}
			once.Do(func() { close(release) })
			select {
			case err := <-finished:
				if !errors.Is(err, worker.ErrLeaseLost) {
					t.Fatalf("late completion was not fenced: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("late worker did not finish")
			}
			logs := f.logs(id)
			if logs[0].Outcome != "UNKNOWN" {
				t.Fatalf("late worker overwrote recovered log: %+v", logs)
			}
			if maxAttempts == 1 {
				f.saved(id, "DEAD", 1)
				if len(logs) != 1 || calls.Load() != 1 {
					t.Fatal("exhausted recovered task was sent again")
				}
			} else {
				f.saved(id, "SUCCEEDED", 2)
				if len(logs) != 2 || logs[1].Outcome != "SUCCESS" {
					t.Fatalf("recovered retry missing: %+v", logs)
				}
			}
		})
	}
}

func TestWorkerPollingRecoversAbandonedLease(t *testing.T) {
	var calls atomic.Int32
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	})
	id := f.submit("inventory")
	// Simulate a process that committed its claim and then crashed. Leave the
	// lease briefly valid so recovery must also run after its initial sweep.
	f.exec(`WITH leased AS (
		UPDATE notification_tasks SET status='PROCESSING', attempt_count=1,
		    lease_token=gen_random_uuid(), lease_until=clock_timestamp()+interval '500 milliseconds'
		WHERE id=$1::uuid RETURNING id, vendor_code, lease_token
	)
	INSERT INTO delivery_logs (task_id, attempt_no, vendor_snapshot, lease_token)
	SELECT l.id, 1, to_jsonb(v), l.lease_token FROM leased l JOIN vendors v ON v.code=l.vendor_code`, id)
	if recovered, err := f.jobs.RecoverExpired(f.ctx); err != nil || recovered != 0 {
		t.Fatalf("live lease recovered: %d %v", recovered, err)
	}
	f.start(f.jobs)
	eventually(t, func() bool {
		value, err := f.store.Get(f.ctx, id)
		return err == nil && value.Status == "SUCCEEDED"
	})
	f.saved(id, "SUCCEEDED", 2)
	logs := f.logs(id)
	if len(logs) != 2 || logs[0].Outcome != "UNKNOWN" || logs[1].Outcome != "SUCCESS" || calls.Load() != 1 {
		t.Fatalf("abandoned task not recovered by polling: logs=%+v calls=%d", logs, calls.Load())
	}
}

func TestFailedLogWriteRollsBackTaskAndRemainsRecoverable(t *testing.T) {
	var calls atomic.Int32
	f := newWorkerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	})
	id := f.submit("inventory")
	f.exec(`CREATE FUNCTION reject_log_finish() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'simulated log write failure'; END $$;
		CREATE TRIGGER reject_log_finish BEFORE UPDATE ON delivery_logs
		FOR EACH ROW EXECUTE FUNCTION reject_log_finish()`)
	if worked, err := f.jobs.ProcessOne(f.ctx); !worked || err == nil {
		t.Fatalf("simulated storage failure not returned: worked=%v err=%v", worked, err)
	}
	f.saved(id, "PROCESSING", 1)
	logs := f.logs(id)
	if len(logs) != 1 || logs[0].Outcome != "STARTED" {
		t.Fatalf("partial finalization committed: %+v", logs)
	}
	f.exec(`DROP TRIGGER reject_log_finish ON delivery_logs`)
	f.exec(`UPDATE notification_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1::uuid`, id)
	if count, err := f.jobs.RecoverExpired(f.ctx); err != nil || count != 1 {
		t.Fatalf("failed finalization could not be recovered: %d %v", count, err)
	}
	f.exec(`UPDATE notification_tasks SET next_attempt_at=clock_timestamp() WHERE id=$1::uuid`, id)
	f.process()
	f.saved(id, "SUCCEEDED", 2)
	logs = f.logs(id)
	if len(logs) != 2 || logs[0].Outcome != "UNKNOWN" || logs[1].Outcome != "SUCCESS" || calls.Load() != 2 {
		t.Fatalf("failed finalization retry incorrect: %+v calls=%d", logs, calls.Load())
	}
}
