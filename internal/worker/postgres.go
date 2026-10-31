package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (w *Worker) claim(parent context.Context) (attempt, bool, error) {
	ctx, cancel := context.WithTimeout(parent, storeTimeout)
	defer cancel()
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return attempt{}, false, err
	}
	defer tx.Rollback(ctx)
	var job attempt
	err = tx.QueryRow(ctx, `
		SELECT t.id::text, t.source_system, t.event_type, t.payload,
		       t.attempt_count, t.retry_start_attempt, to_jsonb(v)
		FROM notification_tasks t JOIN vendors v ON v.code = t.vendor_code
		WHERE t.status IN ('PENDING', 'RETRY_WAIT')
		  AND t.next_attempt_at <= statement_timestamp()
		ORDER BY t.next_attempt_at, t.id
		LIMIT 1 FOR UPDATE OF t SKIP LOCKED`).Scan(
		&job.ID, &job.SourceSystem, &job.EventType, &job.Payload, &job.Number, &job.RetryStart, &job.Snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return attempt{}, false, nil
	}
	if err != nil {
		return attempt{}, false, err
	}
	if err := json.Unmarshal(job.Snapshot, &job.Config); err != nil {
		return attempt{}, false, err
	}
	if job.Number-job.RetryStart >= job.Config.MaxAttempts {
		_, err = tx.Exec(ctx, `UPDATE notification_tasks
			SET status='DEAD', last_error='attempt limit exhausted',
			    updated_at=clock_timestamp(), completed_at=clock_timestamp()
			WHERE id=$1::uuid`, job.ID)
		if err != nil {
			return attempt{}, false, err
		}
		return job, true, tx.Commit(ctx)
	}
	job.Number++
	// Each free slot claims only one job. No locally queued jobs consume leases.
	// The lease exceeds the complete HTTP deadline by a fixed persistence margin.
	leaseMS := int64(job.Config.TimeoutMS) + (30 * time.Second).Milliseconds()
	err = tx.QueryRow(ctx, `UPDATE notification_tasks
		SET status='PROCESSING', attempt_count=attempt_count+1,
		    lease_token=gen_random_uuid(),
		    lease_until=clock_timestamp() + $2::double precision * interval '1 millisecond',
		    updated_at=clock_timestamp(), completed_at=NULL
		WHERE id=$1::uuid RETURNING lease_token::text`, job.ID, leaseMS).Scan(&job.LeaseToken)
	if err != nil {
		return attempt{}, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO delivery_logs
		(task_id, attempt_no, vendor_snapshot, lease_token)
		VALUES ($1::uuid, $2, $3, $4::uuid)`, job.ID, job.Number, job.Snapshot, job.LeaseToken)
	if err != nil {
		return attempt{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return attempt{}, false, err
	}
	job.Deliver = true
	return job, true, nil
}

func (w *Worker) finish(ctx context.Context, job attempt, out result) error {
	status := "DEAD"
	delay := time.Duration(0)
	if out.Outcome == "SUCCESS" {
		status = "SUCCEEDED"
	} else if out.Outcome == "RETRYABLE_FAILURE" && job.Number-job.RetryStart < job.Config.MaxAttempts {
		status = "RETRY_WAIT"
		delay = retryDelay(job.Config, job.Number-job.RetryStart, out.RetryAfter)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	updated, err := tx.Exec(ctx, `UPDATE notification_tasks
		SET status=$3, lease_token=NULL, lease_until=NULL,
		    next_attempt_at=clock_timestamp() + $4::double precision * interval '1 millisecond',
		    last_error=NULLIF($5, ''), updated_at=clock_timestamp(),
		    completed_at=CASE WHEN $3 IN ('SUCCEEDED', 'DEAD') THEN clock_timestamp() ELSE NULL END
		WHERE id=$1::uuid AND lease_token=$2::uuid AND status='PROCESSING'
		  AND lease_until > clock_timestamp()`,
		job.ID, job.LeaseToken, status, delay.Milliseconds(), databaseText(out.Error))
	if err != nil {
		return err
	}
	if updated.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	var httpStatus any
	if out.HTTPStatus != 0 {
		httpStatus = out.HTTPStatus
	}
	updated, err = tx.Exec(ctx, `UPDATE delivery_logs
		SET outcome=$3, http_status=$4, response_excerpt=NULLIF($5, ''),
		    error_message=NULLIF($6, ''), finished_at=clock_timestamp()
		WHERE task_id=$1::uuid AND lease_token=$2::uuid AND outcome='STARTED'`,
		job.ID, job.LeaseToken, out.Outcome, httpStatus, out.Excerpt, databaseText(out.Error))
	if err != nil {
		return err
	}
	if updated.RowsAffected() != 1 {
		return fmt.Errorf("attempt log missing for task %s", job.ID)
	}
	return tx.Commit(ctx)
}

// RecoverExpired marks abandoned attempts UNKNOWN and schedules a new attempt
// (or DEAD when exhausted). It uses current vendor retry settings, not a version.
func (w *Worker) RecoverExpired(parent context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(parent, storeTimeout)
	defer cancel()
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT t.id::text, t.lease_token::text, t.attempt_count, t.retry_start_attempt, to_jsonb(v)
		FROM notification_tasks t JOIN vendors v ON v.code=t.vendor_code
		WHERE t.status='PROCESSING' AND t.lease_until <= statement_timestamp()
		ORDER BY t.lease_until, t.id
		LIMIT 100 FOR UPDATE OF t SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	var expired []attempt
	for rows.Next() {
		var job attempt
		if err := rows.Scan(&job.ID, &job.LeaseToken, &job.Number, &job.RetryStart, &job.Snapshot); err != nil {
			rows.Close()
			return 0, err
		}
		if err := json.Unmarshal(job.Snapshot, &job.Config); err != nil {
			rows.Close()
			return 0, err
		}
		expired = append(expired, job)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	for _, job := range expired {
		const message = "lease expired; delivery outcome unknown"
		status := "RETRY_WAIT"
		delay := retryDelay(job.Config, job.Number-job.RetryStart, 0)
		if job.Number-job.RetryStart >= job.Config.MaxAttempts {
			status, delay = "DEAD", 0
		}
		updated, err := tx.Exec(ctx, `UPDATE delivery_logs
			SET outcome='UNKNOWN', error_message=$3, finished_at=clock_timestamp()
			WHERE task_id=$1::uuid AND lease_token=$2::uuid AND outcome='STARTED'`,
			job.ID, job.LeaseToken, message)
		if err != nil {
			return 0, err
		}
		if updated.RowsAffected() != 1 {
			return 0, fmt.Errorf("expired attempt log missing for task %s", job.ID)
		}
		_, err = tx.Exec(ctx, `UPDATE notification_tasks
			SET status=$3, lease_token=NULL, lease_until=NULL, last_error=$5,
			    next_attempt_at=clock_timestamp() + $4::double precision * interval '1 millisecond',
			    updated_at=clock_timestamp(),
			    completed_at=CASE WHEN $3='DEAD' THEN clock_timestamp() ELSE NULL END
			WHERE id=$1::uuid AND lease_token=$2::uuid`,
			job.ID, job.LeaseToken, status, delay.Milliseconds(), message)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(expired), nil
}
