package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"rightcapital/notification/internal/vendor"
)

func (p *Postgres) List(ctx context.Context, f Filter) ([]Task, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+taskColumns+` FROM notification_tasks
		WHERE ($1='' OR status=$1) AND ($2='' OR vendor_code=$2) AND ($3='' OR source_system=$3)
		ORDER BY created_at DESC, id DESC LIMIT $4 OFFSET $5`,
		f.Status, f.VendorCode, f.SourceSystem, f.Limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Task, 0)
	for rows.Next() {
		item, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (p *Postgres) Logs(ctx context.Context, id string, after, limit int) ([]DeliveryLog, error) {
	if _, err := p.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `SELECT attempt_no, vendor_snapshot, outcome, http_status,
		response_excerpt, error_message, started_at, finished_at FROM delivery_logs
		WHERE task_id=$1::uuid AND attempt_no>$2 ORDER BY attempt_no LIMIT $3`, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]DeliveryLog, 0)
	for rows.Next() {
		var item DeliveryLog
		var snapshot []byte
		if err := rows.Scan(&item.AttemptNo, &snapshot, &item.Outcome, &item.HTTPStatus,
			&item.ResponseExcerpt, &item.ErrorMessage, &item.StartedAt, &item.FinishedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(snapshot, &item.VendorSnapshot); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (p *Postgres) Retry(ctx context.Context, id string) (Accepted, error) {
	var result Accepted
	// Preserve the task ID, monotonically increasing attempt numbers and all
	// old logs. A new budget starts at the current total attempt count.
	err := p.pool.QueryRow(ctx, `UPDATE notification_tasks
		SET status='PENDING', retry_start_attempt=attempt_count, next_attempt_at=clock_timestamp(),
		    last_error=NULL, completed_at=NULL, updated_at=clock_timestamp()
		WHERE id=$1::uuid AND status='DEAD' RETURNING id::text, status`, id).
		Scan(&result.TaskID, &result.Status)
	if err == nil {
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Accepted{}, err
	}
	if _, err := p.Get(ctx, id); err != nil {
		return Accepted{}, err
	}
	return Accepted{}, ErrNotDead
}

func (p *Postgres) Vendors(ctx context.Context) ([]vendor.Config, error) {
	rows, err := p.pool.Query(ctx, `SELECT to_jsonb(v) FROM vendors v ORDER BY code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]vendor.Config, 0)
	for rows.Next() {
		var config vendor.Config
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
		items = append(items, config)
	}
	return items, rows.Err()
}

func (p *Postgres) SaveVendor(ctx context.Context, c vendor.Config) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO vendors
		(code, name, endpoint_url, http_method, headers_template, body_template,
		 timeout_ms, max_attempts, retry_base_seconds, retry_max_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, endpoint_url=EXCLUDED.endpoint_url,
		 http_method=EXCLUDED.http_method, headers_template=EXCLUDED.headers_template,
		 body_template=EXCLUDED.body_template, timeout_ms=EXCLUDED.timeout_ms,
		 max_attempts=EXCLUDED.max_attempts, retry_base_seconds=EXCLUDED.retry_base_seconds,
		 retry_max_seconds=EXCLUDED.retry_max_seconds`, c.Code, c.Name, c.EndpointURL, c.HTTPMethod,
		c.HeadersTemplate, c.BodyTemplate, c.TimeoutMS, c.MaxAttempts, c.RetryBaseSeconds, c.RetryMaxSeconds)
	return err
}

func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	stats := Stats{Tasks: make(map[string]int64)}
	rows, err := p.pool.Query(ctx, `SELECT status, count(*) FROM notification_tasks GROUP BY status`)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			rows.Close()
			return stats, err
		}
		stats.Tasks[state] = count
	}
	if err := rows.Err(); err != nil {
		return stats, err
	}
	rows.Close()
	err = p.pool.QueryRow(ctx, `SELECT COALESCE(GREATEST(0, extract(epoch FROM
		statement_timestamp()-min(created_at))),0)::double precision FROM notification_tasks
		WHERE status IN ('PENDING','RETRY_WAIT')`).Scan(&stats.OldestPendingAgeSeconds)
	if err != nil {
		return stats, fmt.Errorf("queue age: %w", err)
	}
	return stats, nil
}
