package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) Accept(ctx context.Context, key string, s Submission) (Accepted, error) {
	digest, err := s.Digest()
	if err != nil {
		return Accepted{}, fmt.Errorf("hash request: %w", err)
	}
	payload, err := json.Marshal(s.Payload)
	if err != nil {
		return Accepted{}, fmt.Errorf("encode payload: %w", err)
	}

	var result Accepted
	// A single INSERT is an implicit transaction. RETURNING completes only after
	// its commit succeeds; never acknowledge an in-memory or uncommitted task.
	err = p.pool.QueryRow(ctx, `
		INSERT INTO notification_tasks
		    (source_system, idempotency_key, request_hash, vendor_code, event_type, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (source_system, idempotency_key) DO NOTHING
		RETURNING id::text, status`,
		s.SourceSystem, key, digest, s.VendorCode, s.EventType, payload,
	).Scan(&result.TaskID, &result.Status)
	if err == nil {
		return result, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return Accepted{}, ErrUnknownVendor
	}
	// JSON permits NUL escapes and numbers outside PostgreSQL numeric's range.
	// These are permanent input errors, not database outages to retry forever.
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		return Accepted{}, ErrInvalidPayload
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Accepted{}, fmt.Errorf("insert task: %w", err)
	}

	// Use a second statement rather than a same-statement CTE: a conflicting
	// concurrent INSERT may commit after the first statement's snapshot began.
	var existingHash string
	err = p.pool.QueryRow(ctx, `
		SELECT id::text, status, request_hash
		FROM notification_tasks
		WHERE source_system = $1 AND idempotency_key = $2`,
		s.SourceSystem, key,
	).Scan(&result.TaskID, &result.Status, &existingHash)
	if err != nil {
		return Accepted{}, fmt.Errorf("read duplicate task: %w", err)
	}
	if existingHash != digest {
		return Accepted{}, ErrConflict
	}
	result.Duplicate = true
	return result, nil
}

const taskColumns = `id::text, source_system, vendor_code, event_type, status,
    attempt_count, retry_start_attempt, next_attempt_at, last_error, created_at, completed_at`

func scanTask(row interface{ Scan(...any) error }) (Task, error) {
	var result Task
	err := row.Scan(&result.ID, &result.SourceSystem, &result.VendorCode, &result.EventType,
		&result.Status, &result.AttemptCount, &result.RetryStartAttempt, &result.NextAttemptAt,
		&result.LastError, &result.CreatedAt, &result.CompletedAt)
	return result, err
}

func (p *Postgres) Get(ctx context.Context, id string) (Task, error) {
	result, err := scanTask(p.pool.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM notification_tasks WHERE id=$1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("get task: %w", err)
	}
	return result, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}
