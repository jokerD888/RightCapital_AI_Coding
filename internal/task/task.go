package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"rightcapital/notification/internal/vendor"
)

var (
	ErrConflict       = errors.New("idempotency key already used for a different request")
	ErrUnknownVendor  = errors.New("unknown vendor")
	ErrInvalidPayload = errors.New("payload cannot be represented as PostgreSQL JSONB")
	ErrNotFound       = errors.New("task not found")
	ErrNotDead        = errors.New("only DEAD tasks can be retried manually")
)

type Submission struct {
	SourceSystem string         `json:"source_system"`
	VendorCode   string         `json:"vendor_code"`
	EventType    string         `json:"event_type"`
	Payload      map[string]any `json:"payload"`
}

// Digest ignores JSON object key order. The API decoder uses json.Number so
// identifiers and quantities are never rounded through float64.
func (s Submission) Digest() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type Task struct {
	ID                string     `json:"task_id"`
	SourceSystem      string     `json:"source_system"`
	VendorCode        string     `json:"vendor_code"`
	EventType         string     `json:"event_type"`
	Status            string     `json:"status"`
	AttemptCount      int        `json:"attempt_count"`
	RetryStartAttempt int        `json:"retry_start_attempt"`
	NextAttemptAt     time.Time  `json:"next_attempt_at"`
	LastError         *string    `json:"last_error,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

type Accepted struct {
	TaskID    string `json:"task_id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate"`
}

type Store interface {
	Accept(context.Context, string, Submission) (Accepted, error)
	Get(context.Context, string) (Task, error)
	Ping(context.Context) error
	List(context.Context, Filter) ([]Task, error)
	Logs(context.Context, string, int, int) ([]DeliveryLog, error)
	Retry(context.Context, string) (Accepted, error)
	Vendors(context.Context) ([]vendor.Config, error)
	SaveVendor(context.Context, vendor.Config) error
	Stats(context.Context) (Stats, error)
}

type Filter struct {
	Status, VendorCode, SourceSystem string
	Limit, Offset                    int
}

type DeliveryLog struct {
	AttemptNo       int           `json:"attempt_no"`
	VendorSnapshot  vendor.Config `json:"vendor_snapshot"`
	Outcome         string        `json:"outcome"`
	HTTPStatus      *int          `json:"http_status,omitempty"`
	ResponseExcerpt *string       `json:"response_excerpt,omitempty"`
	ErrorMessage    *string       `json:"error_message,omitempty"`
	StartedAt       time.Time     `json:"started_at"`
	FinishedAt      *time.Time    `json:"finished_at,omitempty"`
}

type Stats struct {
	Tasks                   map[string]int64
	OldestPendingAgeSeconds float64
}
