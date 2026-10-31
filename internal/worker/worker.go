package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rightcapital/notification/internal/vendor"
)

var ErrLeaseLost = errors.New("delivery lease expired or no longer owned")

const storeTimeout = 5 * time.Second

type Options struct {
	Concurrency      int
	PollInterval     time.Duration
	RecoveryInterval time.Duration
}

type Worker struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	client  *http.Client
	options Options
}

type attempt struct {
	ID           string
	SourceSystem string
	EventType    string
	Payload      []byte
	Number       int
	RetryStart   int
	LeaseToken   string
	Config       vendor.Config
	Snapshot     []byte
	Deliver      bool
}

type result struct {
	Outcome    string
	HTTPStatus int
	Excerpt    string
	Error      string
	RetryAfter time.Duration
}

func New(pool *pgxpool.Pool, log *slog.Logger, options Options) (*Worker, error) {
	if options.Concurrency < 0 || options.PollInterval < 0 || options.RecoveryInterval < 0 {
		return nil, fmt.Errorf("worker options must not be negative")
	}
	if options.Concurrency == 0 {
		options.Concurrency = 4
	}
	if options.PollInterval == 0 {
		options.PollInterval = 500 * time.Millisecond
	}
	if options.RecoveryInterval == 0 {
		options.RecoveryInterval = 5 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxResponseHeaderBytes = 64 << 10
	client := &http.Client{
		Transport: transport,
		// Keep one attempt directed at its configured endpoint; log redirects as
		// failures instead of silently delivering to a different destination.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Worker{pool: pool, log: log, client: client, options: options}, nil
}

// Run blocks until cancellation and all delivery/recovery goroutines have exited.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range w.options.Concurrency {
		wg.Go(func() {
			for ctx.Err() == nil {
				worked, err := w.ProcessOne(ctx)
				if err != nil && ctx.Err() == nil {
					w.log.Error("worker attempt failed", "error", err)
				}
				if !worked || err != nil {
					if !wait(ctx, w.options.PollInterval) {
						return
					}
				}
			}
		})
	}
	wg.Go(func() {
		for ctx.Err() == nil {
			count, err := w.RecoverExpired(ctx)
			if err != nil && ctx.Err() == nil {
				w.log.Error("lease recovery failed", "error", err)
			} else if count > 0 {
				w.log.Warn("expired delivery leases recovered", "count", count)
			}
			if !wait(ctx, w.options.RecoveryInterval) {
				return
			}
		}
	})
	wg.Wait()
}

// ProcessOne claims at most one due task and completes its attempt. HTTP is
// performed only after the claim transaction has committed and released locks.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, worked, err := w.claim(ctx)
	if err != nil || !worked || !job.Deliver {
		return worked, err
	}
	response := w.deliver(ctx, job)
	// Shutdown cancels in-flight HTTP, but still allows its result to be saved.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.finish(finishCtx, job, response); err != nil {
		return true, fmt.Errorf("finish task %s attempt %d: %w", job.ID, job.Number, err)
	}
	w.log.Info("delivery attempt completed", "task_id", job.ID, "attempt", job.Number,
		"outcome", response.Outcome, "http_status", response.HTTPStatus)
	return true, nil
}

func (w *Worker) CloseIdleConnections() { w.client.CloseIdleConnections() }

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
