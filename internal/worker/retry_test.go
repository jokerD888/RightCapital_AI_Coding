package worker

import (
	"net/http"
	"testing"
	"time"

	"rightcapital/notification/internal/vendor"
)

func TestBackoffIsExponentialCappedAndJittered(t *testing.T) {
	config := vendor.Config{RetryBaseSeconds: 5, RetryMaxSeconds: 60}
	for i, seconds := range []int{5, 10, 20, 40, 60, 60} {
		if got := exponentialDelay(config, i+1); got != time.Duration(seconds)*time.Second {
			t.Fatalf("attempt %d: %s", i+1, got)
		}
		for range 10 {
			got := retryDelay(config, i+1, 0)
			if got < time.Duration(seconds)*time.Second/2 || got > time.Duration(seconds)*time.Second {
				t.Fatalf("jitter outside bounds: %s", got)
			}
		}
	}
	if got := retryDelay(config, 1, 2*time.Minute); got != 2*time.Minute {
		t.Fatalf("Retry-After minimum ignored: %s", got)
	}
	if got := exponentialDelay(config, 1000000); got != time.Minute {
		t.Fatalf("large attempt count overflowed: %s", got)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{"5", 5 * time.Second},
		{now.Add(10 * time.Second).Format(http.TimeFormat), 10 * time.Second},
		{now.Add(-time.Second).Format(http.TimeFormat), 0},
		{"-1", 0}, {"0", 0}, {"", 0}, {"nonsense", 0},
		{"9223372036854775807", 0},
	} {
		if got := parseRetryAfter(tt.value, now); got != tt.want {
			t.Fatalf("%q: got %s want %s", tt.value, got, tt.want)
		}
	}
}
