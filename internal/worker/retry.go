package worker

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"rightcapital/notification/internal/vendor"
)

func exponentialDelay(config vendor.Config, attemptNo int) time.Duration {
	delay := time.Duration(config.RetryBaseSeconds) * time.Second
	cap := time.Duration(config.RetryMaxSeconds) * time.Second
	for n := 1; n < attemptNo; n++ {
		if delay >= cap/2 {
			return cap
		}
		delay *= 2
	}
	return min(delay, cap)
}

func retryDelay(config vendor.Config, attemptNo int, retryAfter time.Duration) time.Duration {
	delay := exponentialDelay(config, attemptNo)
	// Equal jitter: [50%, 100%] of the capped exponential delay.
	half := delay / 2
	delay = half + time.Duration(rand.Int64N(int64(delay-half)+1))
	return max(delay, retryAfter)
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds >= 0 && seconds <= (1<<63-1)/int64(time.Second) {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}
