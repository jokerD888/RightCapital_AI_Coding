package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"rightcapital/notification/internal/vendor"
)

const (
	MaxResponseBytes = 64 << 10
	maxExcerptBytes  = 4 << 10
)

func (w *Worker) deliver(ctx context.Context, job attempt) result {
	failure := func(err error) result {
		return result{Outcome: "RETRYABLE_FAILURE", Error: err.Error()}
	}
	data := vendor.Data{TaskID: job.ID, SourceSystem: job.SourceSystem, EventType: job.EventType}
	decoder := json.NewDecoder(bytes.NewReader(job.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&data.Payload); err != nil {
		return failure(fmt.Errorf("decode task payload: %w", err))
	}
	headers, body, err := vendor.Render(job.Config.HeadersTemplate, job.Config.BodyTemplate, data)
	if err != nil {
		// Configuration can be fixed in place before the next attempt.
		return failure(fmt.Errorf("render vendor configuration: %w", err))
	}
	endpoint, err := url.Parse(job.Config.EndpointURL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return failure(fmt.Errorf("vendor endpoint must be an absolute HTTP(S) URL"))
	}
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(job.Config.TimeoutMS)*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, job.Config.HTTPMethod, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return failure(fmt.Errorf("build vendor request: %w", err))
	}
	request.Header = headers
	response, err := w.client.Do(request)
	if err != nil {
		return failure(fmt.Errorf("send vendor request: %w", err))
	}
	defer response.Body.Close()
	// The extra byte detects truncation. Never drain an unbounded response.
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	truncated := len(responseBody) > MaxResponseBytes
	responseBody = responseBody[:min(len(responseBody), MaxResponseBytes)]
	excerpt := databaseText(string(responseBody[:min(len(responseBody), maxExcerptBytes)]))
	excerptTruncated := len(excerpt) > maxExcerptBytes
	if excerptTruncated {
		excerpt = excerpt[:maxExcerptBytes]
		for !utf8.ValidString(excerpt) {
			excerpt = excerpt[:len(excerpt)-1]
		}
	}
	if truncated || len(responseBody) > maxExcerptBytes || excerptTruncated {
		excerpt += " [truncated]"
	}
	out := result{HTTPStatus: response.StatusCode, Excerpt: excerpt}
	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		out.Outcome = "SUCCESS"
	case response.StatusCode == 408 || response.StatusCode == 429 || response.StatusCode >= 500:
		out.Outcome = "RETRYABLE_FAILURE"
		out.Error = fmt.Sprintf("vendor returned HTTP %d", response.StatusCode)
		out.RetryAfter = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	default:
		out.Outcome = "PERMANENT_FAILURE"
		out.Error = fmt.Sprintf("vendor returned HTTP %d", response.StatusCode)
	}
	if readErr != nil && out.Outcome == "SUCCESS" {
		out.Outcome = "RETRYABLE_FAILURE"
		out.Error = fmt.Sprintf("read vendor response: %v", readErr)
	}
	return out
}

// External bodies can contain invalid UTF-8 and NUL, which PostgreSQL text
// rejects. Sanitize diagnostic strings so logging cannot strand a task.
func databaseText(value string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(value, "\uFFFD"), "\x00", "\uFFFD")
}
