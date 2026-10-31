package worker

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"rightcapital/notification/internal/vendor"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type endlessBody struct {
	read   int
	closed bool
}

func (b *endlessBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}

func (b *endlessBody) Close() error { b.closed = true; return nil }

func TestResponseReadIsBoundedAndClosed(t *testing.T) {
	body := &endlessBody{}
	w := &Worker{client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body, Header: make(http.Header)}, nil
	})}}
	job := attempt{ID: "task-1", Payload: []byte(`{}`), Config: vendor.Config{
		EndpointURL: "http://example.test/", HTTPMethod: "POST", TimeoutMS: 1000,
		HeadersTemplate: `{}`, BodyTemplate: `{}`,
	}}
	got := w.deliver(context.Background(), job)
	if got.Outcome != "SUCCESS" || body.read != MaxResponseBytes+1 || !body.closed {
		t.Fatalf("outcome=%+v read=%d closed=%v", got, body.read, body.closed)
	}
	if len(got.Excerpt) > maxExcerptBytes+len(" [truncated]") || !strings.HasSuffix(got.Excerpt, " [truncated]") {
		t.Fatalf("unbounded excerpt: len=%d", len(got.Excerpt))
	}
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenBody) Close() error             { return nil }

func TestIncompleteResponseIsRetryable(t *testing.T) {
	w := &Worker{client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: brokenBody{}, Header: make(http.Header)}, nil
	})}}
	got := w.deliver(context.Background(), attempt{Payload: []byte(`{}`), Config: vendor.Config{
		EndpointURL: "http://example.test/", HTTPMethod: "POST", TimeoutMS: 1000,
		HeadersTemplate: `{}`, BodyTemplate: `{}`,
	}})
	if got.Outcome != "RETRYABLE_FAILURE" || got.HTTPStatus != 200 {
		t.Fatalf("unexpected result: %+v", got)
	}
}
