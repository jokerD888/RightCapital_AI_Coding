package vendor

import (
	"fmt"
	"math"
	"net/url"
	"strings"
)

// Config is the current vendors row, copied into each attempt's log snapshot.
type Config struct {
	Code             string `json:"code"`
	Name             string `json:"name"`
	EndpointURL      string `json:"endpoint_url"`
	HTTPMethod       string `json:"http_method"`
	HeadersTemplate  string `json:"headers_template"`
	BodyTemplate     string `json:"body_template"`
	TimeoutMS        int    `json:"timeout_ms"`
	MaxAttempts      int    `json:"max_attempts"`
	RetryBaseSeconds int    `json:"retry_base_seconds"`
	RetryMaxSeconds  int    `json:"retry_max_seconds"`
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Name) == "" || strings.ContainsRune(c.Name, 0) {
		return fmt.Errorf("name is required and must not contain NUL")
	}
	u, err := url.Parse(c.EndpointURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("endpoint_url must be an absolute HTTP(S) URL")
	}
	if c.HTTPMethod != "POST" && c.HTTPMethod != "PUT" && c.HTTPMethod != "PATCH" {
		return fmt.Errorf("http_method must be POST, PUT or PATCH")
	}
	if c.TimeoutMS < 100 || c.TimeoutMS > 60000 || c.MaxAttempts < 1 || c.MaxAttempts > 100 {
		return fmt.Errorf("timeout_ms must be 100-60000 and max_attempts must be 1-100")
	}
	if c.RetryBaseSeconds < 1 || c.RetryMaxSeconds < c.RetryBaseSeconds || c.RetryMaxSeconds > math.MaxInt32 {
		return fmt.Errorf("retry delays must be positive int32 seconds with max >= base")
	}
	for name, source := range map[string]string{"headers": c.HeadersTemplate, "body": c.BodyTemplate} {
		if strings.ContainsRune(source, 0) {
			return fmt.Errorf("%s template must not contain NUL", name)
		}
		if _, err := parse(name, source); err != nil {
			return fmt.Errorf("invalid %s template: %w", name, err)
		}
	}
	return nil
}
