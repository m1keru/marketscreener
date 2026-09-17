// Package edgar is the SEC client: rate limiting, gzip, retries and the four
// EDGAR endpoints the screener needs.
package edgar

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Hosts. data.sec.gov serves the JSON APIs, www.sec.gov the static files.
const (
	DataHost = "https://data.sec.gov"
	WWWHost  = "https://www.sec.gov"
)

// Stats counts what the run cost, for the data_quality block of the response.
type Stats struct {
	Requests  atomic.Int64
	TooMany   atomic.Int64 // HTTP 429 responses seen
	Retries   atomic.Int64
	CacheHits atomic.Int64
	Bytes     atomic.Int64
}

// ErrForbidden is returned on HTTP 403, which for SEC always means the
// User-Agent was not acceptable.
type ErrForbidden struct{ URL, UserAgent string }

func (e *ErrForbidden) Error() string {
	return fmt.Sprintf("SEC returned 403 for %s with User-Agent %q: the header must carry a real "+
		"contact address (see user_agent_contact in the config)", e.URL, e.UserAgent)
}

// ErrNotFound marks a 404, which for EDGAR is routine: not every company has
// every concept, and not every frame exists yet.
type ErrNotFound struct{ URL string }

func (e *ErrNotFound) Error() string { return "not found: " + e.URL }

// Client is a rate-limited SEC HTTP client. It is safe for concurrent use and
// the limiter is shared across the whole process, as SEC counts per source.
type Client struct {
	httpc      *http.Client
	limiter    *rate.Limiter
	userAgent  string
	maxRetries int
	log        *slog.Logger
	Stats      *Stats

	// dataHost and wwwHost are overridable so tests never touch the network.
	dataHost string
	wwwHost  string
}

// Options configures a Client.
type Options struct {
	UserAgent         string
	RequestsPerSecond float64
	MaxRetries        int
	Timeout           time.Duration
	Logger            *slog.Logger
	HTTPClient        *http.Client
	// DataHost and WWWHost default to the real SEC hosts.
	DataHost string
	WWWHost  string
}

// New builds a client. UserAgent must already carry a contact address; the
// caller validates that at start-up.
func New(o Options) *Client {
	if o.RequestsPerSecond <= 0 {
		o.RequestsPerSecond = 10
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 5
	}
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	hc := o.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: o.Timeout}
	}
	if o.DataHost == "" {
		o.DataHost = DataHost
	}
	if o.WWWHost == "" {
		o.WWWHost = WWWHost
	}
	return &Client{
		httpc:      hc,
		limiter:    rate.NewLimiter(rate.Limit(o.RequestsPerSecond), 1),
		userAgent:  o.UserAgent,
		maxRetries: o.MaxRetries,
		log:        o.Logger,
		Stats:      &Stats{},
		dataHost:   o.DataHost,
		wwwHost:    o.WWWHost,
	}
}

// get fetches url and decodes the JSON body into out. It retries 429 and 5xx
// with exponential backoff (1s, 2s, 4s, 8s, 16s).
func (c *Client) get(ctx context.Context, url string, out any) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			c.Stats.Retries.Add(1)
			backoff := time.Duration(1<<(attempt-1)) * time.Second
			c.log.Debug("edgar retry", "url", url, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		err := func() error {
			body, err := c.do(ctx, url)
			if err != nil {
				return err
			}
			defer body.Close()
			return json.NewDecoder(body).Decode(out)
		}()
		if err == nil {
			return nil
		}
		var retryable *retryableError
		if asRetryable(err, &retryable) {
			lastErr = retryable.err
			continue
		}
		var nf *ErrNotFound
		var forb *ErrForbidden
		if errors.As(err, &nf) || errors.As(err, &forb) {
			return err
		}
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			// The body arrived intact and does not match the struct. Retrying
			// costs five backoffs and changes nothing.
			return fmt.Errorf("decode %s: %w", url, err)
		}
		// Anything else is usually a truncated transfer; retry it.
		lastErr = fmt.Errorf("decode %s: %w", url, err)
	}
	return fmt.Errorf("%s: giving up after %d attempts: %w", url, c.maxRetries+1, lastErr)
}

type retryableError struct{ err error }

func (r *retryableError) Error() string { return r.err.Error() }

func asRetryable(err error, target **retryableError) bool {
	r, ok := err.(*retryableError)
	if ok {
		*target = r
	}
	return ok
}

// do performs one request and returns a decompressed body.
func (c *Client) do(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")

	c.Stats.Requests.Add(1)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, &retryableError{err: err}
	}
	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusForbidden:
		resp.Body.Close()
		return nil, &ErrForbidden{URL: url, UserAgent: c.userAgent}
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, &ErrNotFound{URL: url}
	case resp.StatusCode == http.StatusTooManyRequests:
		resp.Body.Close()
		c.Stats.TooMany.Add(1)
		return nil, &retryableError{err: fmt.Errorf("%s: HTTP 429", url)}
	case resp.StatusCode >= 500:
		resp.Body.Close()
		return nil, &retryableError{err: fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)}
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}

	// net/http transparently decompresses only when it added the header
	// itself; we set it explicitly, so unwrap gzip here.
	var body io.ReadCloser = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return nil, &retryableError{err: fmt.Errorf("%s: gzip: %w", url, err)}
		}
		body = &gzipBody{zr: zr, raw: resp.Body}
	}
	if n, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64); err == nil {
		c.Stats.Bytes.Add(n)
	}
	return body, nil
}

type gzipBody struct {
	zr  *gzip.Reader
	raw io.ReadCloser
}

func (g *gzipBody) Read(p []byte) (int, error) { return g.zr.Read(p) }
func (g *gzipBody) Close() error {
	g.zr.Close()
	return g.raw.Close()
}

// PadCIK renders a CIK as EDGAR wants it in URLs: ten digits, zero padded.
func PadCIK(cik string) string {
	cik = strings.TrimPrefix(strings.TrimSpace(cik), "CIK")
	cik = strings.TrimLeft(cik, "0")
	if cik == "" {
		cik = "0"
	}
	return fmt.Sprintf("%010s", cik)
}
