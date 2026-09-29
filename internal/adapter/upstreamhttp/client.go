// Package upstreamhttp is the JSON-RPC over HTTP client for one upstream.
// It classifies every failure with the upstream.Err* sentinels and never puts
// the upstream URL in an error: provider URLs usually embed an API key.
package upstreamhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// MaxResponseBytes caps how much of an upstream answer is read. Large
// eth_getLogs answers fit; a misbehaving upstream cannot exhaust memory.
const MaxResponseBytes = 64 << 20

// Client calls one upstream.
type Client struct {
	name    string
	url     string
	timeout time.Duration
	http    *http.Client
}

// New returns a client with a per-call timeout. httpClient may be shared
// between upstreams; nil uses NewHTTPClient().
func New(name, rawURL string, timeout time.Duration, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("upstream %s: url must be http(s)://host[...]", name)
	}
	if httpClient == nil {
		httpClient = NewHTTPClient()
	}
	return &Client{name: name, url: rawURL, timeout: timeout, http: httpClient}, nil
}

// NewHTTPClient returns a client tuned for a proxy: the default transport
// keeps only 2 idle connections per host, which forces a new TCP+TLS
// handshake for most concurrent calls.
func NewHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 64
	t.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: t}
}

// Call sends req and decodes a single JSON-RPC response.
func (c *Client) Call(ctx context.Context, req jsonrpc.Request) (jsonrpc.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	body, err := json.Marshal(req)
	if err != nil {
		return jsonrpc.Response{}, fmt.Errorf("call %s: encode request: %w", c.name, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return jsonrpc.Response{}, fmt.Errorf("call %s: build request: %w", c.name, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		return jsonrpc.Response{}, c.transportError(ctx, err)
	}
	defer httpResp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, MaxResponseBytes+1))
	if err != nil {
		return jsonrpc.Response{}, c.transportError(ctx, err)
	}
	if err := statusError(httpResp.StatusCode); err != nil {
		return jsonrpc.Response{}, fmt.Errorf("call %s: HTTP %d: %w", c.name, httpResp.StatusCode, err)
	}
	if len(payload) > MaxResponseBytes {
		return jsonrpc.Response{}, fmt.Errorf("call %s: %w: body larger than %d bytes", c.name, upstream.ErrBadResponse, MaxResponseBytes)
	}

	var resp jsonrpc.Response
	if err := json.Unmarshal(payload, &resp); err != nil {
		return jsonrpc.Response{}, fmt.Errorf("call %s: %w: %w", c.name, upstream.ErrBadResponse, err)
	}
	if resp.Error == nil && resp.Result == nil {
		return jsonrpc.Response{}, fmt.Errorf("call %s: %w: neither result nor error", c.name, upstream.ErrBadResponse)
	}
	return resp, nil
}

func statusError(code int) error {
	switch {
	case code == http.StatusOK:
		return nil
	case code == http.StatusTooManyRequests:
		return upstream.ErrRateLimited
	case code >= 500:
		return upstream.ErrServer
	}
	return upstream.ErrRejected
}

// transportError classifies a network failure and drops the *url.Error
// wrapper, whose message would leak the URL.
func (c *Client) transportError(ctx context.Context, err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("call %s: %w after %s", c.name, upstream.ErrTimeout, c.timeout)
	}
	return fmt.Errorf("call %s: %w: %w", c.name, upstream.ErrUnreachable, err)
}
