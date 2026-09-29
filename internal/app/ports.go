// Package app holds rpcgate's use cases: routing a JSON-RPC call to the right
// upstream with retries (Router), keeping upstream state fresh
// (HealthChecker) and limiting clients (RateLimiter). It defines the ports it
// needs; adapters implement them.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// Upstream sends one JSON-RPC request to one provider. Implementations must
// bound the call with their own timeout and report transport failures with
// the upstream.Err* sentinels so the router can classify them.
type Upstream interface {
	Call(ctx context.Context, req jsonrpc.Request) (jsonrpc.Response, error)
}

// Cache stores raw JSON results by key.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, value []byte)
}

// Metrics receives what the use cases observe. Method labels are already
// normalised with jsonrpc.MetricLabel.
type Metrics interface {
	UpstreamCall(method, upstream, outcome string, d time.Duration)
	NoUpstream(method string)
	Retry(method string)
	CacheHit(method string)
	UpstreamState(upstream string, head, lag uint64, breaker upstream.BreakerState)
}

// Gateway-level failures: the request could not be answered by any upstream.
var (
	ErrNoUpstream     = errors.New("no healthy upstream")
	ErrUpstreamFailed = errors.New("upstream request failed")
	ErrWriteUnknown   = errors.New("transaction submission failed and was not retried; its outcome is unknown")
	ErrDeadline       = errors.New("request deadline exceeded")
)

// Call outcomes used as metric labels.
const (
	OutcomeOK       = "ok"        // answer returned (result)
	OutcomeRPCError = "rpc_error" // answer returned (JSON-RPC error that is not the upstream's fault)
	OutcomeLimited  = "limited"   // HTTP 429 or JSON-RPC -32005
	OutcomeTimeout  = "timeout"
	OutcomeFailure  = "failure"  // network error, 5xx, invalid response, 4xx
	OutcomeCanceled = "canceled" // the client gave up first
)

// NopMetrics discards everything; useful in tests and tools.
type NopMetrics struct{}

func (NopMetrics) UpstreamCall(string, string, string, time.Duration)          {}
func (NopMetrics) NoUpstream(string)                                           {}
func (NopMetrics) Retry(string)                                                {}
func (NopMetrics) CacheHit(string)                                             {}
func (NopMetrics) UpstreamState(string, uint64, uint64, upstream.BreakerState) {}
