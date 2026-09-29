package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// RouterConfig tunes routing.
type RouterConfig struct {
	MaxRetries       int           // extra attempts for reads, each on a different upstream
	RetryBackoff     time.Duration // base of the jittered backoff between attempts
	FinalityDepth    uint64        // blocks below the best head considered final for caching
	BatchConcurrency int           // batch items routed in parallel
}

// Router answers JSON-RPC requests: cache lookup, upstream selection, call,
// and retries on a different upstream when that is safe.
type Router struct {
	pool    *Pool
	cache   Cache
	metrics Metrics
	log     *slog.Logger
	cfg     RouterConfig
	seq     atomic.Uint64
}

// NewRouter wires a router. cache may be nil to disable caching.
func NewRouter(pool *Pool, cache Cache, metrics Metrics, log *slog.Logger, cfg RouterConfig) *Router {
	if cfg.BatchConcurrency < 1 {
		cfg.BatchConcurrency = 1
	}
	return &Router{pool: pool, cache: cache, metrics: metrics, log: log, cfg: cfg}
}

// Handle answers one request. A non-nil error means no upstream produced an
// answer (ErrNoUpstream, ErrUpstreamFailed, ErrWriteUnknown, ErrDeadline);
// the transport layer turns it into a JSON-RPC error. Otherwise the response
// is what an upstream said, JSON-RPC errors included, with the client's id.
func (r *Router) Handle(ctx context.Context, req jsonrpc.Request) (jsonrpc.Response, error) {
	ref, err := jsonrpc.Block(req)
	if err != nil {
		return jsonrpc.NewError(req.ID, jsonrpc.CodeInvalidParams, err.Error()), nil
	}
	method := jsonrpc.MetricLabel(req.Method)

	var key string
	if r.cache != nil && jsonrpc.CacheRuleFor(req.Method) != jsonrpc.CacheNever {
		key = jsonrpc.CacheKey(req)
		if result, ok := r.cache.Get(ctx, key); ok {
			r.metrics.CacheHit(method)
			return jsonrpc.NewResult(req.ID, result), nil
		}
	}

	resp, err := r.forward(ctx, req, ref, method)
	if err != nil {
		return jsonrpc.Response{}, err
	}
	if key != "" && resp.Error == nil {
		if best, ok := r.pool.BestHead(); ok && jsonrpc.Cacheable(req, ref, resp.Result, best, r.cfg.FinalityDepth) {
			r.cache.Set(ctx, key, resp.Result)
		}
	}
	return resp, nil
}

// BatchResult is the outcome of one batch item, as returned by Handle.
type BatchResult struct {
	Response jsonrpc.Response
	Err      error
}

// HandleBatch splits a batch: every item is routed on its own (a batch can mix
// a write, a historical read and a latest read, each with different eligible
// upstreams). Results are index-aligned with reqs.
func (r *Router) HandleBatch(ctx context.Context, reqs []jsonrpc.Request) []BatchResult {
	results := make([]BatchResult, len(reqs))
	var g errgroup.Group
	g.SetLimit(r.cfg.BatchConcurrency)
	for i, req := range reqs {
		g.Go(func() error {
			results[i].Response, results[i].Err = r.Handle(ctx, req)
			return nil
		})
	}
	_ = g.Wait() // items never return errors; they are recorded per result
	return results
}

// forward runs the attempt loop. Reads get up to MaxRetries extra attempts,
// each on an upstream not tried yet, and only after a retryable failure.
// Writes get exactly one attempt (ADR 0002).
func (r *Router) forward(ctx context.Context, req jsonrpc.Request, ref jsonrpc.BlockRef, method string) (jsonrpc.Response, error) {
	write := jsonrpc.IsWrite(req.Method)
	attempts := 1 + r.cfg.MaxRetries
	if write {
		attempts = 1
	}
	tried := make(map[string]bool, attempts)
	var lastErr error
	var lastAnswer *jsonrpc.Response // a retryable JSON-RPC error (-32005) to pass through

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			r.metrics.Retry(method)
			if err := sleep(ctx, backoff(r.cfg.RetryBackoff, attempt)); err != nil {
				return jsonrpc.Response{}, deadlineErr(err, lastErr)
			}
		}
		if err := ctx.Err(); err != nil {
			return jsonrpc.Response{}, deadlineErr(err, lastErr)
		}
		m, err := r.pool.acquire(ref, tried)
		if err != nil {
			if attempt == 0 {
				r.metrics.NoUpstream(method)
				return jsonrpc.Response{}, fmt.Errorf("route %s (%s block): %w", req.Method, ref.Kind, err)
			}
			break // every eligible upstream has been tried
		}
		tried[m.name] = true

		resp, err := r.call(ctx, m, req, method)
		switch {
		case err == nil && !resp.Error.Retryable():
			return resp, nil
		case err == nil:
			lastAnswer, lastErr = &resp, fmt.Errorf("%s: %w", m.name, resp.Error)
		default:
			lastAnswer, lastErr = nil, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return jsonrpc.Response{}, deadlineErr(ctxErr, lastErr)
		}
		if write {
			if lastAnswer != nil {
				return *lastAnswer, nil // the provider refused it: nothing was sent
			}
			if errors.Is(err, upstream.ErrRateLimited) || errors.Is(err, upstream.ErrRejected) {
				return jsonrpc.Response{}, fmt.Errorf("%w: %w", ErrUpstreamFailed, err)
			}
			return jsonrpc.Response{}, fmt.Errorf("%w: %w", ErrWriteUnknown, err)
		}
		if err != nil && !upstream.Retryable(err) {
			break
		}
		if attempt+1 < attempts {
			r.log.LogAttrs(ctx, slog.LevelWarn, "retrying on another upstream",
				slog.String("method", req.Method), slog.String("upstream", m.name),
				slog.Int("attempt", attempt+1), slog.String("error", lastErr.Error()))
		}
	}
	if lastAnswer != nil {
		return *lastAnswer, nil
	}
	return jsonrpc.Response{}, fmt.Errorf("%w after %d attempt(s): %w", ErrUpstreamFailed, len(tried), lastErr)
}

// call performs one attempt and records its outcome on the member's state.
// The upstream sees a gateway-generated id; the client's id is restored on
// the way back, so ids are unique per upstream connection and a response is
// matched to its request by id, not by position.
func (r *Router) call(ctx context.Context, m *member, req jsonrpc.Request, method string) (jsonrpc.Response, error) {
	upReq := req
	upReq.ID = json.RawMessage(strconv.FormatUint(r.seq.Add(1), 10))

	start := time.Now()
	resp, err := m.client.Call(ctx, upReq)
	elapsed := time.Since(start)
	if err == nil && !bytes.Equal(resp.ID, upReq.ID) {
		err = fmt.Errorf("call %s: %w: response id %s, request id %s", m.name, upstream.ErrBadResponse, resp.ID, upReq.ID)
	}

	var outcome string
	m.mu.Lock()
	switch {
	case err != nil && ctx.Err() != nil:
		m.state.CallAbandoned()
		outcome = OutcomeCanceled
	case err != nil:
		if m.state.CallFailed(time.Now(), err) {
			r.log.Warn("circuit breaker opened", slog.String("upstream", m.name), slog.String("error", err.Error()))
		}
		outcome = failureOutcome(err)
	case resp.Error.Retryable():
		if m.state.CallFailed(time.Now(), resp.Error) {
			r.log.Warn("circuit breaker opened", slog.String("upstream", m.name), slog.String("error", resp.Error.Error()))
		}
		outcome = OutcomeLimited
	case resp.Error != nil:
		m.state.CallSucceeded()
		outcome = OutcomeRPCError
	default:
		m.state.CallSucceeded()
		outcome = OutcomeOK
	}
	m.mu.Unlock()
	r.metrics.UpstreamCall(method, m.name, outcome, elapsed)

	if err != nil {
		return jsonrpc.Response{}, err
	}
	resp.ID = req.ID
	return resp, nil
}

func failureOutcome(err error) string {
	switch {
	case errors.Is(err, upstream.ErrTimeout):
		return OutcomeTimeout
	case errors.Is(err, upstream.ErrRateLimited):
		return OutcomeLimited
	}
	return OutcomeFailure
}

func deadlineErr(ctxErr, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("%w: %w", ErrDeadline, ctxErr)
	}
	return fmt.Errorf("%w: %w (last failure: %w)", ErrDeadline, ctxErr, lastErr)
}

// backoff is "full jitter": a random wait in [0, base*2^(attempt-1)].
func backoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	ceiling := base << (attempt - 1)
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
