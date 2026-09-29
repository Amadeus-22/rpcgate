package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/selection"
)

// HealthConfig tunes probing.
type HealthConfig struct {
	Interval time.Duration
	Timeout  time.Duration
	ChainID  uint64 // expected chain; 0 skips the check
}

// HealthChecker probes every upstream with eth_blockNumber on an interval,
// updating reachability, head and latency. Probe latency is the EWMA input:
// it is the same cheap call every time, so it compares upstreams fairly.
type HealthChecker struct {
	pool    *Pool
	cfg     HealthConfig
	metrics Metrics
	log     *slog.Logger
}

// ErrWrongChain marks an upstream serving a different chain than configured.
var ErrWrongChain = errors.New("upstream serves a different chain")

// NewHealthChecker wires a checker.
func NewHealthChecker(pool *Pool, cfg HealthConfig, metrics Metrics, log *slog.Logger) *HealthChecker {
	return &HealthChecker{pool: pool, cfg: cfg, metrics: metrics, log: log}
}

// Run probes until ctx is cancelled. It returns nil on shutdown.
func (h *HealthChecker) Run(ctx context.Context) error {
	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.CheckOnce(ctx)
		}
	}
}

// CheckOnce probes every upstream concurrently and publishes their state.
func (h *HealthChecker) CheckOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, m := range h.pool.members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.probe(ctx, m)
		}()
	}
	wg.Wait()
	h.publish()
}

func (h *HealthChecker) probe(ctx context.Context, m *member) {
	ctx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()

	m.mu.Lock()
	verified := m.chainVerified
	m.mu.Unlock()
	if h.cfg.ChainID != 0 && !verified {
		if err := h.verifyChain(ctx, m); err != nil {
			h.record(m, 0, 0, err)
			return
		}
	}

	start := time.Now()
	result, err := probeCall(ctx, m.client, "eth_blockNumber")
	latency := time.Since(start)
	var head uint64
	if err == nil {
		var hex string
		if err = json.Unmarshal(result, &hex); err == nil {
			head, err = jsonrpc.ParseQuantity(hex)
		}
		if err != nil {
			err = fmt.Errorf("probe eth_blockNumber: decode head: %w", err)
		}
	}
	h.record(m, head, latency, err)
}

func (h *HealthChecker) verifyChain(ctx context.Context, m *member) error {
	result, err := probeCall(ctx, m.client, "eth_chainId")
	if err != nil {
		return err
	}
	var hex string
	if err := json.Unmarshal(result, &hex); err != nil {
		return fmt.Errorf("probe eth_chainId: decode: %w", err)
	}
	got, err := jsonrpc.ParseQuantity(hex)
	if err != nil {
		return fmt.Errorf("probe eth_chainId: %w", err)
	}
	if got != h.cfg.ChainID {
		return fmt.Errorf("%w: got chain id %d, want %d", ErrWrongChain, got, h.cfg.ChainID)
	}
	m.mu.Lock()
	m.chainVerified = true
	m.mu.Unlock()
	return nil
}

func (h *HealthChecker) record(m *member, head uint64, latency time.Duration, err error) {
	m.mu.Lock()
	was := m.state.Reachable
	if err != nil {
		m.state.ProbeFailed(err)
	} else {
		m.state.ProbeSucceeded(head, latency)
	}
	m.mu.Unlock()

	switch {
	case was && err != nil:
		h.log.Warn("upstream unreachable", slog.String("upstream", m.name), slog.String("error", err.Error()))
	case !was && err != nil:
		h.log.Debug("upstream still unreachable", slog.String("upstream", m.name), slog.String("error", err.Error()))
	case !was:
		h.log.Info("upstream reachable", slog.String("upstream", m.name), slog.Uint64("head", head))
	}
}

func (h *HealthChecker) publish() {
	now := time.Now()
	best, _ := h.pool.BestHead()
	for _, m := range h.pool.members {
		m.mu.Lock()
		head, state := m.state.Head, m.state.Breaker(now)
		m.mu.Unlock()
		h.metrics.UpstreamState(m.name, head, selection.Lag(head, best), state)
	}
}

func probeCall(ctx context.Context, client Upstream, method string) (json.RawMessage, error) {
	resp, err := client.Call(ctx, jsonrpc.Request{JSONRPC: jsonrpc.Version, ID: json.RawMessage("0"), Method: method, Params: json.RawMessage("[]")})
	if err != nil {
		return nil, fmt.Errorf("probe %s: %w", method, err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("probe %s: %w", method, resp.Error)
	}
	return resp.Result, nil
}
