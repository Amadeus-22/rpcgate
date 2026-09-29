package app_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/adapter/lrucache"
	"github.com/Amadeus-22/rpcgate/internal/adapter/upstreamhttp"
	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// behavior is what a chaos node does with non-probe calls.
type behavior int32

const (
	behaveOK       behavior = iota
	behave500               // HTTP 500
	behave429               // HTTP 429
	behave400               // HTTP 400: the request itself is rejected
	behaveRPCLimit          // HTTP 200 with JSON-RPC error -32005
	behaveHang              // never answers until the client gives up
	behaveWrongID           // answers with an id that is not the request's
)

// node is an httptest upstream that can be told to misbehave. Health probes
// (eth_blockNumber, eth_chainId) are controlled separately from other calls,
// so a node can look healthy to the prober and still fail real traffic.
type node struct {
	name       string
	srv        *httptest.Server
	head       atomic.Uint64
	behavior   atomic.Int32
	probeFail  atomic.Bool
	probeDelay atomic.Int64 // nanoseconds
	callDelay  atomic.Int64 // nanoseconds
	calls      atomic.Int64 // non-probe calls received
	stop       chan struct{}

	mu      sync.Mutex
	seenIDs []string
}

func newNode(t *testing.T, name string, head uint64) *node {
	t.Helper()
	n := &node{name: name, stop: make(chan struct{})}
	n.head.Store(head)
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	t.Cleanup(func() { close(n.stop) }) // runs first: releases hung handlers
	return n
}

func (n *node) set(b behavior) *node                 { n.behavior.Store(int32(b)); return n }
func (n *node) withProbeDelay(d time.Duration) *node { n.probeDelay.Store(int64(d)); return n }
func (n *node) withCallDelay(d time.Duration) *node  { n.callDelay.Store(int64(d)); return n }
func (n *node) wait(r *http.Request, d time.Duration) {
	select {
	case <-time.After(d):
	case <-r.Context().Done():
	case <-n.stop:
	}
}

func (n *node) serve(w http.ResponseWriter, r *http.Request) {
	var req jsonrpc.Request
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	reply := func(result any) {
		raw, _ := json.Marshal(result)
		_ = json.NewEncoder(w).Encode(jsonrpc.NewResult(req.ID, raw))
	}
	switch req.Method {
	case "eth_blockNumber", "eth_chainId":
		if n.probeFail.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		n.wait(r, time.Duration(n.probeDelay.Load()))
		if req.Method == "eth_chainId" {
			reply("0x7a69")
			return
		}
		// eth_blockNumber as a real call (not a probe) is counted by id != 0.
		if string(req.ID) != "0" {
			n.calls.Add(1)
			reply(map[string]string{"served_by": n.name})
			return
		}
		reply(jsonrpc.FormatQuantity(n.head.Load()))
		return
	}

	n.calls.Add(1)
	n.mu.Lock()
	n.seenIDs = append(n.seenIDs, string(req.ID))
	n.mu.Unlock()
	n.wait(r, time.Duration(n.callDelay.Load()))

	switch behavior(n.behavior.Load()) {
	case behave500:
		http.Error(w, "internal error", http.StatusInternalServerError)
	case behave429:
		http.Error(w, "slow down", http.StatusTooManyRequests)
	case behave400:
		http.Error(w, "bad request", http.StatusBadRequest)
	case behaveRPCLimit:
		_ = json.NewEncoder(w).Encode(jsonrpc.NewError(req.ID, jsonrpc.CodeLimitExceeded, "limit exceeded"))
	case behaveHang:
		n.wait(r, time.Hour)
	case behaveWrongID:
		_ = json.NewEncoder(w).Encode(jsonrpc.NewResult(json.RawMessage(`"not-yours"`), json.RawMessage(`"0x1"`)))
	default:
		result := map[string]string{"served_by": n.name}
		if req.Method == "eth_getBlockByNumber" {
			var params []string
			_ = json.Unmarshal(req.Params, &params)
			if len(params) > 0 {
				result["number"] = params[0]
			}
		}
		reply(result)
	}
}

// recMetrics is a hand-written fake of app.Metrics that counts what matters.
type recMetrics struct {
	mu         sync.Mutex
	calls      map[string]int // "upstream/outcome"
	retries    int
	noUpstream int
	cacheHits  int
}

func newRecMetrics() *recMetrics { return &recMetrics{calls: map[string]int{}} }

func (m *recMetrics) UpstreamCall(_, up, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[up+"/"+outcome]++
}
func (m *recMetrics) NoUpstream(string)                                           { m.mu.Lock(); m.noUpstream++; m.mu.Unlock() }
func (m *recMetrics) Retry(string)                                                { m.mu.Lock(); m.retries++; m.mu.Unlock() }
func (m *recMetrics) CacheHit(string)                                             { m.mu.Lock(); m.cacheHits++; m.mu.Unlock() }
func (m *recMetrics) UpstreamState(string, uint64, uint64, upstream.BreakerState) {}

// counts is a lock-free copy of recMetrics for assertions.
type counts struct {
	calls      map[string]int
	retries    int
	noUpstream int
	cacheHits  int
}

func (m *recMetrics) snapshot() counts {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make(map[string]int, len(m.calls))
	for k, v := range m.calls {
		calls[k] = v
	}
	return counts{calls: calls, retries: m.retries, noUpstream: m.noUpstream, cacheHits: m.cacheHits}
}

type options struct {
	maxRetries       int
	breakerThreshold int
	breakerCooldown  time.Duration
	upstreamTimeout  time.Duration
	maxLag           uint64
	chainID          uint64
	maxRPS           map[string]float64
	cacheBytes       int
}

func defaults() options {
	return options{
		maxRetries:       2,
		breakerThreshold: 3,
		breakerCooldown:  time.Second,
		upstreamTimeout:  500 * time.Millisecond,
		maxLag:           5,
	}
}

type harness struct {
	router  *app.Router
	pool    *app.Pool
	health  *app.HealthChecker
	metrics *recMetrics
}

// newHarness wires the real router, pool, health checker and HTTP client
// against chaos nodes, and runs one probe round so heads are known.
func newHarness(t *testing.T, o options, nodes ...*node) *harness {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	members := make([]app.Member, 0, len(nodes))
	for _, n := range nodes {
		c, err := upstreamhttp.New(n.name, n.srv.URL, o.upstreamTimeout, nil)
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, app.Member{Name: n.name, Weight: 1, MaxRPS: o.maxRPS[n.name], Client: c})
	}
	pool := app.NewPool(app.PoolConfig{
		MaxLagBlocks: o.maxLag,
		State: upstream.Settings{
			BreakerThreshold: o.breakerThreshold,
			BreakerCooldown:  o.breakerCooldown,
			LatencyAlpha:     0.5,
			ErrorRateAlpha:   0.5,
		},
	}, members)
	m := newRecMetrics()
	var cache app.Cache
	if o.cacheBytes > 0 {
		cache = lrucache.New(o.cacheBytes)
	}
	router := app.NewRouter(pool, cache, m, log, app.RouterConfig{
		MaxRetries:       o.maxRetries,
		RetryBackoff:     time.Millisecond,
		FinalityDepth:    64,
		BatchConcurrency: 8,
	})
	health := app.NewHealthChecker(pool, app.HealthConfig{Interval: time.Hour, Timeout: time.Second, ChainID: o.chainID}, m, log)
	health.CheckOnce(context.Background())
	return &harness{router: router, pool: pool, health: health, metrics: m}
}

func (h *harness) status(t *testing.T, name string) app.UpstreamStatus {
	t.Helper()
	for _, s := range h.pool.Status() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no upstream %q", name)
	return app.UpstreamStatus{}
}

func rpc(id, method, params string) jsonrpc.Request {
	return jsonrpc.Request{JSONRPC: jsonrpc.Version, ID: json.RawMessage(id), Method: method, Params: json.RawMessage(params)}
}

func servedBy(t *testing.T, resp jsonrpc.Response) string {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("unexpected JSON-RPC error: %v", resp.Error)
	}
	var r struct {
		ServedBy string `json:"served_by"`
	}
	if err := json.Unmarshal(resp.Result, &r); err != nil {
		t.Fatalf("decode result %s: %v", resp.Result, err)
	}
	return r.ServedBy
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
