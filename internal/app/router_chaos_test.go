package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
)

// slow is the probe delay that makes a node rank behind a 0-delay node.
const slow = 30 * time.Millisecond

func TestRetryOnDifferentUpstream(t *testing.T) {
	tests := []struct {
		name     string
		behavior behavior
		outcome  string
	}{
		{"HTTP 500", behave500, app.OutcomeFailure},
		{"HTTP 429", behave429, app.OutcomeLimited},
		{"JSON-RPC -32005", behaveRPCLimit, app.OutcomeLimited},
		{"hang until upstream timeout", behaveHang, app.OutcomeTimeout},
		{"mismatched response id", behaveWrongID, app.OutcomeFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := newNode(t, "bad", 100).set(tt.behavior)
			good := newNode(t, "good", 100).withProbeDelay(slow)
			o := defaults()
			o.upstreamTimeout = 150 * time.Millisecond
			h := newHarness(t, o, bad, good)

			resp, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc(`"client-id"`, "eth_getBalance", `["0xabc","latest"]`))
			if err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if got := servedBy(t, resp); got != "good" {
				t.Fatalf("served by %q, want good", got)
			}
			if string(resp.ID) != `"client-id"` {
				t.Fatalf("client id not restored: %s", resp.ID)
			}
			if bad.calls.Load() != 1 || good.calls.Load() != 1 {
				t.Fatalf("calls bad=%d good=%d, want 1 and 1", bad.calls.Load(), good.calls.Load())
			}
			m := h.metrics.snapshot()
			if m.retries != 1 || m.calls["bad/"+tt.outcome] != 1 || m.calls["good/ok"] != 1 {
				t.Fatalf("metrics = %+v", m)
			}
			if s := h.status(t, "bad"); s.ErrorRate == 0 || s.LastError == "" {
				t.Fatalf("failure not recorded on bad: %+v", s)
			}
		})
	}
}

func TestRetriesAreBoundedAndUseDistinctUpstreams(t *testing.T) {
	nodes := []*node{}
	for _, name := range []string{"n1", "n2", "n3", "n4"} {
		nodes = append(nodes, newNode(t, name, 100).set(behave500))
	}
	h := newHarness(t, defaults(), nodes...)

	_, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("1", "eth_call", `[{},"latest"]`))
	if !errors.Is(err, app.ErrUpstreamFailed) {
		t.Fatalf("err = %v, want ErrUpstreamFailed", err)
	}
	total := int64(0)
	for _, n := range nodes {
		if c := n.calls.Load(); c > 1 {
			t.Fatalf("%s called %d times; retries must go to a different upstream", n.name, c)
		}
		total += n.calls.Load()
	}
	if total != 3 {
		t.Fatalf("total calls = %d, want 1 + 2 retries", total)
	}
}

func TestRetryStopsWhenEveryEligibleUpstreamWasTried(t *testing.T) {
	a := newNode(t, "a", 100).set(behave500)
	b := newNode(t, "b", 100).set(behave500)
	h := newHarness(t, defaults(), a, b)
	_, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("1", "eth_call", `[{},"latest"]`))
	if !errors.Is(err, app.ErrUpstreamFailed) || !strings.Contains(err.Error(), "2 attempt(s)") {
		t.Fatalf("err = %v", err)
	}
}

func TestRejectedRequestIsNotRetried(t *testing.T) {
	bad := newNode(t, "bad", 100).set(behave400)
	good := newNode(t, "good", 100).withProbeDelay(slow)
	h := newHarness(t, defaults(), bad, good)
	_, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("1", "eth_call", `[{},"latest"]`))
	if !errors.Is(err, app.ErrUpstreamFailed) {
		t.Fatalf("err = %v, want ErrUpstreamFailed", err)
	}
	if good.calls.Load() != 0 {
		t.Fatal("a 4xx rejection means the request is bad; it must not be retried")
	}
}

func TestPassThroughLimitErrorWhenNoUpstreamLeft(t *testing.T) {
	only := newNode(t, "only", 100).set(behaveRPCLimit)
	h := newHarness(t, defaults(), only)
	resp, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("7", "eth_call", `[{},"latest"]`))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != jsonrpc.CodeLimitExceeded || string(resp.ID) != "7" {
		t.Fatalf("want the provider's -32005 with the client id, got %+v", resp)
	}
}

func TestWriteIsNeverRetried(t *testing.T) {
	tests := []struct {
		name     string
		behavior behavior
		wantErr  error // nil: the upstream's JSON-RPC answer is returned
	}{
		{"server error: outcome unknown", behave500, app.ErrWriteUnknown},
		{"timeout: outcome unknown", behaveHang, app.ErrWriteUnknown},
		{"HTTP 429: refused, nothing sent", behave429, app.ErrUpstreamFailed},
		{"JSON-RPC -32005 passes through", behaveRPCLimit, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := newNode(t, "first", 100).set(tt.behavior)
			second := newNode(t, "second", 100).withProbeDelay(slow)
			o := defaults()
			o.upstreamTimeout = 150 * time.Millisecond
			h := newHarness(t, o, first, second)

			resp, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("1", "eth_sendRawTransaction", `["0x02f8"]`))
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && (err != nil || resp.Error == nil || resp.Error.Code != jsonrpc.CodeLimitExceeded) {
				t.Fatalf("want -32005 answer, got %+v, %v", resp, err)
			}
			if first.calls.Load() != 1 || second.calls.Load() != 0 {
				t.Fatalf("calls first=%d second=%d: a write goes to exactly one upstream", first.calls.Load(), second.calls.Load())
			}
			if h.metrics.snapshot().retries != 0 {
				t.Fatal("write was retried")
			}
		})
	}
}

func TestHeadAwareRouting(t *testing.T) {
	// "lagging" is the fastest node but 10 blocks behind (max lag 5).
	lagging := newNode(t, "lagging", 90)
	a := newNode(t, "a", 100).withProbeDelay(slow)
	b := newNode(t, "b", 100).withProbeDelay(slow)
	h := newHarness(t, defaults(), lagging, a, b)

	tests := []struct {
		name   string
		req    jsonrpc.Request
		inTip  bool   // must be served by a or b
		wantBy string // exact upstream, when set
	}{
		{"block above lagging head", rpc("1", "eth_getBlockByNumber", `["0x5f",false]`), true, ""},                     // 95
		{"old block uses the fast lagging node", rpc("1", "eth_getBlockByNumber", `["0x32",false]`), false, "lagging"}, // 50
		{"block equal to lagging head", rpc("1", "eth_getBalance", `["0xabc","0x5a"]`), false, "lagging"},              // 90
		{"latest goes to the best head", rpc("1", "eth_getBalance", `["0xabc","latest"]`), true, ""},
		{"omitted block means latest", rpc("1", "eth_call", `[{}]`), true, ""},
		{"head-independent method skips a node beyond max lag", rpc("1", "net_version", `[]`), true, ""},
		{"future block goes to the best head", rpc("1", "eth_getBlockByNumber", `["0x1f4",false]`), true, ""}, // 500
		{"logs range ending above lagging head", rpc("1", "eth_getLogs", `[{"fromBlock":"0x1","toBlock":"0x60"}]`), true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := h.router.Handle(ctxTimeout(t, 5*time.Second), tt.req)
			if err != nil {
				t.Fatalf("Handle: %v", err)
			}
			got := servedBy(t, resp)
			if tt.wantBy != "" && got != tt.wantBy {
				t.Fatalf("served by %q, want %q", got, tt.wantBy)
			}
			if tt.inTip && got == "lagging" {
				t.Fatal("served by the lagging node")
			}
		})
	}
	if s := h.status(t, "lagging"); s.Healthy || s.Lag != 10 || !s.Reachable {
		t.Fatalf("lagging status = %+v", s)
	}
}

func TestBreakerOpensThenHalfOpensThenCloses(t *testing.T) {
	flaky := newNode(t, "flaky", 100).set(behave500)
	steady := newNode(t, "steady", 100).withProbeDelay(slow)
	o := defaults()
	o.breakerThreshold = 2
	o.breakerCooldown = 150 * time.Millisecond
	h := newHarness(t, o, flaky, steady)
	call := func() string {
		t.Helper()
		resp, err := h.router.Handle(ctxTimeout(t, 5*time.Second), rpc("1", "eth_call", `[{},"latest"]`))
		if err != nil {
			t.Fatalf("Handle: %v", err)
		}
		return servedBy(t, resp)
	}

	// Two failures open the breaker; the retry hides them from the client.
	call()
	call()
	if flaky.calls.Load() != 2 || h.status(t, "flaky").Breaker != "open" {
		t.Fatalf("after 2 failures: calls=%d breaker=%s", flaky.calls.Load(), h.status(t, "flaky").Breaker)
	}
	// Open: traffic skips flaky entirely.
	if got := call(); got != "steady" || flaky.calls.Load() != 2 {
		t.Fatalf("open breaker let traffic through: served=%s calls=%d", got, flaky.calls.Load())
	}

	// After cooldown: half-open, one trial. It fails, so the breaker reopens.
	time.Sleep(200 * time.Millisecond)
	if b := h.status(t, "flaky").Breaker; b != "half_open" {
		t.Fatalf("breaker = %s, want half_open", b)
	}
	if got := call(); got != "steady" || flaky.calls.Load() != 3 {
		t.Fatalf("trial call: served=%s flaky calls=%d", got, flaky.calls.Load())
	}
	if b := h.status(t, "flaky").Breaker; b != "open" {
		t.Fatalf("failed trial should reopen, breaker = %s", b)
	}

	// After another cooldown the upstream has recovered: the trial succeeds and closes it.
	flaky.set(behaveOK)
	time.Sleep(200 * time.Millisecond)
	if got := call(); got != "flaky" {
		t.Fatalf("recovered upstream not used: served=%s", got)
	}
	if b := h.status(t, "flaky").Breaker; b != "closed" {
		t.Fatalf("successful trial should close, breaker = %s", b)
	}
}

func TestBatchSplitAndReassembledByID(t *testing.T) {
	// "lagging" is fastest but 10 behind and answers slowly, so batch items
	// complete out of order across two upstreams.
	lagging := newNode(t, "lagging", 90).withCallDelay(80 * time.Millisecond)
	tip := newNode(t, "tip", 100).withProbeDelay(slow)
	h := newHarness(t, defaults(), lagging, tip)

	reqs := []jsonrpc.Request{
		rpc(`"old"`, "eth_getBlockByNumber", `["0x0a",false]`), // 10: lagging (fastest eligible)
		rpc(`7`, "eth_getBlockByNumber", `["0x60",false]`),     // 96: tip only
		rpc(`"tx"`, "eth_sendRawTransaction", `["0x02"]`),      // any, but lagging exceeds max lag: tip
		rpc(`null`, "eth_getBalance", `["0xabc","latest"]`),    // tip
		rpc(`"bad"`, "eth_getBalance", `["0xabc","0xzz"]`),     // invalid params, answered locally
	}
	want := []string{"lagging", "tip", "tip", "tip", ""}

	results := h.router.HandleBatch(ctxTimeout(t, 5*time.Second), reqs)
	if len(results) != len(reqs) {
		t.Fatalf("got %d results for %d requests", len(results), len(reqs))
	}
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("item %d: %v", i, res.Err)
		}
		if string(res.Response.ID) != string(reqs[i].ID) {
			t.Fatalf("item %d: id %s, want %s", i, res.Response.ID, reqs[i].ID)
		}
		if want[i] == "" {
			if res.Response.Error == nil || res.Response.Error.Code != jsonrpc.CodeInvalidParams {
				t.Fatalf("item %d: want -32602, got %+v", i, res.Response)
			}
			continue
		}
		if got := servedBy(t, res.Response); got != want[i] {
			t.Fatalf("item %d served by %s, want %s", i, got, want[i])
		}
	}
	// Upstreams only ever see gateway ids; client ids never leave rpcgate.
	for _, n := range []*node{lagging, tip} {
		n.mu.Lock()
		for _, id := range n.seenIDs {
			if id == `"old"` || id == `"tx"` || id == "7" || id == "null" {
				t.Errorf("%s saw client id %s", n.name, id)
			}
		}
		n.mu.Unlock()
	}
}

func TestClientDeadlineIsRespected(t *testing.T) {
	a := newNode(t, "a", 100).set(behaveHang)
	b := newNode(t, "b", 100).set(behaveHang)
	o := defaults()
	o.upstreamTimeout = 5 * time.Second // longer than the client will wait
	h := newHarness(t, o, a, b)

	start := time.Now()
	_, err := h.router.Handle(ctxTimeout(t, 150*time.Millisecond), rpc("1", "eth_call", `[{},"latest"]`))
	elapsed := time.Since(start)
	if !errors.Is(err, app.ErrDeadline) {
		t.Fatalf("err = %v, want ErrDeadline", err)
	}
	if elapsed > time.Second {
		t.Fatalf("took %v; the client deadline was 150ms", elapsed)
	}
	if calls := a.calls.Load() + b.calls.Load(); calls != 1 {
		t.Fatalf("calls = %d; no retry may start after the deadline", calls)
	}
	// The client gave up, not the upstream: no breaker failure is recorded.
	for _, name := range []string{"a", "b"} {
		if s := h.status(t, name); s.ErrorRate != 0 || s.Breaker != "closed" {
			t.Fatalf("%s penalised for a client timeout: %+v", name, s)
		}
	}
	if h.metrics.snapshot().calls["a/canceled"]+h.metrics.snapshot().calls["b/canceled"] != 1 {
		t.Fatalf("metrics = %+v", h.metrics.snapshot())
	}
}

func TestAllUnhealthyMeansNoUpstream(t *testing.T) {
	a := newNode(t, "a", 100)
	b := newNode(t, "b", 100)
	a.probeFail.Store(true)
	b.probeFail.Store(true)
	h := newHarness(t, defaults(), a, b)

	_, err := h.router.Handle(ctxTimeout(t, time.Second), rpc("1", "eth_blockNumber", `[]`))
	if !errors.Is(err, app.ErrNoUpstream) {
		t.Fatalf("err = %v, want ErrNoUpstream", err)
	}
	if h.pool.Ready() {
		t.Fatal("pool reports ready with every upstream down")
	}
	if h.metrics.snapshot().noUpstream != 1 || a.calls.Load()+b.calls.Load() != 0 {
		t.Fatal("no upstream should be called and the metric should increment")
	}

	// Recovery is picked up by the next probe round.
	a.probeFail.Store(false)
	h.health.CheckOnce(ctxTimeout(t, time.Second))
	if _, err := h.router.Handle(ctxTimeout(t, time.Second), rpc("1", "eth_blockNumber", `[]`)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestMaxRPSSpillsToNextUpstream(t *testing.T) {
	capped := newNode(t, "capped", 100)
	other := newNode(t, "other", 100).withProbeDelay(slow)
	o := defaults()
	o.maxRPS = map[string]float64{"capped": 1}
	h := newHarness(t, o, capped, other)

	first, _ := h.router.Handle(ctxTimeout(t, time.Second), rpc("1", "eth_call", `[{}]`))
	second, _ := h.router.Handle(ctxTimeout(t, time.Second), rpc("2", "eth_call", `[{}]`))
	if servedBy(t, first) != "capped" || servedBy(t, second) != "other" {
		t.Fatalf("served %s then %s", servedBy(t, first), servedBy(t, second))
	}
}

func TestCacheServesFinalBlocksOnly(t *testing.T) {
	n := newNode(t, "n", 1000)
	o := defaults()
	o.cacheBytes = 1 << 20
	h := newHarness(t, o, n)
	ctx := ctxTimeout(t, 5*time.Second)

	final := rpc("1", "eth_getBlockByNumber", `["0x10",false]`)   // 16, final at head 1000
	recent := rpc("2", "eth_getBlockByNumber", `["0x3e0",false]`) // 992, not final
	for range 3 {
		for _, r := range []jsonrpc.Request{final, recent} {
			if _, err := h.router.Handle(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n.calls.Load() != 1+3 || h.metrics.snapshot().cacheHits != 2 {
		t.Fatalf("calls=%d hits=%d; want 4 calls (1 final + 3 recent) and 2 hits", n.calls.Load(), h.metrics.snapshot().cacheHits)
	}
	resp, _ := h.router.Handle(ctx, rpc(`"again"`, "eth_getBlockByNumber", `["0x10", false]`))
	if string(resp.ID) != `"again"` {
		t.Fatalf("cached answer must carry the new client id, got %s", resp.ID)
	}
	var block map[string]string
	_ = json.Unmarshal(resp.Result, &block)
	if block["number"] != "0x10" {
		t.Fatalf("cached result = %s", resp.Result)
	}
}

func TestChainIDMismatchKeepsUpstreamOut(t *testing.T) {
	n := newNode(t, "mainnet-by-mistake", 100) // answers chain id 31337
	o := defaults()
	o.chainID = 1
	h := newHarness(t, o, n)
	s := h.status(t, n.name)
	if s.Reachable || !strings.Contains(s.LastError, "different chain") {
		t.Fatalf("status = %+v", s)
	}
}

func TestInvalidBlockParamIsAnsweredLocally(t *testing.T) {
	n := newNode(t, "n", 100)
	h := newHarness(t, defaults(), n)
	resp, err := h.router.Handle(ctxTimeout(t, time.Second), rpc("1", "eth_getBalance", `["0xabc", 12]`))
	if err != nil || resp.Error == nil || resp.Error.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("got %+v, %v", resp, err)
	}
	if n.calls.Load() != 0 {
		t.Fatal("invalid request reached an upstream")
	}
}

func TestHealthCheckerRunStopsOnCancel(t *testing.T) {
	n := newNode(t, "n", 100)
	h := newHarness(t, defaults(), n)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.health.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
