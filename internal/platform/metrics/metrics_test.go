package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

func TestRecorder(t *testing.T) {
	r := New()
	r.UpstreamCall("eth_call", "a", "ok", 20*time.Millisecond)
	r.UpstreamCall("eth_call", "a", "ok", 30*time.Millisecond)
	r.NoUpstream("eth_call")
	r.Retry("eth_call")
	r.CacheHit("eth_chainId")
	r.UpstreamState("a", 100, 3, upstream.BreakerOpen)

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"requests ok", testutil.ToFloat64(r.requests.WithLabelValues("eth_call", "a", "ok")), 2},
		{"no upstream", testutil.ToFloat64(r.requests.WithLabelValues("eth_call", "none", "no_upstream")), 1},
		{"retries", testutil.ToFloat64(r.retries.WithLabelValues("eth_call")), 1},
		{"cache hits", testutil.ToFloat64(r.cacheHits.WithLabelValues("eth_chainId")), 1},
		{"head", testutil.ToFloat64(r.head.WithLabelValues("a")), 100},
		{"lag", testutil.ToFloat64(r.lag.WithLabelValues("a")), 3},
		{"breaker", testutil.ToFloat64(r.breaker.WithLabelValues("a")), 2},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, name := range []string{
		"rpcgate_requests_total", "rpcgate_request_duration_seconds", "rpcgate_upstream_head",
		"rpcgate_upstream_lag_blocks", "rpcgate_breaker_state", "rpcgate_cache_hits_total",
		"rpcgate_retries_total", "go_goroutines",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("exposition lacks %s", name)
		}
	}
	if r.Registry() == nil {
		t.Fatal("nil registry")
	}
}
