package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/adapter/httpapi"
	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
	"github.com/Amadeus-22/rpcgate/internal/platform/metrics"
)

// fakeUpstream answers eth_blockNumber with a head and anything else with "0x1".
type fakeUpstream struct{ fail bool }

func (f fakeUpstream) Call(_ context.Context, req jsonrpc.Request) (jsonrpc.Response, error) {
	if f.fail {
		return jsonrpc.Response{}, upstream.ErrServer
	}
	if req.Method == "eth_blockNumber" {
		return jsonrpc.NewResult(req.ID, json.RawMessage(`"0x64"`)), nil
	}
	return jsonrpc.NewResult(req.ID, json.RawMessage(`"0x1"`)), nil
}

type setup struct {
	probe     bool             // run a health round so the upstream is reachable
	anonymous *app.ClientLimit // nil: key required
	fail      bool
}

func newServer(t *testing.T, s setup) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	rec := metrics.New()
	pool := app.NewPool(app.PoolConfig{MaxLagBlocks: 5, State: upstream.Settings{BreakerThreshold: 5, BreakerCooldown: time.Second}},
		[]app.Member{{Name: "up-1", Weight: 1, Client: fakeUpstream{fail: s.fail}}})
	if s.probe {
		app.NewHealthChecker(pool, app.HealthConfig{Timeout: time.Second}, rec, log).CheckOnce(context.Background())
	}
	router := app.NewRouter(pool, nil, rec, log, app.RouterConfig{MaxRetries: 2, BatchConcurrency: 4})
	limiter := app.NewRateLimiter([]app.ClientLimit{{Key: "good-key", RPS: 1, Burst: 3}}, s.anonymous)
	api := httpapi.New(router, limiter, pool, rec.Handler(), log, httpapi.Config{
		RequestTimeout: time.Second, MaxBatchSize: 5, MaxBodyBytes: 1024,
	})
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, srv *httptest.Server, key, body string) (int, http.Header, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, strings.TrimSpace(string(b))
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

const single = `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{}]}`

func TestJSONRPCEndpoint(t *testing.T) {
	anon := &app.ClientLimit{RPS: 100, Burst: 100}
	tests := []struct {
		name       string
		setup      setup
		key        string
		body       string
		wantStatus int
		wantBody   string // exact, when set
		contains   string
	}{
		{"single request", setup{probe: true, anonymous: anon}, "", single, 200,
			`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, ""},
		{"notification gets no body", setup{probe: true, anonymous: anon}, "", `{"jsonrpc":"2.0","method":"eth_call","params":[{}]}`, 204, "", ""},
		{"batch keeps order, drops notifications, answers invalid items", setup{probe: true, anonymous: anon}, "",
			`[{"jsonrpc":"2.0","id":"a","method":"eth_call"},{"jsonrpc":"2.0","method":"eth_call"},{"id":3},{"jsonrpc":"2.0","id":4,"method":"eth_blockNumber"}]`, 200,
			`[{"jsonrpc":"2.0","id":"a","result":"0x1"},{"jsonrpc":"2.0","id":3,"error":{"code":-32600,"message":"invalid request: jsonrpc must be \"2.0\""}},{"jsonrpc":"2.0","id":4,"result":"0x64"}]`, ""},
		{"batch of notifications", setup{probe: true, anonymous: anon}, "", `[{"jsonrpc":"2.0","method":"eth_call"}]`, 204, "", ""},
		{"parse error", setup{probe: true, anonymous: anon}, "", `{"jsonrpc":`, 400, "", `"code":-32700`},
		{"empty batch", setup{probe: true, anonymous: anon}, "", `[]`, 400, "", `"code":-32600`},
		{"batch too large", setup{probe: true, anonymous: anon}, "", "[" + strings.Repeat(single+",", 5) + single + "]", 400, "", "exceeds the limit of 5"},
		{"body too large", setup{probe: true, anonymous: anon}, "", `{"jsonrpc":"2.0","id":1,"method":"` + strings.Repeat("x", 2000) + `"}`, 413, "", "exceeds 1024 bytes"},
		{"key required", setup{probe: true}, "", single, 401, "", "api key required"},
		{"unknown key", setup{probe: true}, "bad-key", single, 401, "", "unknown api key"},
		{"known key", setup{probe: true}, "good-key", single, 200, "", `"result":"0x1"`},
		{"batch costs one token per item", setup{probe: true}, "good-key", "[" + single + "," + single + "," + single + "," + single + "]", 429, "", `"code":-32005`},
		{"no healthy upstream", setup{anonymous: anon}, "", single, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"no healthy upstream"}}`, ""},
		{"no healthy upstream inside a batch is per item", setup{anonymous: anon}, "", "[" + single + "]", 200, "", `"message":"no healthy upstream"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newServer(t, tt.setup)
			status, header, body := post(t, srv, tt.key, tt.body)
			if status != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", status, tt.wantStatus, body)
			}
			if tt.wantBody != "" && body != tt.wantBody {
				t.Fatalf("body =\n%s\nwant\n%s", body, tt.wantBody)
			}
			if !strings.Contains(body, tt.contains) {
				t.Fatalf("body %s does not contain %s", body, tt.contains)
			}
			if status == 429 && header.Get("Retry-After") == "" {
				t.Fatal("429 without Retry-After")
			}
			if status != 204 && header.Get("Content-Type") != "application/json" {
				t.Fatalf("content type %q", header.Get("Content-Type"))
			}
		})
	}
}

func TestUpstreamFailureIsBadGateway(t *testing.T) {
	// The upstream answers probes but fails real calls.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	up := &switchable{}
	pool := app.NewPool(app.PoolConfig{MaxLagBlocks: 5, State: upstream.Settings{BreakerThreshold: 5, BreakerCooldown: time.Second}},
		[]app.Member{{Name: "up-1", Weight: 1, Client: up}})
	app.NewHealthChecker(pool, app.HealthConfig{Timeout: time.Second}, app.NopMetrics{}, log).CheckOnce(context.Background())
	up.fail = true
	router := app.NewRouter(pool, nil, app.NopMetrics{}, log, app.RouterConfig{MaxRetries: 2})
	api := httpapi.New(router, app.NewRateLimiter(nil, &app.ClientLimit{RPS: 10, Burst: 10}), pool, http.NotFoundHandler(), log,
		httpapi.Config{RequestTimeout: time.Second, MaxBatchSize: 5, MaxBodyBytes: 1024})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	status, _, body := post(t, srv, "", single)
	if status != http.StatusBadGateway || !strings.Contains(body, "upstream request failed") {
		t.Fatalf("status %d body %s", status, body)
	}
	status, _, body = post(t, srv, "", `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x02"]}`)
	if status != http.StatusBadGateway || !strings.Contains(body, "outcome is unknown") {
		t.Fatalf("status %d body %s", status, body)
	}
}

type switchable struct{ fail bool }

func (s *switchable) Call(ctx context.Context, req jsonrpc.Request) (jsonrpc.Response, error) {
	if s.fail {
		return jsonrpc.Response{}, upstream.ErrServer
	}
	return fakeUpstream{}.Call(ctx, req)
}

func TestOperationalEndpoints(t *testing.T) {
	ready := newServer(t, setup{probe: true})
	notReady := newServer(t, setup{})

	tests := []struct {
		name       string
		srv        *httptest.Server
		path       string
		wantStatus int
		contains   string
	}{
		{"healthz", notReady, "/healthz", 200, `"status":"ok"`},
		{"readyz ready", ready, "/readyz", 200, `"status":"ready"`},
		{"readyz not ready", notReady, "/readyz", 503, `"code":"no_upstream_reachable"`},
		{"upstreams", ready, "/v1/upstreams", 200, `"name":"up-1","healthy":true,"reachable":true,"head":100`},
		{"metrics", ready, "/metrics", 200, "rpcgate_upstream_head"},
		{"unknown path", ready, "/nope", 404, `"code":"not_found"`},
		{"GET on rpc endpoint", ready, "/", 405, `"code":"method_not_allowed"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := get(t, tt.srv, tt.path)
			if status != tt.wantStatus || !strings.Contains(body, tt.contains) {
				t.Fatalf("status %d body %s", status, body)
			}
		})
	}
}
