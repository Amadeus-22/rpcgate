package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

func TestRateLimiter(t *testing.T) {
	keys := []app.ClientLimit{{Key: "k1", RPS: 1, Burst: 3}}
	tests := []struct {
		name      string
		anonymous *app.ClientLimit
		key       string
		n         []int
		want      []error
	}{
		{"known key within burst", nil, "k1", []int{2, 1}, []error{nil, nil}},
		{"known key over burst", nil, "k1", []int{3, 1}, []error{nil, app.ErrRateLimited}},
		{"batch larger than burst", nil, "k1", []int{4}, []error{app.ErrRateLimited}},
		{"unknown key", nil, "nope", []int{1}, []error{app.ErrUnknownKey}},
		{"key required", nil, "", []int{1}, []error{app.ErrKeyRequired}},
		{"anonymous allowed", &app.ClientLimit{RPS: 1, Burst: 1}, "", []int{1, 1}, []error{nil, app.ErrRateLimited}},
		{"unknown key refused even with anonymous", &app.ClientLimit{RPS: 1, Burst: 1}, "nope", []int{1}, []error{app.ErrUnknownKey}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := app.NewRateLimiter(keys, tt.anonymous)
			for i, n := range tt.n {
				if err := l.Allow(tt.key, n); !errors.Is(err, tt.want[i]) {
					t.Fatalf("call %d: err = %v, want %v", i, err, tt.want[i])
				}
			}
		})
	}
}

// fakeUpstream answers every call with a fixed response; a hand-written fake
// of the Upstream port for cases a chaos node cannot express.
type fakeUpstream struct {
	result string
	rpcErr *jsonrpc.Error
}

func (f fakeUpstream) Call(_ context.Context, req jsonrpc.Request) (jsonrpc.Response, error) {
	if f.rpcErr != nil {
		return jsonrpc.Response{JSONRPC: jsonrpc.Version, ID: req.ID, Error: f.rpcErr}, nil
	}
	return jsonrpc.NewResult(req.ID, json.RawMessage(f.result)), nil
}

func TestProbeRejectsBadHeads(t *testing.T) {
	tests := []struct {
		name    string
		up      fakeUpstream
		wantErr string
	}{
		{"not a string", fakeUpstream{result: `123`}, "decode head"},
		{"not hex", fakeUpstream{result: `"12"`}, "decode head"},
		{"json-rpc error", fakeUpstream{rpcErr: &jsonrpc.Error{Code: -32000, Message: "syncing"}}, "syncing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := app.NewPool(app.PoolConfig{MaxLagBlocks: 5, State: upstream.Settings{BreakerThreshold: 1}},
				[]app.Member{{Name: "u", Weight: 1, Client: tt.up}})
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			app.NewHealthChecker(pool, app.HealthConfig{Timeout: 100 * time.Millisecond}, app.NopMetrics{}, log).CheckOnce(context.Background())
			s := pool.Status()[0]
			if s.Reachable || !strings.Contains(s.LastError, tt.wantErr) {
				t.Fatalf("status = %+v, want unreachable with %q", s, tt.wantErr)
			}
		})
	}
}

func TestStatusReportsLagAndHealth(t *testing.T) {
	pool := app.NewPool(app.PoolConfig{MaxLagBlocks: 2, State: upstream.Settings{BreakerThreshold: 1}}, []app.Member{
		{Name: "ahead", Weight: 1, Client: fakeUpstream{result: `"0x64"`}},  // 100
		{Name: "behind", Weight: 1, Client: fakeUpstream{result: `"0x60"`}}, // 96
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	app.NewHealthChecker(pool, app.HealthConfig{Timeout: 100 * time.Millisecond}, app.NopMetrics{}, log).CheckOnce(context.Background())
	st := pool.Status()
	if !st[0].Healthy || st[0].Lag != 0 || st[1].Healthy || st[1].Lag != 4 || st[1].Breaker != "closed" {
		t.Fatalf("status = %+v", st)
	}
	if best, ok := pool.BestHead(); !ok || best != 100 {
		t.Fatalf("best head = %d %v", best, ok)
	}
}
