package upstreamhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

var testReq = jsonrpc.Request{JSONRPC: jsonrpc.Version, ID: json.RawMessage("1"), Method: "eth_blockNumber", Params: json.RawMessage("[]")}

func TestCall(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr error
		check   func(t *testing.T, resp jsonrpc.Response)
	}{
		{
			name: "result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Header.Get("Content-Type") != "application/json" || !strings.Contains(string(body), `"eth_blockNumber"`) {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
			},
			check: func(t *testing.T, resp jsonrpc.Response) {
				if string(resp.Result) != `"0x10"` {
					t.Fatalf("result %s", resp.Result)
				}
			},
		},
		{
			name: "null result is a result",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
			},
			check: func(t *testing.T, resp jsonrpc.Response) {
				if string(resp.Result) != "null" {
					t.Fatalf("result %q", resp.Result)
				}
			},
		},
		{
			name: "json-rpc error is an answer, not a failure",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`))
			},
			check: func(t *testing.T, resp jsonrpc.Response) {
				if resp.Error == nil || resp.Error.Code != -32000 {
					t.Fatalf("error %+v", resp.Error)
				}
			},
		},
		{name: "429", handler: status(http.StatusTooManyRequests), wantErr: upstream.ErrRateLimited},
		{name: "502", handler: status(http.StatusBadGateway), wantErr: upstream.ErrServer},
		{name: "401", handler: status(http.StatusUnauthorized), wantErr: upstream.ErrRejected},
		{name: "html body", handler: body("<html>oops</html>"), wantErr: upstream.ErrBadResponse},
		{name: "empty object", handler: body(`{"jsonrpc":"2.0","id":1}`), wantErr: upstream.ErrBadResponse},
		{name: "batch answer to single request", handler: body(`[]`), wantErr: upstream.ErrBadResponse},
		{
			name: "slow upstream times out",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body) // lets the server notice the client hanging up
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
			},
			wantErr: upstream.ErrTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			c, err := New("test", srv.URL+"/secret-api-key", 100*time.Millisecond, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Call(context.Background(), testReq)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				if strings.Contains(err.Error(), "secret-api-key") || strings.Contains(err.Error(), srv.URL) {
					t.Fatalf("error leaks the upstream URL: %v", err)
				}
				if !strings.Contains(err.Error(), "test") {
					t.Fatalf("error does not name the upstream: %v", err)
				}
				return
			}
			tt.check(t, resp)
		})
	}
}

func TestUnreachableDoesNotLeakURL(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/v3/secret-key"
	srv.Close() // nothing listens any more
	c, err := New("gone", url, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Call(context.Background(), testReq)
	if !errors.Is(err, upstream.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("error leaks the URL: %v", err)
	}
}

func TestNewValidatesURL(t *testing.T) {
	for _, u := range []string{"", "ftp://x", "http://", "::"} {
		if _, err := New("x", u, time.Second, nil); err == nil {
			t.Errorf("New(%q) accepted", u)
		}
	}
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { http.Error(w, http.StatusText(code), code) }
}

func body(s string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(s)) }
}
