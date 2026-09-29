//go:build integration

// Integration tests run against a live rpcgate in front of real anvil nodes:
//
//	docker compose -f deploy/docker-compose.yml up -d --build
//	make test-integration
//
// RPCGATE_URL overrides the default http://localhost:8545.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func gatewayURL() string {
	if u := os.Getenv("RPCGATE_URL"); u != "" {
		return u
	}
	return "http://localhost:8545"
}

func postJSON(t *testing.T, body string) (int, []byte) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(gatewayURL(), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST %s: %v", gatewayURL(), err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestGatewayAgainstAnvil(t *testing.T) {
	status, body := postJSON(t, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"result":"0x7a69"`)) {
		t.Fatalf("eth_chainId: %d %s", status, body)
	}

	status, body = postJSON(t, `[
		{"jsonrpc":"2.0","id":"head","method":"eth_blockNumber"},
		{"jsonrpc":"2.0","id":"genesis","method":"eth_getBlockByNumber","params":["0x0",false]},
		{"jsonrpc":"2.0","id":"balance","method":"eth_getBalance","params":["0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266","latest"]}
	]`)
	var batch []struct {
		ID     string          `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &batch); err != nil || status != http.StatusOK || len(batch) != 3 {
		t.Fatalf("batch: %d %s (%v)", status, body, err)
	}
	for i, id := range []string{"head", "genesis", "balance"} {
		if batch[i].ID != id || batch[i].Error != nil || len(batch[i].Result) == 0 {
			t.Fatalf("batch item %d: %+v", i, batch[i])
		}
	}

	resp, err := http.Get(gatewayURL() + "/v1/upstreams")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var admin struct {
		Upstreams []struct {
			Name      string `json:"name"`
			Reachable bool   `json:"reachable"`
		} `json:"upstreams"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&admin); err != nil || len(admin.Upstreams) == 0 {
		t.Fatalf("admin API: %v %+v", err, admin)
	}
	for _, u := range admin.Upstreams {
		if !u.Reachable {
			t.Errorf("upstream %s not reachable", u.Name)
		}
	}
}
