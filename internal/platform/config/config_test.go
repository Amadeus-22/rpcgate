package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestExampleConfigIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(raw, env(map[string]string{"RPCGATE_DEMO_KEY": "demo"}))
	if err != nil {
		t.Fatalf("example config: %v", err)
	}
	if len(f.Upstreams) != 3 || f.ChainID != 31337 || f.Clients.Keys[0].Key != "demo" || f.Clients.Anonymous.Burst != 100 {
		t.Fatalf("unexpected parse: %+v", f)
	}
}

func TestDefaults(t *testing.T) {
	f, err := Parse([]byte("upstreams:\n  - {name: a, url: 'http://a'}\n"), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"weight", f.Upstreams[0].Weight == 1},
		{"health interval", f.Health.Interval == 2*time.Second},
		{"health timeout", f.Health.Timeout == time.Second},
		{"max lag", f.Health.MaxLagBlocks == 5},
		{"breaker", f.Breaker.FailureThreshold == 5 && f.Breaker.Cooldown == 10*time.Second},
		{"retries", *f.Routing.MaxRetries == 2},
		{"timeouts", f.Routing.UpstreamTimeout == 5*time.Second && f.Routing.RequestTimeout == 10*time.Second},
		{"batch and body", f.Routing.MaxBatchSize == 100 && f.Routing.MaxBodyBytes == 2<<20},
		{"finality", f.Cache.FinalityDepth == 64},
		{"cache off by default", f.Cache.MaxBytes == 0},
		{"key required by default", f.Clients.Anonymous == nil},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("default %s wrong: %+v", c.name, f)
		}
	}
}

func TestZeroRetriesIsKept(t *testing.T) {
	f, err := Parse([]byte("upstreams: [{name: a, url: 'http://a'}]\nrouting: {max_retries: 0}\n"), env(nil))
	if err != nil || *f.Routing.MaxRetries != 0 {
		t.Fatalf("max_retries 0 must mean no retries: %v", err)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"no upstreams", "upstreams: []", "at least one is required"},
		{"missing name", "upstreams: [{url: 'http://a'}]", "name: required"},
		{"duplicate name", "upstreams: [{name: a, url: 'http://a'}, {name: a, url: 'http://b'}]", "duplicated"},
		{"bad url", "upstreams: [{name: a, url: 'ws://a'}]", "must start with http"},
		{"negative weight", "upstreams: [{name: a, url: 'http://a', weight: -1}]", "weight"},
		{"negative rps", "upstreams: [{name: a, url: 'http://a', max_rps: -1}]", "max_rps"},
		{"typo is rejected", "upstreams: [{name: a, url: 'http://a'}]\nhelth: {}", "field helth not found"},
		{"bad duration", "upstreams: [{name: a, url: 'http://a'}]\nhealth: {interval: soon}", "decode yaml"},
		{"probe timeout above interval", "upstreams: [{name: a, url: 'http://a'}]\nhealth: {interval: 1s, timeout: 2s}", "must not exceed"},
		{"too many retries", "upstreams: [{name: a, url: 'http://a'}]\nrouting: {max_retries: 9}", "between 0 and 5"},
		{"negative cache", "upstreams: [{name: a, url: 'http://a'}]\ncache: {max_bytes: -1}", "cache.max_bytes"},
		{"key without rps", "upstreams: [{name: a, url: 'http://a'}]\nclients: {keys: [{key: k}]}", "rps: must be > 0"},
		{"duplicate key", "upstreams: [{name: a, url: 'http://a'}]\nclients: {keys: [{key: k, rps: 1}, {key: k, rps: 1}]}", "duplicated"},
		{"empty key", "upstreams: [{name: a, url: 'http://a'}]\nclients: {keys: [{rps: 1}]}", "key: required"},
		{"anonymous without rps", "upstreams: [{name: a, url: 'http://a'}]\nclients: {anonymous: {burst: 1}}", "anonymous.rps"},
		{"unset env var", "upstreams: [{name: a, url: 'http://a/${MISSING_KEY}'}]", "MISSING_KEY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml), env(nil))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestEnvExpansion(t *testing.T) {
	f, err := Parse([]byte("upstreams: [{name: a, url: 'https://host/v2/${API_KEY}'}]"), env(map[string]string{"API_KEY": "s3cret"}))
	if err != nil || f.Upstreams[0].URL != "https://host/v2/s3cret" {
		t.Fatalf("got %+v, %v", f.Upstreams, err)
	}
}

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rpcgate.yaml")
	if err := os.WriteFile(path, []byte("upstreams: [{name: a, url: 'http://a'}]"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(env(map[string]string{EnvConfigPath: path, EnvLogLevel: "debug"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":8545" || cfg.LogLevel.String() != "DEBUG" {
		t.Fatalf("cfg = %+v", cfg)
	}

	if _, err := Load(env(map[string]string{EnvConfigPath: path, EnvLogLevel: "loud"})); err == nil {
		t.Fatal("bad log level accepted")
	}
	if _, err := Load(env(map[string]string{EnvConfigPath: filepath.Join(t.TempDir(), "missing.yaml")})); err == nil {
		t.Fatal("missing file accepted")
	}
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	_ = os.WriteFile(bad, []byte("upstreams: []"), 0o600)
	if _, err := Load(env(map[string]string{EnvConfigPath: bad})); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("error should name the file: %v", err)
	}
}
