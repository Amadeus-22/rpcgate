// Package config loads rpcgate's configuration once at startup. Process
// settings come from environment variables; the upstream topology (a list of
// structured records) comes from a YAML file named by RPCGATE_CONFIG.
// Upstream URLs and client keys may reference environment variables as
// ${NAME}, so provider API keys stay out of the file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the validated configuration.
type Config struct {
	ListenAddr string
	LogLevel   slog.Level
	File       File
}

// File is the YAML document.
type File struct {
	ChainID   uint64     `yaml:"chain_id"`
	Upstreams []Upstream `yaml:"upstreams"`
	Health    Health     `yaml:"health"`
	Breaker   Breaker    `yaml:"breaker"`
	Routing   Routing    `yaml:"routing"`
	Cache     Cache      `yaml:"cache"`
	Clients   Clients    `yaml:"clients"`
}

// Upstream is one JSON-RPC provider.
type Upstream struct {
	Name   string  `yaml:"name"`
	URL    string  `yaml:"url"`
	Weight int     `yaml:"weight"`
	MaxRPS float64 `yaml:"max_rps"`
}

// Health configures probing.
type Health struct {
	Interval     time.Duration `yaml:"interval"`
	Timeout      time.Duration `yaml:"timeout"`
	MaxLagBlocks uint64        `yaml:"max_lag_blocks"`
}

// Breaker configures every upstream's circuit breaker.
type Breaker struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	Cooldown         time.Duration `yaml:"cooldown"`
}

// Routing configures the router and the HTTP endpoint.
type Routing struct {
	MaxRetries      *int          `yaml:"max_retries"` // nil until defaults apply; 0 is meaningful
	UpstreamTimeout time.Duration `yaml:"upstream_timeout"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
	MaxBatchSize    int           `yaml:"max_batch_size"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
}

// Cache configures the response cache.
type Cache struct {
	MaxBytes      int    `yaml:"max_bytes"`
	FinalityDepth uint64 `yaml:"finality_depth"`
}

// Clients configures client rate limits.
type Clients struct {
	Anonymous *Limit `yaml:"anonymous"` // nil: an API key is required
	Keys      []Key  `yaml:"keys"`
}

// Limit is a token bucket.
type Limit struct {
	RPS   float64 `yaml:"rps"`
	Burst int     `yaml:"burst"`
}

// Key is one client API key and its limit.
type Key struct {
	Key   string `yaml:"key"`
	Limit `yaml:",inline"`
}

// Environment variables read by Load.
const (
	EnvConfigPath = "RPCGATE_CONFIG"
	EnvListenAddr = "RPCGATE_LISTEN_ADDR"
	EnvLogLevel   = "RPCGATE_LOG_LEVEL"
)

// Load reads the environment through getenv and the YAML file it names.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{ListenAddr: or(getenv(EnvListenAddr), ":8545")}
	if err := cfg.LogLevel.UnmarshalText([]byte(or(getenv(EnvLogLevel), "info"))); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", EnvLogLevel, err)
	}
	path := or(getenv(EnvConfigPath), "config.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}
	cfg.File, err = Parse(raw, getenv)
	if err != nil {
		return Config{}, fmt.Errorf("config file %s: %w", path, err)
	}
	return cfg, nil
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Parse decodes the YAML strictly (unknown keys are errors, so a typo cannot
// silently fall back to a default), expands ${NAME} references in upstream
// URLs and client keys, applies defaults and validates.
func Parse(raw []byte, getenv func(string) string) (File, error) {
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return File{}, fmt.Errorf("decode yaml: %w", err)
	}
	var missing []string
	expand := func(s string) string {
		return envRef.ReplaceAllStringFunc(s, func(m string) string {
			name := envRef.FindStringSubmatch(m)[1]
			v := getenv(name)
			if v == "" {
				missing = append(missing, name)
			}
			return v
		})
	}
	for i := range f.Upstreams {
		f.Upstreams[i].URL = expand(f.Upstreams[i].URL)
	}
	for i := range f.Clients.Keys {
		f.Clients.Keys[i].Key = expand(f.Clients.Keys[i].Key)
	}
	if len(missing) > 0 {
		return File{}, fmt.Errorf("unset environment variables referenced: %s", strings.Join(missing, ", "))
	}
	f.applyDefaults()
	if err := f.validate(); err != nil {
		return File{}, err
	}
	return f, nil
}

func (f *File) applyDefaults() {
	for i := range f.Upstreams {
		if f.Upstreams[i].Weight == 0 {
			f.Upstreams[i].Weight = 1
		}
	}
	setDur(&f.Health.Interval, 2*time.Second)
	setDur(&f.Health.Timeout, time.Second)
	setUint(&f.Health.MaxLagBlocks, 5)
	setInt(&f.Breaker.FailureThreshold, 5)
	setDur(&f.Breaker.Cooldown, 10*time.Second)
	if f.Routing.MaxRetries == nil {
		two := 2
		f.Routing.MaxRetries = &two
	}
	setDur(&f.Routing.UpstreamTimeout, 5*time.Second)
	setDur(&f.Routing.RequestTimeout, 10*time.Second)
	setInt(&f.Routing.MaxBatchSize, 100)
	if f.Routing.MaxBodyBytes == 0 {
		f.Routing.MaxBodyBytes = 2 << 20
	}
	setUint(&f.Cache.FinalityDepth, 64)
	for i := range f.Clients.Keys {
		setInt(&f.Clients.Keys[i].Burst, max(1, int(f.Clients.Keys[i].RPS)))
	}
	if a := f.Clients.Anonymous; a != nil {
		setInt(&a.Burst, max(1, int(a.RPS)))
	}
}

func (f *File) validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(f.Upstreams) == 0 {
		add("upstreams: at least one is required")
	}
	names := map[string]bool{}
	for i, u := range f.Upstreams {
		switch {
		case u.Name == "":
			add("upstreams[%d].name: required", i)
		case names[u.Name]:
			add("upstreams[%d].name: %q is duplicated", i, u.Name)
		}
		names[u.Name] = true
		if !strings.HasPrefix(u.URL, "http://") && !strings.HasPrefix(u.URL, "https://") {
			add("upstreams[%d].url: must start with http:// or https://", i)
		}
		if u.Weight < 0 {
			add("upstreams[%d].weight: must be >= 0", i)
		}
		if u.MaxRPS < 0 {
			add("upstreams[%d].max_rps: must be >= 0 (0 = unlimited)", i)
		}
	}
	if f.Health.Interval <= 0 || f.Health.Timeout <= 0 {
		add("health.interval and health.timeout: must be > 0")
	}
	if f.Health.Timeout > f.Health.Interval {
		add("health.timeout: must not exceed health.interval")
	}
	if f.Breaker.FailureThreshold < 1 || f.Breaker.Cooldown <= 0 {
		add("breaker: failure_threshold must be >= 1 and cooldown > 0")
	}
	if *f.Routing.MaxRetries < 0 || *f.Routing.MaxRetries > 5 {
		add("routing.max_retries: must be between 0 and 5")
	}
	if f.Routing.UpstreamTimeout <= 0 || f.Routing.RequestTimeout <= 0 {
		add("routing.upstream_timeout and routing.request_timeout: must be > 0")
	}
	if f.Routing.MaxBatchSize < 1 || f.Routing.MaxBodyBytes < 1 {
		add("routing.max_batch_size and routing.max_body_bytes: must be >= 1")
	}
	if f.Cache.MaxBytes < 0 {
		add("cache.max_bytes: must be >= 0 (0 disables the cache)")
	}
	keys := map[string]bool{}
	for i, k := range f.Clients.Keys {
		switch {
		case k.Key == "":
			add("clients.keys[%d].key: required", i)
		case keys[k.Key]:
			add("clients.keys[%d].key: duplicated", i)
		}
		keys[k.Key] = true
		if k.RPS <= 0 {
			add("clients.keys[%d].rps: must be > 0", i)
		}
	}
	if a := f.Clients.Anonymous; a != nil && a.RPS <= 0 {
		add("clients.anonymous.rps: must be > 0 (omit the block to require a key)")
	}
	return errors.Join(errs...)
}

func or(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func setDur(p *time.Duration, d time.Duration) {
	if *p == 0 {
		*p = d
	}
}

func setInt(p *int, d int) {
	if *p == 0 {
		*p = d
	}
}

func setUint(p *uint64, d uint64) {
	if *p == 0 {
		*p = d
	}
}
