package app

import (
	"errors"
	"sync"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/ratelimit"
)

// Client rate-limit failures.
var (
	ErrKeyRequired = errors.New("api key required")
	ErrUnknownKey  = errors.New("unknown api key")
	ErrRateLimited = errors.New("client rate limit exceeded")
)

// ClientLimit is the allowance for one API key.
type ClientLimit struct {
	Key   string
	RPS   float64
	Burst int
}

// RateLimiter keeps one token bucket per API key, plus an optional bucket
// shared by all anonymous clients. Each batch item costs one token.
type RateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*ratelimit.Bucket
	anonymous *ratelimit.Bucket // nil: requests without a key are refused
	now       func() time.Time
}

// NewRateLimiter builds a limiter. anonymous == nil requires every client to
// send a known X-Api-Key.
func NewRateLimiter(keys []ClientLimit, anonymous *ClientLimit) *RateLimiter {
	now := time.Now()
	l := &RateLimiter{buckets: make(map[string]*ratelimit.Bucket, len(keys)), now: time.Now}
	for _, k := range keys {
		b := ratelimit.NewBucket(k.RPS, k.Burst, now)
		l.buckets[k.Key] = &b
	}
	if anonymous != nil {
		b := ratelimit.NewBucket(anonymous.RPS, anonymous.Burst, now)
		l.anonymous = &b
	}
	return l
}

// Allow takes n tokens for key ("" means anonymous).
func (l *RateLimiter) Allow(key string, n int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.anonymous
	if key != "" {
		var ok bool
		if b, ok = l.buckets[key]; !ok {
			return ErrUnknownKey
		}
	}
	if b == nil {
		return ErrKeyRequired
	}
	if !b.AllowN(l.now(), n) {
		return ErrRateLimited
	}
	return nil
}
