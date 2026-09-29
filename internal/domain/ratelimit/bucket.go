// Package ratelimit implements a token bucket with the clock passed in, so
// the arithmetic is pure and testable. Callers serialise access.
package ratelimit

import "time"

// Bucket refills at Rate tokens per second up to Burst tokens.
type Bucket struct {
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket. rate <= 0 means unlimited.
func NewBucket(rate float64, burst int, now time.Time) Bucket {
	b := float64(burst)
	if b < 1 {
		b = 1
	}
	return Bucket{rate: rate, burst: b, tokens: b, last: now}
}

// Unlimited reports whether the bucket never refuses.
func (b *Bucket) Unlimited() bool { return b.rate <= 0 }

// AllowN takes n tokens if available and reports whether it did. A request
// larger than the burst can never pass and is refused immediately.
func (b *Bucket) AllowN(now time.Time, n int) bool {
	if b.Unlimited() {
		return true
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(b.burst, b.tokens+elapsed*b.rate)
		b.last = now
	}
	if float64(n) > b.tokens {
		return false
	}
	b.tokens -= float64(n)
	return true
}
