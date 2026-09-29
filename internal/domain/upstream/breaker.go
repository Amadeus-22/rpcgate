package upstream

import "time"

// BreakerState is the circuit breaker position.
type BreakerState int

const (
	BreakerClosed   BreakerState = iota // traffic flows
	BreakerHalfOpen                     // one trial call allowed
	BreakerOpen                         // no traffic until cooldown elapses
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerHalfOpen:
		return "half_open"
	case BreakerOpen:
		return "open"
	}
	return "unknown"
}

// Breaker is a consecutive-failure circuit breaker. It is not safe for
// concurrent use; the owner serialises access. Time is passed in so the
// logic stays pure and testable.
type Breaker struct {
	threshold int
	cooldown  time.Duration

	state       BreakerState
	failures    int
	openedAt    time.Time
	trialActive bool
}

// NewBreaker opens after threshold consecutive failures and allows a trial
// call after cooldown. threshold < 1 is treated as 1.
func NewBreaker(threshold int, cooldown time.Duration) Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return Breaker{threshold: threshold, cooldown: cooldown}
}

// State returns the position as of now: an open breaker whose cooldown has
// elapsed reports half-open even before a trial is taken.
func (b *Breaker) State(now time.Time) BreakerState {
	if b.state == BreakerOpen && now.Sub(b.openedAt) >= b.cooldown {
		return BreakerHalfOpen
	}
	return b.state
}

// Ready reports whether Allow would succeed, without changing anything.
func (b *Breaker) Ready(now time.Time) bool {
	switch b.State(now) {
	case BreakerClosed:
		return true
	case BreakerHalfOpen:
		return !b.trialActive
	}
	return false
}

// Allow reserves the right to send one call. In half-open only one trial
// call is in flight at a time; its outcome decides the next state.
func (b *Breaker) Allow(now time.Time) bool {
	if !b.Ready(now) {
		return false
	}
	if b.State(now) == BreakerHalfOpen {
		b.state = BreakerHalfOpen
		b.trialActive = true
	}
	return true
}

// Success records a successful call and closes the breaker.
func (b *Breaker) Success() {
	b.state = BreakerClosed
	b.failures = 0
	b.trialActive = false
}

// Failure records a failed call. It reports whether this call opened the breaker.
func (b *Breaker) Failure(now time.Time) (opened bool) {
	b.trialActive = false
	if b.state == BreakerHalfOpen {
		b.state = BreakerOpen
		b.openedAt = now
		return true
	}
	b.failures++
	if b.state == BreakerClosed && b.failures >= b.threshold {
		b.state = BreakerOpen
		b.openedAt = now
		return true
	}
	return false
}

// Release gives back a reserved call whose outcome says nothing about the
// upstream (the client went away first).
func (b *Breaker) Release() { b.trialActive = false }
