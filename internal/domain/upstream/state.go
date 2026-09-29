// Package upstream models what rpcgate knows about one JSON-RPC provider:
// reachability, head, latency, error rate and circuit breaker. It is pure;
// callers pass the current time and serialise access.
package upstream

import (
	"errors"
	"time"
)

// Failure classes reported by upstream clients. The router decides on retries
// with errors.Is against these.
var (
	ErrUnreachable = errors.New("upstream unreachable")
	ErrTimeout     = errors.New("upstream timed out")
	ErrRateLimited = errors.New("upstream rate limited")
	ErrServer      = errors.New("upstream server error")
	ErrRejected    = errors.New("upstream rejected the request")
	ErrBadResponse = errors.New("upstream returned an invalid response")
)

// Retryable reports whether a failed call may be tried on another upstream.
// A rejection (4xx other than 429) means the request itself is the problem.
func Retryable(err error) bool {
	return err != nil && !errors.Is(err, ErrRejected)
}

// Settings are the per-upstream knobs that shape its state.
type Settings struct {
	BreakerThreshold int
	BreakerCooldown  time.Duration
	LatencyAlpha     float64
	ErrorRateAlpha   float64
}

// State is everything rpcgate tracks about one upstream.
type State struct {
	Name   string
	Weight int

	Reachable bool   // last probe succeeded
	Head      uint64 // valid when Reachable was ever true
	LastError string

	latency   EWMA // probe latency in seconds
	errorRate EWMA // 1 per failed call, 0 per success
	breaker   Breaker
}

// NewState returns an upstream that is unreachable until its first probe.
func NewState(name string, weight int, s Settings) State {
	return State{
		Name:      name,
		Weight:    weight,
		latency:   NewEWMA(s.LatencyAlpha),
		errorRate: NewEWMA(s.ErrorRateAlpha),
		breaker:   NewBreaker(s.BreakerThreshold, s.BreakerCooldown),
	}
}

// ProbeSucceeded records a successful head probe.
func (s *State) ProbeSucceeded(head uint64, latency time.Duration) {
	s.Reachable = true
	s.Head = head
	s.LastError = ""
	s.latency.Observe(latency.Seconds())
}

// ProbeFailed records a failed head probe; the upstream stops receiving traffic.
func (s *State) ProbeFailed(err error) {
	s.Reachable = false
	s.LastError = err.Error()
}

// Acquire reserves a call slot from the breaker.
func (s *State) Acquire(now time.Time) bool { return s.breaker.Allow(now) }

// CallSucceeded records a call that got an answer.
func (s *State) CallSucceeded() {
	s.breaker.Success()
	s.errorRate.Observe(0)
}

// CallFailed records a failed call. It reports whether the breaker opened.
func (s *State) CallFailed(now time.Time, err error) (opened bool) {
	s.LastError = err.Error()
	s.errorRate.Observe(1)
	return s.breaker.Failure(now)
}

// CallAbandoned releases a call slot whose outcome says nothing about the upstream.
func (s *State) CallAbandoned() { s.breaker.Release() }

// Latency is the EWMA of probe latency.
func (s *State) Latency() time.Duration {
	return time.Duration(s.latency.Value() * float64(time.Second))
}

// ErrorRate is the EWMA of call failures, between 0 and 1.
func (s *State) ErrorRate() float64 { return s.errorRate.Value() }

// Breaker returns the breaker position as of now.
func (s *State) Breaker(now time.Time) BreakerState { return s.breaker.State(now) }

// BreakerReady reports whether the breaker would admit a call now.
func (s *State) BreakerReady(now time.Time) bool { return s.breaker.Ready(now) }
