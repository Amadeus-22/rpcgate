package upstream

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBreakerLifecycle(t *testing.T) {
	b := NewBreaker(3, 10*time.Second)
	steps := []struct {
		name      string
		at        time.Duration
		act       func(now time.Time) bool
		wantState BreakerState
		wantReady bool
	}{
		{"starts closed", 0, nil, BreakerClosed, true},
		{"one failure stays closed", 0, func(n time.Time) bool { return b.Failure(n) }, BreakerClosed, true},
		{"success resets count", 0, func(time.Time) bool { b.Success(); return false }, BreakerClosed, true},
		{"failure 1", 0, func(n time.Time) bool { return b.Failure(n) }, BreakerClosed, true},
		{"failure 2", 0, func(n time.Time) bool { return b.Failure(n) }, BreakerClosed, true},
		{"failure 3 opens", 0, func(n time.Time) bool { return !b.Failure(n) }, BreakerOpen, false},
		{"still open before cooldown", 9 * time.Second, nil, BreakerOpen, false},
		{"half-open after cooldown", 10 * time.Second, nil, BreakerHalfOpen, true},
		{"trial taken", 10 * time.Second, func(n time.Time) bool { return !b.Allow(n) }, BreakerHalfOpen, false},
		{"second trial refused", 10 * time.Second, func(n time.Time) bool { return b.Allow(n) }, BreakerHalfOpen, false},
		{"trial fails reopens", 11 * time.Second, func(n time.Time) bool { return !b.Failure(n) }, BreakerOpen, false},
		{"half-open again", 21 * time.Second, nil, BreakerHalfOpen, true},
		{"trial abandoned", 21 * time.Second, func(n time.Time) bool { ok := b.Allow(n); b.Release(); return !ok }, BreakerHalfOpen, true},
		{"trial succeeds closes", 21 * time.Second, func(n time.Time) bool { ok := b.Allow(n); b.Success(); return !ok }, BreakerClosed, true},
	}
	for _, s := range steps {
		now := t0.Add(s.at)
		if s.act != nil && s.act(now) {
			t.Fatalf("%s: unexpected action result", s.name)
		}
		if got := b.State(now); got != s.wantState {
			t.Fatalf("%s: state = %s, want %s", s.name, got, s.wantState)
		}
		if got := b.Ready(now); got != s.wantReady {
			t.Fatalf("%s: ready = %v, want %v", s.name, got, s.wantReady)
		}
	}
}

func TestBreakerOpenReportsTransitionOnce(t *testing.T) {
	b := NewBreaker(0, time.Second) // threshold clamps to 1
	if !b.Failure(t0) {
		t.Fatal("first failure should open a threshold-1 breaker")
	}
	if b.Failure(t0) {
		t.Fatal("failure while open must not report opening again")
	}
	if b.Allow(t0) {
		t.Fatal("open breaker allowed a call")
	}
}

func TestBreakerStateString(t *testing.T) {
	for s, want := range map[BreakerState]string{BreakerClosed: "closed", BreakerHalfOpen: "half_open", BreakerOpen: "open", 7: "unknown"} {
		if s.String() != want {
			t.Errorf("%d = %s", s, s.String())
		}
	}
}

func TestEWMA(t *testing.T) {
	e := NewEWMA(0.5)
	if e.Seeded() || e.Value() != 0 {
		t.Fatal("new EWMA should be empty")
	}
	e.Observe(10)
	if e.Value() != 10 {
		t.Fatalf("first sample seeds: %v", e.Value())
	}
	e.Observe(20)
	if e.Value() != 15 {
		t.Fatalf("got %v, want 15", e.Value())
	}
	bad := NewEWMA(7)
	bad.Observe(1)
	bad.Observe(2)
	if math.Abs(bad.Value()-1.3) > 1e-9 {
		t.Fatalf("invalid alpha should fall back to 0.3, got %v", bad.Value())
	}
}

func TestStateProbeAndCalls(t *testing.T) {
	s := NewState("a", 2, Settings{BreakerThreshold: 2, BreakerCooldown: time.Second, LatencyAlpha: 1, ErrorRateAlpha: 0.5})
	if s.Reachable {
		t.Fatal("new upstream must be unreachable until probed")
	}
	s.ProbeSucceeded(100, 20*time.Millisecond)
	if !s.Reachable || s.Head != 100 || s.Latency() != 20*time.Millisecond || s.LastError != "" {
		t.Fatalf("after probe: %+v latency=%v", s, s.Latency())
	}
	s.ProbeFailed(errors.New("dial tcp: refused"))
	if s.Reachable || s.LastError != "dial tcp: refused" || s.Head != 100 {
		t.Fatalf("after failed probe: %+v", s)
	}

	if !s.Acquire(t0) {
		t.Fatal("closed breaker refused")
	}
	s.CallSucceeded()
	if s.ErrorRate() != 0 {
		t.Fatalf("error rate %v", s.ErrorRate())
	}
	if s.CallFailed(t0, ErrServer) {
		t.Fatal("opened after one failure with threshold 2")
	}
	if s.ErrorRate() != 0.5 {
		t.Fatalf("error rate %v", s.ErrorRate())
	}
	if !s.CallFailed(t0, ErrServer) || s.Breaker(t0) != BreakerOpen || s.BreakerReady(t0) {
		t.Fatal("second failure should open the breaker")
	}
	later := t0.Add(time.Second)
	if !s.Acquire(later) {
		t.Fatal("half-open should admit a trial")
	}
	s.CallAbandoned()
	if !s.BreakerReady(later) {
		t.Fatal("abandoned trial should free the slot")
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{ErrUnreachable, true},
		{ErrTimeout, true},
		{fmt.Errorf("call a: %w", ErrRateLimited), true},
		{ErrServer, true},
		{ErrBadResponse, true},
		{fmt.Errorf("call a: %w", ErrRejected), false},
	}
	for _, tt := range tests {
		if got := Retryable(tt.err); got != tt.want {
			t.Errorf("Retryable(%v) = %v", tt.err, got)
		}
	}
}
