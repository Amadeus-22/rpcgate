package ratelimit

import (
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := NewBucket(10, 5, t0) // 10 tokens/s, burst 5
	steps := []struct {
		at   time.Duration
		n    int
		want bool
	}{
		{0, 3, true},                      // 2 left
		{0, 3, false},                     // not enough, nothing taken
		{0, 2, true},                      // 0 left
		{50 * time.Millisecond, 1, false}, // 0.5 refilled
		{100 * time.Millisecond, 1, true}, // 1 refilled
		{10 * time.Second, 5, true},       // refill capped at burst
		{10 * time.Second, 1, false},      // cap really applied
		{20 * time.Second, 6, false},      // larger than burst never passes
		{20 * time.Second, 5, true},       // refused request took nothing
		{19 * time.Second, 1, false},      // clock going backwards adds nothing
	}
	for i, s := range steps {
		if got := b.AllowN(t0.Add(s.at), s.n); got != s.want {
			t.Fatalf("step %d: AllowN(%v, %d) = %v, want %v", i, s.at, s.n, got, s.want)
		}
	}
}

func TestUnlimitedAndMinimumBurst(t *testing.T) {
	t0 := time.Now()
	u := NewBucket(0, 0, t0)
	if !u.Unlimited() || !u.AllowN(t0, 1_000_000) {
		t.Fatal("rate 0 must be unlimited")
	}
	b := NewBucket(1, 0, t0)
	if !b.AllowN(t0, 1) || b.AllowN(t0, 1) {
		t.Fatal("burst < 1 should behave as burst 1")
	}
}
