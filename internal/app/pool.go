package app

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
	"github.com/Amadeus-22/rpcgate/internal/domain/ratelimit"
	"github.com/Amadeus-22/rpcgate/internal/domain/selection"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// Member describes one configured upstream.
type Member struct {
	Name   string
	Weight int
	MaxRPS float64 // 0 = unlimited
	Client Upstream
}

// PoolConfig tunes upstream state for every member.
type PoolConfig struct {
	MaxLagBlocks uint64
	State        upstream.Settings
}

// Pool owns the mutable state of every upstream. Each member has its own
// lock, so a slow probe of one upstream never blocks routing to another.
type Pool struct {
	maxLag  uint64
	members []*member
	byName  map[string]*member
	spread  atomic.Uint64 // rotates load across equally ranked upstreams
}

type member struct {
	name   string
	client Upstream

	mu            sync.Mutex
	state         upstream.State
	bucket        ratelimit.Bucket
	chainVerified bool
}

// NewPool builds a pool; names must be unique (config validation enforces it).
func NewPool(cfg PoolConfig, members []Member) *Pool {
	p := &Pool{maxLag: cfg.MaxLagBlocks, byName: make(map[string]*member, len(members))}
	now := time.Now()
	for _, m := range members {
		mm := &member{
			name:   m.Name,
			client: m.Client,
			state:  upstream.NewState(m.Name, m.Weight, cfg.State),
			bucket: ratelimit.NewBucket(m.MaxRPS, max(1, int(m.MaxRPS)), now),
		}
		p.members = append(p.members, mm)
		p.byName[m.Name] = mm
	}
	return p
}

func (p *Pool) candidates(now time.Time, exclude map[string]bool) []selection.Candidate {
	out := make([]selection.Candidate, 0, len(p.members))
	for _, m := range p.members {
		if exclude[m.name] {
			continue
		}
		m.mu.Lock()
		out = append(out, selection.Candidate{
			Name:         m.name,
			Weight:       m.state.Weight,
			Reachable:    m.state.Reachable,
			BreakerReady: m.state.BreakerReady(now),
			Head:         m.state.Head,
			Latency:      m.state.Latency(),
		})
		m.mu.Unlock()
	}
	return out
}

// acquire picks the best eligible upstream not in exclude and reserves a call
// slot on it (breaker admission and max_rps token).
func (p *Pool) acquire(ref jsonrpc.BlockRef, exclude map[string]bool) (*member, error) {
	now := time.Now()
	ordered := selection.Eligible(p.candidates(now, exclude), ref, p.maxLag, p.spread.Add(1)-1)
	if len(ordered) == 0 {
		return nil, ErrNoUpstream
	}
	for _, c := range ordered {
		m := p.byName[c.Name]
		m.mu.Lock()
		// State may have moved since the snapshot; re-check under the lock.
		ok := m.state.BreakerReady(now) && m.bucket.AllowN(now, 1) && m.state.Acquire(now)
		m.mu.Unlock()
		if ok {
			return m, nil
		}
	}
	return nil, fmt.Errorf("%w: every eligible upstream is at max_rps or tripped", ErrNoUpstream)
}

// BestHead is the highest head among reachable upstreams.
func (p *Pool) BestHead() (uint64, bool) {
	return selection.BestHead(p.candidates(time.Now(), nil))
}

// Ready reports whether at least one upstream is reachable.
func (p *Pool) Ready() bool {
	_, ok := p.BestHead()
	return ok
}

// UpstreamStatus is the admin view of one upstream. It never includes the
// URL, which often embeds a provider API key.
type UpstreamStatus struct {
	Name      string  `json:"name"`
	Healthy   bool    `json:"healthy"`
	Reachable bool    `json:"reachable"`
	Head      uint64  `json:"head"`
	Lag       uint64  `json:"lag_blocks"`
	LatencyMS float64 `json:"latency_ms"`
	Breaker   string  `json:"breaker"`
	ErrorRate float64 `json:"error_rate"`
	LastError string  `json:"last_error,omitempty"`
}

// Status returns every upstream in configuration order.
func (p *Pool) Status() []UpstreamStatus {
	now := time.Now()
	best, _ := p.BestHead()
	out := make([]UpstreamStatus, 0, len(p.members))
	for _, m := range p.members {
		m.mu.Lock()
		s := &m.state
		lag := selection.Lag(s.Head, best)
		out = append(out, UpstreamStatus{
			Name:      m.name,
			Healthy:   s.Reachable && lag <= p.maxLag,
			Reachable: s.Reachable,
			Head:      s.Head,
			Lag:       lag,
			LatencyMS: float64(s.Latency().Microseconds()) / 1000,
			Breaker:   s.Breaker(now).String(),
			ErrorRate: s.ErrorRate(),
			LastError: s.LastError,
		})
		m.mu.Unlock()
	}
	return out
}
