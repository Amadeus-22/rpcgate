// Package selection decides which upstreams may serve a request and in what
// order to try them. It is pure: callers pass snapshots of upstream state.
package selection

import (
	"sort"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
)

// LatencyBand is the width of the latency buckets used for ranking. Upstreams
// whose latency falls in the same band are considered equally fast, so the
// configured weight decides between them instead of sub-millisecond noise.
const LatencyBand = 2 * time.Millisecond

// Candidate is a snapshot of one upstream as seen by the selector.
type Candidate struct {
	Name         string
	Weight       int
	Reachable    bool
	BreakerReady bool
	Head         uint64
	Latency      time.Duration
}

func (c Candidate) available() bool { return c.Reachable && c.BreakerReady }

// BestHead is the highest head among reachable upstreams; ok is false when
// none is reachable.
func BestHead(cands []Candidate) (head uint64, ok bool) {
	for _, c := range cands {
		if c.Reachable && (!ok || c.Head > head) {
			head, ok = c.Head, true
		}
	}
	return head, ok
}

// Lag is how far head trails best, never negative.
func Lag(head, best uint64) uint64 {
	if head >= best {
		return 0
	}
	return best - head
}

// Eligible returns the upstreams that may serve a request for ref, best
// first. Upstreams whose breaker is open or whose last probe failed never
// qualify. Then, by ref kind:
//
//   - RefTip: only upstreams at the best head among available ones.
//   - RefNumber: only upstreams whose head >= the block, even lagging ones.
//     A block above every head goes to the tip upstreams, which answer as a
//     node would for a future block.
//   - RefAny: upstreams lagging by at most maxLag blocks.
//
// Order: lowest latency band first, then higher weight, then name. The
// leading group of equally ranked upstreams is rotated by spread (a counter
// the caller increments per request), so equal upstreams share the load
// instead of the alphabetically first one taking all of it.
func Eligible(cands []Candidate, ref jsonrpc.BlockRef, maxLag, spread uint64) []Candidate {
	var avail []Candidate
	var best uint64
	for _, c := range cands {
		if !c.available() {
			continue
		}
		if len(avail) == 0 || c.Head > best {
			best = c.Head
		}
		avail = append(avail, c)
	}
	if len(avail) == 0 {
		return nil
	}

	var out []Candidate
	switch ref.Kind {
	case jsonrpc.RefTip:
		out = filter(avail, func(c Candidate) bool { return c.Head == best })
	case jsonrpc.RefNumber:
		if ref.Number > best {
			out = filter(avail, func(c Candidate) bool { return c.Head == best })
		} else {
			out = filter(avail, func(c Candidate) bool { return c.Head >= ref.Number })
		}
	default:
		out = filter(avail, func(c Candidate) bool { return Lag(c.Head, best) <= maxLag })
	}
	sort.SliceStable(out, func(i, j int) bool { return less(out[i], out[j]) })
	rotateTopTier(out, spread)
	return out
}

func rotateTopTier(out []Candidate, spread uint64) {
	tier := 1
	for tier < len(out) && sameRank(out[0], out[tier]) {
		tier++
	}
	k := int(spread % uint64(tier))
	if k == 0 {
		return
	}
	rotated := append(append(make([]Candidate, 0, tier), out[k:tier]...), out[:k]...)
	copy(out, rotated)
}

func sameRank(a, b Candidate) bool {
	return a.Latency/LatencyBand == b.Latency/LatencyBand && a.Weight == b.Weight
}

func less(a, b Candidate) bool {
	ab, bb := a.Latency/LatencyBand, b.Latency/LatencyBand
	if ab != bb {
		return ab < bb
	}
	if a.Weight != b.Weight {
		return a.Weight > b.Weight
	}
	return a.Name < b.Name
}

func filter(cands []Candidate, keep func(Candidate) bool) []Candidate {
	out := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if keep(c) {
			out = append(out, c)
		}
	}
	return out
}
