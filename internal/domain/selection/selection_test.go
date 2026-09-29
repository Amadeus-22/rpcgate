package selection

import (
	"reflect"
	"testing"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
)

func cand(name string, head uint64, latency time.Duration, weight int) Candidate {
	return Candidate{Name: name, Weight: weight, Reachable: true, BreakerReady: true, Head: head, Latency: latency}
}

func names(cs []Candidate) []string {
	out := []string{}
	for _, c := range cs {
		out = append(out, c.Name)
	}
	return out
}

func TestEligible(t *testing.T) {
	ms := time.Millisecond
	down := cand("down", 200, 0, 1)
	down.Reachable = false
	open := cand("open", 200, 0, 1)
	open.BreakerReady = false

	base := []Candidate{
		cand("a", 100, 10*ms, 1),
		cand("b", 100, 5*ms, 1),
		cand("lagging", 90, 1*ms, 1),
		cand("slightly-behind", 98, 2*ms, 1),
		down, open,
	}
	tip := jsonrpc.BlockRef{Kind: jsonrpc.RefTip}
	num := func(n uint64) jsonrpc.BlockRef { return jsonrpc.BlockRef{Kind: jsonrpc.RefNumber, Number: n} }
	anyRef := jsonrpc.BlockRef{Kind: jsonrpc.RefAny}

	tests := []struct {
		name  string
		cands []Candidate
		ref   jsonrpc.BlockRef
		want  []string
	}{
		{"tip only at best head, fastest first", base, tip, []string{"b", "a"}},
		{"number above lagging head excludes it", base, num(95), []string{"slightly-behind", "b", "a"}},
		{"old number uses lagging upstream too", base, num(50), []string{"lagging", "slightly-behind", "b", "a"}},
		{"number at best head", base, num(100), []string{"b", "a"}},
		{"future block goes to tip upstreams", base, num(5000), []string{"b", "a"}},
		{"any excludes lag above max", base, anyRef, []string{"slightly-behind", "b", "a"}},
		{"nothing available", []Candidate{down, open}, tip, nil},
		{"empty", nil, anyRef, nil},
		{"same latency band: weight decides", []Candidate{cand("x", 1, 1*ms, 1), cand("y", 1, 0, 5)}, tip, []string{"y", "x"}},
		{"same band and weight: name decides", []Candidate{cand("z", 1, 0, 1), cand("y", 1, 0, 1)}, tip, []string{"y", "z"}},
		{"different band beats weight", []Candidate{cand("heavy", 1, 9*ms, 10), cand("light", 1, 0, 1)}, tip, []string{"light", "heavy"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Eligible(tt.cands, tt.ref, 5, 0)
			if tt.want == nil {
				if len(got) != 0 {
					t.Fatalf("got %v, want none", names(got))
				}
				return
			}
			if !reflect.DeepEqual(names(got), tt.want) {
				t.Fatalf("got %v, want %v", names(got), tt.want)
			}
		})
	}
}

func TestSpreadRotatesOnlyEqualTopTier(t *testing.T) {
	ms := time.Millisecond
	cands := []Candidate{cand("a", 1, 0, 1), cand("b", 1, 0, 1), cand("c", 1, 0, 1), cand("slow", 1, 10*ms, 1), cand("light", 1, 0, 0)}
	tip := jsonrpc.BlockRef{Kind: jsonrpc.RefTip}
	want := [][]string{
		{"a", "b", "c", "light", "slow"},
		{"b", "c", "a", "light", "slow"},
		{"c", "a", "b", "light", "slow"},
		{"a", "b", "c", "light", "slow"},
	}
	for spread, w := range want {
		if got := names(Eligible(cands, tip, 5, uint64(spread))); !reflect.DeepEqual(got, w) {
			t.Fatalf("spread %d: got %v, want %v", spread, got, w)
		}
	}
	if got := names(Eligible(cands[3:4], tip, 5, 7)); !reflect.DeepEqual(got, []string{"slow"}) {
		t.Fatalf("single candidate: %v", got)
	}
}

func TestBestHeadIgnoresBreakerButNotReachability(t *testing.T) {
	down := cand("down", 500, 0, 1)
	down.Reachable = false
	open := cand("open", 300, 0, 1)
	open.BreakerReady = false
	head, ok := BestHead([]Candidate{cand("a", 100, 0, 1), down, open})
	if !ok || head != 300 {
		t.Fatalf("got %d %v", head, ok)
	}
	if _, ok := BestHead([]Candidate{down}); ok {
		t.Fatal("no reachable upstream should report !ok")
	}
}

func TestLag(t *testing.T) {
	if Lag(90, 100) != 10 || Lag(100, 100) != 0 || Lag(101, 100) != 0 {
		t.Fatal("lag arithmetic wrong")
	}
}
