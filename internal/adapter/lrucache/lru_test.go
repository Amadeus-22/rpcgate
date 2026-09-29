package lrucache

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

func TestGetSet(t *testing.T) {
	ctx := context.Background()
	c := New(1 << 10)
	if _, ok := c.Get(ctx, "a"); ok {
		t.Fatal("empty cache hit")
	}
	v := []byte("value")
	c.Set(ctx, "a", v)
	v[0] = 'X' // the cache must own its copy
	got, ok := c.Get(ctx, "a")
	if !ok || string(got) != "value" {
		t.Fatalf("got %q %v", got, ok)
	}
	c.Set(ctx, "a", []byte("longer value"))
	if got, _ := c.Get(ctx, "a"); string(got) != "longer value" || c.Len() != 1 {
		t.Fatalf("overwrite: got %q len %d", got, c.Len())
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	per := size("k0", []byte("v"))
	c := New(3 * per) // room for exactly three entries
	for i := range 3 {
		c.Set(ctx, "k"+strconv.Itoa(i), []byte("v"))
	}
	c.Get(ctx, "k0")              // k0 becomes most recent; k1 is now oldest
	c.Set(ctx, "k3", []byte("v")) // evicts k1
	if _, ok := c.Get(ctx, "k1"); ok {
		t.Fatal("k1 should have been evicted")
	}
	for _, k := range []string{"k0", "k2", "k3"} {
		if _, ok := c.Get(ctx, k); !ok {
			t.Fatalf("%s evicted", k)
		}
	}
	if c.Len() != 3 {
		t.Fatalf("len = %d", c.Len())
	}
}

func TestGrowingAnEntryEvictsOthers(t *testing.T) {
	ctx := context.Background()
	c := New(2*entryOverhead + 20)
	c.Set(ctx, "a", []byte("1"))
	c.Set(ctx, "b", []byte("1"))
	c.Set(ctx, "b", make([]byte, 20)) // b grows; a (older) must go
	if _, ok := c.Get(ctx, "a"); ok || c.Len() != 1 {
		t.Fatalf("len = %d", c.Len())
	}
}

func TestOversizedValueIsNotStored(t *testing.T) {
	ctx := context.Background()
	c := New(entryOverhead + 4)
	c.Set(ctx, "k", []byte("12345"))
	if _, ok := c.Get(ctx, "k"); ok {
		t.Fatal("value larger than the cache was stored")
	}
}

func TestConcurrentUse(t *testing.T) {
	ctx := context.Background()
	c := New(50 * entryOverhead)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				k := strconv.Itoa((g * i) % 97)
				c.Set(ctx, k, []byte(k))
				c.Get(ctx, k)
			}
		}()
	}
	wg.Wait()
	if c.Len() > 50 {
		t.Fatalf("len %d exceeds cap", c.Len())
	}
}
