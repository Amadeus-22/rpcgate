// Package lrucache is an in-memory LRU cache bounded by total bytes.
package lrucache

import (
	"container/list"
	"context"
	"sync"
)

// entryOverhead approximates the per-entry bookkeeping (list element, map
// slot, string and slice headers) so many tiny entries still count.
const entryOverhead = 128

// Cache is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	maxBytes int
	used     int
	order    *list.List // front = most recently used
	items    map[string]*list.Element
}

type entry struct {
	key   string
	value []byte
}

// New returns a cache holding at most maxBytes (keys, values and overhead).
func New(maxBytes int) *Cache {
	return &Cache{maxBytes: maxBytes, order: list.New(), items: make(map[string]*list.Element)}
}

func size(key string, value []byte) int { return len(key) + len(value) + entryOverhead }

// Get returns a copy-free view of the stored value; callers must not modify it.
func (c *Cache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*entry).value, true
}

// Set stores value, evicting least recently used entries to stay under the
// cap. A value larger than the whole cache is not stored.
func (c *Cache) Set(_ context.Context, key string, value []byte) {
	n := size(key, value)
	if n > c.maxBytes {
		return
	}
	stored := append([]byte(nil), value...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		old := el.Value.(*entry)
		c.used += n - size(key, old.value)
		old.value = stored
		c.order.MoveToFront(el)
	} else {
		c.items[key] = c.order.PushFront(&entry{key: key, value: stored})
		c.used += n
	}
	for c.used > c.maxBytes {
		oldest := c.order.Back()
		e := oldest.Value.(*entry)
		c.order.Remove(oldest)
		delete(c.items, e.key)
		c.used -= size(e.key, e.value)
	}
}

// Len returns the number of entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
