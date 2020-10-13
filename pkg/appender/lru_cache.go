package appender

import (
	clist "container/list"

	"github.com/pkg/errors"
)

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	ll    *clist.List
	cache map[interface{}]*clist.Element
}

type entry struct {
	key   uint64
	value *MetricState
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewCache(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         clist.New(),
		cache:      make(map[interface{}]*clist.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key uint64, value *MetricState) error {
	if c.cache == nil {
		c.cache = make(map[interface{}]*clist.Element)
		c.ll = clist.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return nil
	}
	ele := c.ll.PushFront(&entry{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		return c.RemoveOldest()
	}
	return nil
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key uint64) (value *MetricState, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() error {
	if c.cache == nil {
		return nil
	}
	ele := c.ll.Back()
	if ele != nil {
		if ele.Value.(*entry).value.state == storeStateInit {
			c.removeElement(ele)
		} else {
			return errors.Errorf("Can't remove metric from cache")
		}
	}
	return nil
}

func (c *Cache) removeElement(e *clist.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *Cache) Clear() {
	c.ll = nil
	c.cache = nil
}
