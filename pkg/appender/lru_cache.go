package appender

import (
	clist "container/list"
	"sync"
)

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	free  *clist.List
	used  *clist.List
	cache map[interface{}]*clist.Element
	cond  *sync.Cond
	mtx   sync.RWMutex
}

type entry struct {
	key   uint64
	value *MetricState
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewCache(maxEntries int) *Cache {
	newCache := Cache{
		MaxEntries: maxEntries,
		free:       clist.New(),
		used:       clist.New(),
		cache:      make(map[interface{}]*clist.Element),
	}
	newCache.cond = sync.NewCond(&newCache.mtx)
	return &newCache
}

// Add adds a value to the cache.
func (c *Cache) Add(key uint64, value *MetricState) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.cache == nil {
		c.cache = make(map[interface{}]*clist.Element)
		c.free = clist.New()
	}
	if ee, ok := c.cache[key]; ok {
		if ee.Value.(*entry).value.getState() == storeStateInit {
			c.free.MoveToFront(ee)
		}
		ee.Value.(*entry).value = value
		return
	}
	ele := c.used.PushFront(&entry{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.free.Len()+c.used.Len() > c.MaxEntries {
		c.removeOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key uint64) (value *MetricState, ok bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.free.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) removeOldest() {
	if c.cache == nil {
		return
	}
	ele := c.free.Back()
	if ele != nil {
		c.free.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
	} else {
		c.cond.Wait()
		c.removeOldest()
	}
}

func (c *Cache) ResetMetric(metric *MetricState) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.cache == nil {
		return
	}
	if ele, ok := c.cache[metric.hash]; ok {
		c.used.Remove(ele)
		c.free.PushFront(ele)
		c.cond.Signal()
	}
}
