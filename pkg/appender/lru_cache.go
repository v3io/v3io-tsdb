package appender

import (
	clist "container/list"
	"sync"
)

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxEntries int
	free       *clist.List
	used       *clist.List
	cache      map[uint64]*clist.Element
	cond       *sync.Cond
	mtx        sync.Mutex
}

type entry struct {
	key   uint64
	value *MetricState
}

func NewCache(max int) *Cache {
	newCache := Cache{
		maxEntries: max,
		free:       clist.New(),
		used:       clist.New(),
		cache:      make(map[uint64]*clist.Element),
	}
	newCache.cond = sync.NewCond(&newCache.mtx)
	return &newCache
}

// Add adds a value to the cache.
func (c *Cache) Add(key uint64, value *MetricState) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, ok := c.cache[key]; ok {
		//deleting from both lists as a workaround to not knowing which list actually contains the elem
		//if the elem is not present in the list Remove won't do anything
		// Remove clears ele's head,prev and next pointers so it can't be reused
		c.free.Remove(ele)
		c.used.Remove(ele)
	}
	c.cache[key] = c.used.PushFront(&entry{key, value})
	if c.maxEntries != 0 && c.free.Len()+c.used.Len() > c.maxEntries {
		c.removeOldest()
	}

}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key uint64) (value *MetricState, ok bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, hit := c.cache[key]; hit {
		//deleting from both lists as a workaround to not knowing which list actually contains the elem
		//if the elem is not present in the list Remove won't do anything
		// Remove clears ele's head,prev and next pointers so it can't be reused
		c.free.Remove(ele)
		c.used.Remove(ele)
		c.cache[key] = c.used.PushFront(&entry{key, ele.Value.(*entry).value})
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *Cache) removeOldest() {
	for {
		ele := c.free.Back()
		if ele != nil {
			c.free.Remove(ele)
			delete(c.cache, ele.Value.(*entry).key)
			return
		}
		c.cond.Wait()
	}
}

func (c *Cache) ResetMetric(key uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, ok := c.cache[key]; ok {
		//deleting from both lists as a workaround to not knowing which list actually contains the elem
		//if the elem is not present in the list Remove won't do anything
		// Remove clears ele's head,prev and next pointers so it can't be reused
		c.used.Remove(ele)
		c.free.Remove(ele)
		c.cache[key] = c.free.PushFront(&entry{key, ele.Value.(*entry).value})
		c.cond.Signal()
	}
}
