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
	if ee, ok := c.cache[key]; ok {
		c.free.Remove(ee)
		//check if element was already in list and if not push to front
		c.used.MoveToFront(ee)
		if c.used.Front() != ee {
			c.used.PushFront(ee)
		}
		ee.Value.(*entry).value = value
		return
	}
	ele := c.used.PushFront(&entry{key, value})
	c.cache[key] = ele
	if c.maxEntries != 0 && c.free.Len()+c.used.Len() > c.maxEntries {
		c.removeOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key uint64) (value *MetricState, ok bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, hit := c.cache[key]; hit {
		c.free.Remove(ele)
		//check if element was already in list and if not push to front
		c.used.MoveToFront(ele)
		if c.used.Front() != ele {
			c.used.PushFront(ele)
		}
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *Cache) removeOldest() {
	for {
		ele := c.free.Back()
		if ele != nil {
			c.free.Remove(ele)
			kv := ele.Value.(*clist.Element).Value.(*entry)
			delete(c.cache, kv.key)
			return
		}
		c.cond.Wait()
	}
}

func (c *Cache) ResetMetric(key uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, ok := c.cache[key]; ok {
		c.used.Remove(ele)
		c.free.PushFront(ele)
		c.cond.Signal()
	}
}
