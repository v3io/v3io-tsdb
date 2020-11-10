package appender

import (
	clist "container/list"
	"fmt"
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
		c.free.Remove(ele)
		c.used.Remove(ele)
	}
	newEle := c.used.PushFront(&entry{key, value})
	c.cache[key] = newEle
	fmt.Printf("ADD used %+v free %+v \n ", c.used, c.free)
	if c.maxEntries != 0 && c.free.Len()+c.used.Len() > c.maxEntries {
		c.removeOldest()
		fmt.Printf("remove used %+v free %+v \n ", c.used, c.free)
	}

}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key uint64) (value *MetricState, ok bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if ele, hit := c.cache[key]; hit {
		c.free.Remove(ele)
		c.used.Remove(ele)
		newEle := c.used.PushFront(&entry{key, ele.Value.(*entry).value})
		c.cache[key] = newEle
		fmt.Printf("GET used %+v free %+v \n ", c.used, c.free)
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *Cache) removeOldest() {
	for {
		ele := c.free.Back()
		if ele != nil {
			c.free.Remove(ele)
			kv := ele.Value.(*entry)
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
		newEle := c.free.PushFront(&entry{key, ele.Value.(*entry).value})
		c.cache[key] = newEle
		fmt.Printf("RESET used %+v free %+v \n", c.used, c.free)
		c.cond.Signal()
	}
}
