package main

import (
	"sync"
	"time"
)

const maxCacheEntries = 10000

// cache is a small TTL cache that also merges identical requests that arrive
// at the same time, so a burst of block_count calls hits the node once.
type cache struct {
	mu       sync.Mutex
	entries  map[string]cacheEntry
	inflight map[string]*call
}

type cacheEntry struct {
	body    []byte
	expires time.Time
}

type call struct {
	done chan struct{}
	body []byte
	err  error
}

func newCache() *cache {
	c := &cache{entries: map[string]cacheEntry{}, inflight: map[string]*call{}}
	go func() {
		for range time.Tick(30 * time.Second) {
			now := time.Now()
			c.mu.Lock()
			for k, e := range c.entries {
				if now.After(e.expires) {
					delete(c.entries, k)
				}
			}
			c.mu.Unlock()
		}
	}()
	return c
}

// get returns a cached body, or runs fetch once for all concurrent callers.
// fetch reports whether its result may be stored.
func (c *cache) get(key string, ttl time.Duration, fetch func() ([]byte, bool, error)) ([]byte, bool, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		return e.body, true, nil
	}
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		<-cl.done
		return cl.body, true, cl.err
	}
	cl := &call{done: make(chan struct{})}
	c.inflight[key] = cl
	c.mu.Unlock()

	body, storable, err := fetch()
	cl.body, cl.err = body, err

	c.mu.Lock()
	delete(c.inflight, key)
	if err == nil && storable && len(c.entries) < maxCacheEntries {
		c.entries[key] = cacheEntry{body: body, expires: time.Now().Add(ttl)}
	}
	c.mu.Unlock()
	close(cl.done)
	return body, false, err
}
