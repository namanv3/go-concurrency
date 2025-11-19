package main

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrCacheClosed = errors.New("cache already closed, cannot accept new tasks")
)

type LoaderFunc func(key string) (value any, err error)

type Data struct {
	value any
	ts    time.Time
	mu    sync.Mutex
}

type CacheShard struct {
	entries map[string]*Data
	mu      sync.RWMutex
}

func NewCacheShard() *CacheShard {
	return &CacheShard{
		entries: map[string]*Data{},
	}
}

type Cache struct {
	shards [16]*CacheShard
	loader LoaderFunc
	ttl    time.Duration

	closed    bool
	closedMu  sync.RWMutex
	closeOnce sync.Once
}

func NewCache(loader LoaderFunc, ttl time.Duration) *Cache {
	shards := [16]*CacheShard{}
	for i := range 16 {
		shards[i] = NewCacheShard()
	}
	return &Cache{
		shards: shards,
		loader: loader,
		ttl:    ttl,
	}
}

func (c *Cache) Get(key string) (value any, err error) {
	c.closedMu.RLock()
	defer c.closedMu.RUnlock()
	if c.closed {
		return nil, ErrCacheClosed
	}

	shardNum := c.getShardIdx(key)
	shard := c.shards[shardNum]
	shard.mu.RLock()

	for {
		data, ok := shard.entries[key]
		if !ok {
			shard.mu.RUnlock()
			shard.mu.Lock()
			_, ok = shard.entries[key]
			if !ok {
				data = &Data{
					ts: time.Now().Add(-2 * c.ttl),
				}
				shard.entries[key] = data
			}
			shard.mu.Unlock()
			shard.mu.RLock()
			continue
		}

		data.mu.Lock()
		defer data.mu.Unlock()
		if time.Since(data.ts) > c.ttl {
			value, err := c.loader(key)
			if err != nil {
				shard.mu.RUnlock()
				return nil, err
			}
			data.value = value
			data.ts = time.Now()
			shard.mu.RUnlock()
			return value, nil
		}
		data.ts = time.Now()
		shard.mu.RUnlock()
		return data.value, nil
	}
}
func (c *Cache) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closedMu.Lock()
		defer c.closedMu.Unlock()
		c.closed = true
	})
	return err
}

func (c *Cache) getShardIdx(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % 16)
}
