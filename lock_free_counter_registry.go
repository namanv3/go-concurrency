package main

import (
	"sync"
	"sync/atomic"
)

type Counter struct {
	name  string
	count atomic.Int64
}

// this is meant to be a read heavy registry
type Registry struct {
	counters *sync.Map
}

func NewRegistry() *Registry {
	return &Registry{
		counters: new(sync.Map),
	}
}

func (r *Registry) Get(name string) *Counter {
	counter, _ := r.counters.LoadOrStore(name, &Counter{name: name})
	return counter.(*Counter)
}

func (c *Counter) Add(delta int64) {
	c.count.Add(delta)
}

func (c *Counter) Value() int64 {
	return c.count.Load()
}
