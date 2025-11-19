package main

import (
	"fmt"
	"sync"
	"time"
)

type Count struct {
	count int64
	ts    time.Time
}

type CounterStoreShard struct {
	counts map[int64]Count
	mutex  sync.Mutex
}

func newShard() *CounterStoreShard {
	return &CounterStoreShard{
		counts: map[int64]Count{},
		mutex:  sync.Mutex{},
	}
}

func (shard *CounterStoreShard) evict(evictionTime time.Duration) {
	toDelete := []int64{}
	for cust, count := range shard.counts {
		if time.Since(count.ts) > evictionTime {
			toDelete = append(toDelete, cust)
		}
	}
	fmt.Printf("Found %d customers to evict out of %d\n", len(toDelete), len(shard.counts))
	for _, cust := range toDelete {
		delete(shard.counts, cust)
	}
}

type CounterStore struct {
	numShards int
	shards    []*CounterStoreShard

	evictionTime       time.Duration
	stopEvictionSignal chan struct{}
	evictionWG         sync.WaitGroup
}

func NewCounterStore(numShards int, evictionTime time.Duration) *CounterStore {
	shards := make([]*CounterStoreShard, numShards)
	for i := range numShards {
		shards[i] = newShard()
	}
	store := &CounterStore{
		numShards:          numShards,
		shards:             shards,
		evictionTime:       evictionTime,
		stopEvictionSignal: make(chan struct{}, 1),
		evictionWG:         sync.WaitGroup{},
	}
	go store.Eviction()
	return store
}

func (s *CounterStore) getShard(customerID int64) *CounterStoreShard {
	idx := customerID % int64(s.numShards)
	return s.shards[idx]
}

func (s *CounterStore) Inc(customerID int64) {
	shard := s.getShard(customerID)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if count, ok := shard.counts[customerID]; !ok {
		shard.counts[customerID] = Count{count: 1, ts: time.Now()}
	} else {
		shard.counts[customerID] = Count{count: count.count + 1, ts: time.Now()}
	}
}

func (s *CounterStore) Snapshot() map[int64]int64 {
	snapshot := map[int64]int64{}
	for _, shard := range s.shards {
		shard.mutex.Lock()
		for key, value := range shard.counts {
			snapshot[key] = value.count
		}
		shard.mutex.Unlock()
	}
	return snapshot
}

func (s *CounterStore) Eviction() {
	s.evictionWG.Add(1)
	evictionTicker := time.NewTicker(s.evictionTime / time.Duration(s.numShards))
	defer evictionTicker.Stop()
	defer s.evictionWG.Done()

	shardToProcess := 0
	for {
		select {
		case <-evictionTicker.C:
			shard := s.shards[shardToProcess]
			shard.mutex.Lock()
			shard.evict(s.evictionTime)
			shard.mutex.Unlock()
			shardToProcess = (shardToProcess + 1) % s.numShards
		case <-s.stopEvictionSignal:
			return
		}
	}
}

func (s *CounterStore) Stop() {
	s.stopEvictionSignal <- struct{}{}
	s.evictionWG.Wait()
}
