package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type DummyValue struct {
	key   string
	value string
}

func TestCache(t *testing.T) {
	numKeys := 256
	numProducers := 1024
	ttl := 50 * time.Millisecond
	testDuration := 5 * time.Second
	waitAfterGet := 10 * time.Microsecond
	varianceFactor := int64(2)
	chanceOf6SecDelay := 0.01

	type counter struct {
		loads atomic.Int64
		// add other test params later
	}
	perKey := make(map[string]*counter)
	keys := make([]string, 0, numKeys)
	for range numKeys {
		k := uuid.NewString()
		keys = append(keys, k)
		perKey[k] = &counter{}
	}

	loader := func(key string) (value any, err error) {
		sleepChance := rand.Float32()
		if sleepChance < float32(chanceOf6SecDelay) {
			fmt.Printf("%f%% chance of seeing a 6 second delay has happened\n", chanceOf6SecDelay*100)
			time.Sleep(6000 * time.Millisecond)
		}
		errChance := rand.Float32()
		if errChance < 0.000001 {
			fmt.Printf("0.0001%% chance of seeing an error has happened\n")
			return nil, errors.New("unknown error occurred")
		}
		perKey[key].loads.Add(1)
		return DummyValue{key: key, value: uuid.New().String()}, nil
	}

	cache := NewCache(loader, ttl)

	var producersWg sync.WaitGroup

	producer := func() {
		defer producersWg.Done()
		ticker := time.NewTicker(waitAfterGet)
		defer ticker.Stop()
		for range ticker.C {
			k := keys[rand.Intn(numKeys)]
			v, err := cache.Get(k)
			if err == ErrCacheClosed {
				return
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if v == nil {
				t.Errorf("nil value returned")
			}
		}
	}

	for range numProducers {
		producersWg.Add(1)
		go producer()
	}

	time.Sleep(testDuration)
	fmt.Printf("Test Duration Over\n")
	cache.Close()
	fmt.Printf("Cache closed\n")

	producersWg.Wait()
	fmt.Printf("Producers Stopped\n")

	_, err := cache.Get(keys[0])
	if err != ErrCacheClosed {
		t.Errorf("Cache closed but still able to fetch elements from the cache")
	}

	for k, c := range perKey {
		loads := c.loads.Load()
		if loads == 0 {
			t.Errorf("key %s was never loaded (bad test or coalescing error)", k)
		}
	}

	avgHitInterval := time.Duration(numKeys * int(waitAfterGet) / numProducers)
	expectedLoadsPerKey := int64(testDuration / avgHitInterval)
	allowedVariance := expectedLoadsPerKey * varianceFactor // wide margin
	for k, c := range perKey {
		loads := c.loads.Load()
		if loads > allowedVariance {
			t.Errorf("key %s loaded too many times: %d expected ~%d",
				k, loads, expectedLoadsPerKey)
		}
	}

	totalLoads := int64(0)
	for _, c := range perKey {
		totalLoads += c.loads.Load()
	}
	fmt.Printf("stress test complete: %d total loads across %d keys", totalLoads, numKeys)
}

func TestCacheInitialLoadingCount(t *testing.T) {
	ttl := 200 * time.Millisecond
	numProducers := 4096
	numKeys := 32
	counts := sync.Map{}

	simpleLoader := func(key string) (value any, err error) {
		v, _ := counts.LoadOrStore(key, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
		return DummyValue{key: key, value: uuid.New().String()}, nil
	}

	keys := []string{}
	for range numKeys {
		keys = append(keys, uuid.NewString())
	}

	cache := NewCache(simpleLoader, ttl)

	var producersWg sync.WaitGroup
	for range numProducers {
		producersWg.Add(1)
		go func() {
			defer producersWg.Done()
			idx := rand.Intn(numKeys)
			_, _ = cache.Get(keys[idx])
		}()
	}
	producersWg.Wait()
	counts.Range(func(key, val any) bool {
		if val.(*atomic.Int64).Load() > 1 {
			t.Fatalf("Key %s was loaded %d times", key, val.(*atomic.Int64).Load())
		}
		return true
	})
}
