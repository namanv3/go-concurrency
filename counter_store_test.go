package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCounterStore(t *testing.T) {
	const (
		NUM_OPS       = 1_000_000
		NUM_CUSTOMERS = 8192
		SNAPSHOT_RATE = 0.02
	)

	var customerIDs = func() []int64 {
		out := make([]int64, NUM_CUSTOMERS)
		rng := rand.New(rand.NewSource(1))
		for i := range NUM_CUSTOMERS {
			v := rng.Int63()
			if v == 0 {
				v = 1
			}
			out[i] = v
		}
		return out
	}()

	numShardsList := []int{1, 2, 4, 8, 16, 32, 64}
	numWorkersList := []int{1, 10, 100}

	for _, numShards := range numShardsList {
		for _, numWorkers := range numWorkersList {
			store := NewCounterStore(numShards, 1*time.Hour)
			var wg sync.WaitGroup
			numTasksPerWorker := NUM_OPS / numWorkers
			start := time.Now()
			for w := range numWorkers {
				// Give each worker a unique, deterministic RNG seed.
				workerRNG := rand.New(rand.NewSource(int64(w) + 99))
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range numTasksPerWorker {
						if workerRNG.Float64() < SNAPSHOT_RATE {
							_ = store.Snapshot()
						} else {
							store.Inc(customerIDs[workerRNG.Intn(NUM_CUSTOMERS)])
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			snap := store.Snapshot()
			var total int64
			for _, v := range snap {
				total += v
			}
			store.Stop()
			fmt.Printf("Shards=%2d Workers=%3d Total=%6d Time=%v\n", numShards, numWorkers, total, elapsed)
		}
		println()
	}
}

func TestEvictionLeak(t *testing.T) {
	const (
		numShards      = 8
		numWorkers     = 32
		numCustomers   = 1_000_000 // large, forces churn
		memPrintEvery  = 11 * time.Second
		evictionTime   = 30 * time.Second
		workerWaitTime = 1 * time.Millisecond
	)

	var customerIDs = func() []int64 {
		out := make([]int64, numCustomers)
		rng := rand.New(rand.NewSource(1))
		for i := range numCustomers {
			v := rng.Int63()
			if v == 0 {
				v = 1
			}
			out[i] = v
		}
		return out
	}()

	store := NewCounterStore(numShards, evictionTime)

	var testWait sync.WaitGroup
	testWait.Add(1)

	for range numWorkers {
		go func() {
			for {
				k := customerIDs[rand.Intn(numCustomers)]
				store.Inc(k)
				time.Sleep(workerWaitTime)
			}
		}()
	}

	go func() {
		defer testWait.Done()
		var m runtime.MemStats
		for {
			time.Sleep(memPrintEvery)
			runtime.ReadMemStats(&m)
			fmt.Printf("HeapAlloc = %.2fMB  NumGC=%d  Sys=%.2fMB\n",
				float64(m.HeapAlloc)/1024/1024,
				m.NumGC,
				float64(m.Sys)/1024/1024,
			)
		}
	}()

	testWait.Wait()
}
