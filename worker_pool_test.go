package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	startG := runtime.NumGoroutine()

	const (
		pauseAfterProducingTask = 1 * time.Microsecond
		numWorkers              = 32
		maxTasks                = 100000
		testDuration            = 5 * time.Second
	)

	tasks := []func(){PrimeSieve, Mandelbrot, MatrixMultiply, HashStorm, JSONShuffle, RandomWalk, SortLarge}
	// tasks := []func(){func() {}, func() {}, func() {}, func() {}, func() {}, func() {}, func() {}}

	workerPool := NewWorkerPool(numWorkers, maxTasks)

	var executedCount atomic.Int64
	var queuefullCount atomic.Int64

	stopProducer := make(chan struct{})
	var producerWg sync.WaitGroup
	fmt.Printf("Starting Producer Thread\n")
	producerWg.Add(1)
	go func() {
		ticker := time.NewTicker(pauseAfterProducingTask)
		defer ticker.Stop()
		defer producerWg.Done()
		for {
			select {
			case <-ticker.C:
				taskIdx := rand.Intn(7)
				wrappedTask := func() {
					tasks[taskIdx]()
					executedCount.Add(1)
					// n := executedCount.Add(1)
					// fmt.Printf("%d tasks executed\n", n)
				}
				err := workerPool.Submit(wrappedTask)
				if err == ErrPoolQueueFull {
					queuefullCount.Add(1)
					// n := queuefullCount.Add(1)
					// fmt.Printf("%d tasks blocked\n", n)
				}
			case <-stopProducer:
				fmt.Printf("Closing Producer Thread\n")
				return
			}
		}
	}()

	deadline := time.After(testDuration)
	closed := make(chan struct{})
	go func() {
		<-deadline
		fmt.Printf("Deadline passed. Sending signal to stop Producer\n")
		stopProducer <- struct{}{}
		close(stopProducer)
		producerWg.Wait()
		workerPool.Close()
		closed <- struct{}{}
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		t.Fatalf("Waited too long for closure")
	}

	err := workerPool.Submit(tasks[1])
	if err != ErrPoolClosed {
		t.Fatalf("Worker Pool Accepted Task even after it was closed")
	}

	fmt.Printf("%d tasks executed\n", executedCount.Load())
	fmt.Printf("%d tasks rejected due to queue being full\n", queuefullCount.Load())
	if executedCount.Load() < 100 {
		t.Fatalf("too few tasks executed: %d", executedCount.Load())
	}
	endG := runtime.NumGoroutine()

	if endG-startG > numWorkers+20 {
		t.Fatalf("suspected goroutine leak: started with %d, ended with %d\n", startG, endG)
	}
}
