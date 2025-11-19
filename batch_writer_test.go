package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type DummyEvent struct {
	id string
}

func NewEvent() DummyEvent {
	return DummyEvent{
		id: uuid.New().String(),
	}
}

func (e DummyEvent) GetID() string {
	return e.id
}

func TestBatchWriter(t *testing.T) {
	startG := runtime.NumGoroutine()

	const (
		batchSize        = 8192
		flushInterval    = 90 * time.Millisecond
		pauseAfterSubmit = 50 * time.Millisecond
		numProducers     = 1024
		deadline         = 60 * time.Second
		waitTime         = 66 * time.Second
	)

	var (
		maxBatch            atomic.Int64
		minBatch            atomic.Int64
		emptyBatches        atomic.Int64
		singularBatches     atomic.Int64
		intervalViolations  atomic.Int64
		flushCount          atomic.Int64
		flushedEventsCount  atomic.Int64
		queueFullRejections atomic.Int64
		doubleFlushes       atomic.Int64
	)

	minBatch.Store(1 << 62) // init to huge
	eventIDsFlushed := map[string]bool{}

	writeFunc := func(events []Event) error {
		// --- 1. Validate batch size ---
		batchLen := int64(len(events))
		if batchLen == 0 {
			emptyBatches.Add(1)
			fmt.Printf("EMPTY BATCH FLUSHED — bug in batch writer\n")
		}

		if batchLen == 1 {
			singularBatches.Add(1)
		}

		// update min/max batch sizes
		if batchLen > maxBatch.Load() {
			maxBatch.Store(batchLen)
		}
		if batchLen < minBatch.Load() {
			minBatch.Store(batchLen)
		}

		flushCount.Add(1)
		flushedEventsCount.Add(int64(len(events)))

		// --- 2. Simulate realistic work ---
		// Simulate CPU/GCS-rich batch write, not just sleep()
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Microsecond)

		// Do a little fake CPU load to emulate encoding/compressing
		for i := 0; i < 2000; i++ {
			_ = i * i
		}

		// sleep as well
		sleepChance := rand.Float32()
		if sleepChance < 0.0001 {
			fmt.Printf("0.01%% chance of seeing a 6 second delay has happened\n")
			time.Sleep(6000 * time.Millisecond)
		} else if sleepChance < 0.008 {
			fmt.Printf("0.8%% chance of seeing a 2s second delay has happened\n")
			time.Sleep(2000 * time.Millisecond)
		} else if sleepChance < 0.01 {
			fmt.Printf("1%% chance of seeing a 300ms second delay has happened\n")
			time.Sleep(300 * time.Millisecond)
		} else {
			time.Sleep(3 * time.Millisecond)
		}

		// --- 3. Check for double flushes ---
		found := false
		for _, event := range events {
			if _, ok := eventIDsFlushed[event.GetID()]; ok {
				found = true
				doubleFlushes.Add(1)
			}
			eventIDsFlushed[event.GetID()] = true
		}
		if found {
			fmt.Printf("Flush has some events flushed again - bug in batch writer\n")
		}

		return nil
	}

	batchWriter := NewBatchWriter(batchSize, flushInterval, writeFunc)

	stopProducers := make(chan struct{})
	var producersWg sync.WaitGroup
	for range numProducers {
		producersWg.Add(1)
		go func() {
			defer producersWg.Done()
			ticker := time.NewTicker(pauseAfterSubmit)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					err := batchWriter.Submit(NewEvent())
					if err == ErrWriterQueueFull {
						queueFullRejections.Add(1)
					}
				case <-stopProducers:
					return
				}
			}
		}()
	}

	deadlineTimer := time.After(deadline)
	closed := make(chan struct{})
	go func() {
		<-deadlineTimer
		fmt.Printf("Deadline passed. Sending signal to stop Producers\n")
		for range numProducers {
			stopProducers <- struct{}{}
		}
		producersWg.Wait()
		fmt.Printf("Producers stopped\n")
		close(stopProducers)
		batchWriter.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(waitTime):
		t.Fatalf("Waited too long for closure")
	}

	err := batchWriter.Submit(DummyEvent{})
	if err != ErrWriterClosed {
		t.Fatalf("Worker Pool Accepted Task even after it was closed")
	}

	endG := runtime.NumGoroutine()
	fmt.Printf("Goroutines: started with %d, ended with %d\n", startG, endG)

	fmt.Printf("Flushes: %d\n", flushCount.Load())
	fmt.Printf("Events flushed: %d\n", flushedEventsCount.Load())
	fmt.Printf("Events rejected due to queue being full: %d\n", queueFullRejections.Load())
	fmt.Printf("Min batch size: %d\n", minBatch.Load())
	fmt.Printf("Max batch size: %d\n", maxBatch.Load())
	fmt.Printf("Empty batches: %d\n", emptyBatches.Load())
	fmt.Printf("Singular batches: %d\n", singularBatches.Load())
	fmt.Printf("Interval violations: %d\n", intervalViolations.Load())
	fmt.Printf("Double flushed events: %d\n", doubleFlushes.Load())
}
