package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	const (
		rate              = 50
		burst             = 1749
		numProducers      = 4
		testDuration      = 5 * time.Second
		pauseAfterSubmit  = 10 * time.Millisecond
		chanceOf6SecDelay = 0.000001
		chanceOf2SecDelay = 0.00001
	)
	/*
		num tasks per sec ~ numProducers * tasksPerSecond
		num tasks per sec ~ numProducers * (1 / pauseAfterSubmit)

		for 16, 10ms: num tasks ~ 8000
		for  4, 10ms: num tasks ~ 2000
		for  2, 10ms: num tasks ~ 1000

		num tasks consumed in 1 / rate secs <= burst
		num tasks consimed per sec <= burst * rate
		it can be significantly less though, cuz task performance rate can be way less
	*/
	var executedCount atomic.Int64
	var queuefullCount atomic.Int64

	workerFn := func(task Task) error {
		executedCount.Add(1)
		sleepChance := rand.Float32()
		if sleepChance < float32(chanceOf6SecDelay) {
			fmt.Printf("%f%% chance of seeing a 6 second delay has happened\n", chanceOf6SecDelay*100)
			time.Sleep(6000 * time.Millisecond)
		} else if sleepChance < float32(chanceOf2SecDelay) {
			fmt.Printf("%f%% chance of seeing a 2 second delay has happened\n", chanceOf2SecDelay*100)
			time.Sleep(2000 * time.Millisecond)
		}
		errChance := rand.Float32()
		if errChance < 0.000001 {
			fmt.Printf("0.0001%% chance of seeing an error has happened\n")
			return errors.New("unknown error occurred")
		}
		return nil
	}

	scheduler := NewScheduler(rate, burst, workerFn)

	var producersWg sync.WaitGroup

	producer := func() {
		defer producersWg.Done()
		ticker := time.NewTicker(pauseAfterSubmit)
		defer ticker.Stop()
		for range ticker.C {
			err := scheduler.Submit(Task{})
			if err == ErrSchedulerClosed {
				return
			} else if err == ErrSchedulerQueueFull {
				queuefullCount.Add(1)
				continue
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}

	for range numProducers {
		producersWg.Add(1)
		go producer()
	}

	time.Sleep(testDuration)
	fmt.Printf("Test Duration Over\n")
	scheduler.Close()
	fmt.Printf("Scheduler closed\n")

	producersWg.Wait()
	fmt.Printf("Producers Stopped\n")

	err := scheduler.Submit(Task{})
	if err != ErrSchedulerClosed {
		t.Errorf("Scheduler closed but still able to submit tasks")
	}

	fmt.Printf("%d tasks executed\n", executedCount.Load())
	fmt.Printf("%d tasks rejected due to queue being full\n", queuefullCount.Load())
}
