package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrWriterClosed    = errors.New("writer already closed, cannot accept new events")
	ErrWriterQueueFull = errors.New("writer queue is full, cannot accept new events")
)

type Event interface {
	GetID() string
}

type BatchWriter struct {
	events        []Event
	eventsMtx     sync.Mutex
	batchSize     int
	flushInterval time.Duration
	writeFn       func([]Event) error
	incoming      chan Event
	flush         chan []Event

	flushersWg sync.WaitGroup
	flushWg    sync.WaitGroup

	closed     bool
	closedMtx  sync.RWMutex
	closedOnce sync.Once
	stopTicker chan struct{}
}

func NewBatchWriter(batchSize int, flushInterval time.Duration, writeFn func([]Event) error) *BatchWriter {
	queueSize := 4 * batchSize
	incoming := make(chan Event, queueSize)
	flush := make(chan []Event)
	stopTicker := make(chan struct{})
	bw := &BatchWriter{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		writeFn:       writeFn,
		incoming:      incoming,
		flush:         flush,
		stopTicker:    stopTicker,
	}
	bw.flushersWg.Add(2)
	go bw.process()
	go bw.ticker()
	bw.flushWg.Add(1)
	go bw.flushEvents()
	return bw
}

func (bw *BatchWriter) process() {
	defer bw.flushersWg.Done()
	for eventToProcess := range bw.incoming {
		bw.eventsMtx.Lock()
		bw.events = append(bw.events, eventToProcess)
		if len(bw.events) == bw.batchSize {
			toFlush := []Event{}
			toFlush = append(toFlush, bw.events...)
			bw.flush <- toFlush
			bw.events = []Event{}
		}
		bw.eventsMtx.Unlock()
	}
}

func (bw *BatchWriter) ticker() {
	ticker := time.NewTicker(bw.flushInterval)
	defer bw.flushersWg.Done()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bw.eventsMtx.Lock()
			toFlush := []Event{}
			toFlush = append(toFlush, bw.events...)
			if len(toFlush) == 0 {
				bw.eventsMtx.Unlock()
				continue
			}
			bw.flush <- toFlush
			bw.events = []Event{}
			bw.eventsMtx.Unlock()
		case <-bw.stopTicker:
			fmt.Printf("Stopping Ticker\n")
			return
		}
	}
}

func (bw *BatchWriter) flushEvents() {
	defer bw.flushWg.Done()
	for events := range bw.flush {
		bw.writeFn(events)
	}
}

func (bw *BatchWriter) Submit(event Event) error {
	bw.closedMtx.RLock()
	defer bw.closedMtx.RUnlock()
	if bw.closed {
		return ErrWriterClosed
	}
	select {
	case bw.incoming <- event:
		return nil
	default:
		return ErrWriterQueueFull
	}
}

func (bw *BatchWriter) Close() error {
	var err error
	bw.closedOnce.Do(func() {
		bw.closedMtx.Lock()
		defer bw.closedMtx.Unlock()
		bw.closed = true

		close(bw.stopTicker)
		close(bw.incoming)
		bw.flushersWg.Wait()
		close(bw.flush)
		fmt.Printf("Flush channel closed\n")
		bw.flushWg.Wait()
		fmt.Printf("Flush channel done with all tasks\n")
	})
	return err
}
