package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrSchedulerClosed    = errors.New("scheduler already closed, cannot accept new tasks")
	ErrSchedulerQueueFull = errors.New("scheduler queue is full, cannot accept new tasks")
)

type Task struct {
	ID   string
	Data any
}

type Scheduler struct {
	rate     int
	workerFn func(Task) error

	tasks   chan Task
	tasksWg sync.WaitGroup

	closed    bool
	closedMu  sync.RWMutex
	closeOnce sync.Once
	stop      chan struct{}
	forceStop chan struct{}
}

func NewScheduler(rate, burst int, workerFn func(Task) error) *Scheduler {
	tasks := make(chan Task, burst)
	stop := make(chan struct{})
	forceStop := make(chan struct{})
	scheduler := &Scheduler{
		rate:      rate,
		workerFn:  workerFn,
		tasks:     tasks,
		stop:      stop,
		forceStop: forceStop,
	}

	scheduler.tasksWg.Add(1)
	go scheduler.worker()

	return scheduler
}

func (s *Scheduler) worker() {
	defer s.tasksWg.Done()
	period := (1 * time.Second) / time.Duration(s.rate) // time period = 1 / frequency
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case task, ok := <-s.tasks:
				if !ok {
					return
				}
				err := s.workerFn(task)
				if err != nil {
					fmt.Printf("Encountered error in task %s: %s\n", task.ID, err.Error())
				}
			default:
				continue
			}
		case <-s.stop:
			for task := range s.tasks {
				err := s.workerFn(task)
				if err != nil {
					fmt.Printf("Encountered error in task %s: %s\n", task.ID, err.Error())
				}
			}
			return
		case <-s.forceStop:
			return
		}
	}
}

func (s *Scheduler) Submit(t Task) error {
	s.closedMu.RLock()
	defer s.closedMu.RUnlock()
	if s.closed {
		return ErrSchedulerClosed
	}
	select {
	case s.tasks <- t:
		return nil
	default:
		return ErrSchedulerQueueFull
	}
}

func (s *Scheduler) Close() error {
	s.closeOnce.Do(func() {
		s.closedMu.Lock()
		s.closed = true
		s.closedMu.Unlock()
		close(s.stop)
		fmt.Printf("Stop signal sent, waiting for remaining tasks to finish\n")
		close(s.tasks)
		s.tasksWg.Wait()
		fmt.Printf("Remaining tasks finished\n")
		close(s.forceStop)
	})
	return nil
}

func (s *Scheduler) ForceClose() error {
	s.closeOnce.Do(func() {
		s.closedMu.Lock()
		s.closed = true
		s.closedMu.Unlock()
		close(s.forceStop)
		close(s.tasks)
		s.tasksWg.Wait()
		close(s.stop)
	})
	return nil
}
