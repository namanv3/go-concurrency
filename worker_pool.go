package main

import (
	"fmt"
	"sync"
)

type WorkerPool struct {
	tasks      chan func()
	backlog    chan func()
	numWorkers int
	maxTasks   int
	closed     bool
	closedMtx  sync.RWMutex
	closeOnce  sync.Once
	tasksWg    *sync.WaitGroup
}

func NewWorkerPool(numWorkers, maxTasks int) *WorkerPool {
	tasks := make(chan func())
	backlog := make(chan func(), maxTasks)
	var tasksWg sync.WaitGroup
	for range numWorkers {
		tasksWg.Add(1)
		go func() {
			defer tasksWg.Done()
			for task := range tasks {
				func() {
					defer func() {
						if r := recover(); r != nil {
							fmt.Printf("worker task panic recovered: %v\n", r)
						}
					}()
					task()
				}()
			}
		}()
	}
	go func() {
		for backlogTask := range backlog {
			tasks <- backlogTask
		}
		fmt.Printf("Closing Tasks\n")
		close(tasks)
	}()
	return &WorkerPool{
		tasks:      tasks,
		backlog:    backlog,
		numWorkers: numWorkers,
		maxTasks:   maxTasks,
		closed:     false,
		tasksWg:    &tasksWg,
	}
}

func (p *WorkerPool) Submit(task func()) error {
	p.closedMtx.RLock()
	defer p.closedMtx.RUnlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case p.backlog <- task:
		return nil
	default:
		return ErrPoolQueueFull
	}
}

func (p *WorkerPool) Close() {
	p.closeOnce.Do(func() {
		p.closedMtx.Lock()
		fmt.Printf("Closing Pool\n")
		p.closed = true
		fmt.Printf("Closing Backlog\n")
		close(p.backlog)
		p.closedMtx.Unlock()
	})
	fmt.Printf("Waiting for Tasks to finish\n")
	p.tasksWg.Wait()
}
