// web crawler
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TIME_PERIOD_PRODUCER   = 100 * time.Millisecond
	CRAWLER_CAPACITY       = 2
	SIZE_QUEUE             = 1024
	DEFAULT_COUNT_CRAWLERS = 2
	MIN_CRAWLERS           = 1
)

var (
	ErrCrawlerStopped = errors.New("crawler has stopped")
	ErrCrawlerFull    = errors.New("crawler is full")
)

type Producer struct {
	next       int
	push       func(string)
	timePeriod time.Duration
}

func NewProducer(timePeriod time.Duration, push func(string)) *Producer {
	return &Producer{
		timePeriod: timePeriod,
		push:       push,
	}
}

func (p *Producer) start() {
	go func() {
		ticker := time.NewTicker(p.timePeriod)
		for range ticker.C {
			nextURL := p.getNext()
			p.push(nextURL)
		}
	}()
}

func (p *Producer) getNext() string {
	nextURL := fmt.Sprintf("http://google.com/%d", p.next)
	p.next++
	return nextURL
}

type Crawler struct {
	incoming chan string

	wg sync.WaitGroup

	stopOnce   sync.Once
	stopped    atomic.Bool // check if a lock is a better idea here
	stopSignal chan struct{}
}

func NewCrawler() *Crawler {
	return &Crawler{
		incoming:   make(chan string, CRAWLER_CAPACITY),
		stopSignal: make(chan struct{}),
	}
}

func (c *Crawler) start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case url, ok := <-c.incoming:
				if !ok {
					return
				}
				c.crawl(url)
			case <-c.stopSignal:
				return
			}
		}
	}()
}

func (c *Crawler) isFree() bool {
	return len(c.incoming) < cap(c.incoming)
}

func (c *Crawler) consume(url string) error {
	isStopped := c.stopped.Load()
	if isStopped {
		return ErrCrawlerStopped
	}
	select {
	case c.incoming <- url:
		return nil
	default:
		return ErrCrawlerFull
	}
}

func (c *Crawler) stop() {
	c.stopOnce.Do(func() {
		c.stopped.Store(true)
		c.stopSignal <- struct{}{}
		c.wg.Wait()
		close(c.incoming)
	})
}

func (c *Crawler) crawl(url string) {
	chanceLowSpeed := rand.Float32()
	if chanceLowSpeed < 0.3 {
		fmt.Printf("%s: Slow!\n", url)
		time.Sleep(2 * time.Second)
	} else {
		fmt.Printf("%s: Usual\n", url)
		time.Sleep(1 * time.Second)
	}

}

type CrawlerManager struct {
	lock     sync.Mutex
	crawlers []*Crawler

	queue *Queue
}

func NewCrawlerManager(q *Queue) *CrawlerManager {
	crawlers := make([]*Crawler, DEFAULT_COUNT_CRAWLERS)
	for i := range DEFAULT_COUNT_CRAWLERS {
		crawlers[i] = NewCrawler()
	}
	return &CrawlerManager{
		crawlers: crawlers,
		queue:    q,
	}
}

func (cm *CrawlerManager) start() {
	sendToCrawler := func(crawler *Crawler, url string) {
		err := crawler.consume(url)
		if err == ErrCrawlerStopped {
			return
		}
	}
	for i := range DEFAULT_COUNT_CRAWLERS {
		cm.crawlers[i].start()
	}

	go func() {
		for {
			url := cm.queue.next()
			for {
				cm.lock.Lock()
				found := false
				for _, crawler := range cm.crawlers {
					if crawler.isFree() {
						found = true
						sendToCrawler(crawler, url)
						break
					}
				}
				cm.lock.Unlock()
				if found {
					break
				}
			}
		}
	}()

	go func() {
		count := DEFAULT_COUNT_CRAWLERS
		ticker := time.NewTicker(1000 * time.Millisecond)
		for range ticker.C {
			currSize := cm.queue.size()
			if currSize > 20 {
				fmt.Printf("Current size of queue is %d, increasing number of crawlers (currently %d) by 1\n", currSize, len(cm.crawlers))
				newCrawler := NewCrawler()
				cm.lock.Lock()
				cm.crawlers = append(cm.crawlers, newCrawler)
				count++
				newCrawler.start()
				cm.lock.Unlock()
			} else {
				if len(cm.crawlers) == MIN_CRAWLERS {
					fmt.Printf("Current size of queue is %d, Number of crawlers (currently %d) is at its minimum\n", currSize, len(cm.crawlers))
					continue
				}
				fmt.Printf("Current size of queue is %d, decreasing number of crawlers (currently %d) by 1\n", currSize, len(cm.crawlers))
				cm.lock.Lock()
				lastCrawler := cm.crawlers[len(cm.crawlers)-1]
				cm.crawlers = cm.crawlers[:len(cm.crawlers)-1]
				lastCrawler.stop()
				cm.lock.Unlock()
			}
		}
	}()

}

type Queue struct {
	queue chan string
}

func NewQueue() *Queue {
	return &Queue{
		queue: make(chan string, SIZE_QUEUE),
	}
}

func (q *Queue) size() int {
	return len(q.queue)
}

func (q *Queue) push(url string) {
	q.queue <- url
}

func (q *Queue) next() string {
	return <-q.queue
}

func main() {
	queue := NewQueue()
	producer := NewProducer(TIME_PERIOD_PRODUCER, queue.push)
	producer.start()
	manager := NewCrawlerManager(queue)
	manager.start()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
