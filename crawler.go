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
	doneSignal chan struct{}
}

func NewCrawler() *Crawler {
	return &Crawler{
		incoming:   make(chan string, CRAWLER_CAPACITY),
		stopSignal: make(chan struct{}, 1),
		doneSignal: make(chan struct{}, CRAWLER_CAPACITY),
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
				for url := range c.incoming {
					c.crawl(url)
				}
				return
			}
		}
	}()
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
		close(c.incoming)
		c.wg.Wait()
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
	c.doneSignal <- struct{}{}
}

type CrawlerManager struct {
	crawlerPool chan *Crawler
	numCrawlers int

	queue *Queue
}

func NewCrawlerManager(q *Queue) *CrawlerManager {
	crawlerPool := make(chan *Crawler, 100000)
	for range DEFAULT_COUNT_CRAWLERS {
		newCrawler := NewCrawler()
		newCrawler.start()
		crawlerPool <- newCrawler
	}
	return &CrawlerManager{
		numCrawlers: DEFAULT_COUNT_CRAWLERS,
		crawlerPool: crawlerPool,
		queue:       q,
	}
}

func (cm *CrawlerManager) start() {
	go func() {
		for {
			url := cm.queue.next()
			for {
				crawler := <-cm.crawlerPool
				err := crawler.consume(url)
				if err == ErrCrawlerStopped {
					continue
				} else if err == ErrCrawlerFull {
					go func() {
						<-crawler.doneSignal
						cm.crawlerPool <- crawler
					}()
				} else {
					cm.crawlerPool <- crawler
					break
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		for range ticker.C {
			currSize := cm.queue.size()
			if currSize > 20 {
				fmt.Printf("Current size of queue is %d, increasing number of crawlers (currently %d) by 1\n", currSize, cm.numCrawlers)
				newCrawler := NewCrawler()
				newCrawler.start()
				cm.crawlerPool <- newCrawler
				cm.numCrawlers++
			} else {
				if cm.numCrawlers == MIN_CRAWLERS {
					fmt.Printf("Current size of queue is %d, Number of crawlers (currently %d) is at its minimum\n", currSize, cm.numCrawlers)
					continue
				}
				fmt.Printf("Current size of queue is %d, decreasing number of crawlers (currently %d) by 1\n", currSize, cm.numCrawlers)
				lastCrawler := <-cm.crawlerPool
				cm.numCrawlers--
				go lastCrawler.stop()
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
