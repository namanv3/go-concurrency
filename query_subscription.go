package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	NUM_SHARDS                    = 16
	SUBSCRIPTION_CHANNELS_BACKLOG = 4
	UPDATE_INTERVAL               = 1000 * time.Millisecond
	QUERY_TIME_LIMIT              = 100 * time.Millisecond
	CHANCE_6_SEC_DELAY            = 0.0001
	CHANCE_OF_NO_UPDATE           = 0.3
)

type Result struct {
	query string
}

type Subscription struct {
	channel chan Result
	id      string
}

type SubscriptionShard struct {
	subscriptions map[string]map[string]Subscription
	mu            sync.Mutex
}

func NewSubscriptionShard() *SubscriptionShard {
	subscriptions := make(map[string]map[string]Subscription)
	return &SubscriptionShard{
		subscriptions: subscriptions,
	}
}

type QuerySubscription struct {
	shards [NUM_SHARDS]*SubscriptionShard

	updateEvery time.Duration
}

func NewQuerySubscription() *QuerySubscription {
	shards := [NUM_SHARDS]*SubscriptionShard{}
	for i := range NUM_SHARDS {
		shards[i] = NewSubscriptionShard()
	}
	qs := &QuerySubscription{
		shards: shards,

		updateEvery: UPDATE_INTERVAL,
	}
	go qs.checkAndSendUpdates()
	return qs
}

func (qs *QuerySubscription) Subscribe(query string) (<-chan Result, func()) {
	idx := qs.getShardIdx(query)
	shard := qs.shards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	newSubscription := Subscription{
		channel: make(chan Result, SUBSCRIPTION_CHANNELS_BACKLOG),
		id:      uuid.NewString(),
	}
	if _, ok := shard.subscriptions[query]; ok {
		shard.subscriptions[query][newSubscription.id] = newSubscription
	} else {
		shard.subscriptions[query] = map[string]Subscription{newSubscription.id: newSubscription}
	}
	return newSubscription.channel, func() { qs.unsubscribe(query, newSubscription.id) }
}

func (qs *QuerySubscription) unsubscribe(query, id string) {
	idx := qs.getShardIdx(query)
	shard := qs.shards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, ok := shard.subscriptions[query]; !ok {
		return
	} else if _, ok := shard.subscriptions[query][id]; !ok {
		return
	}
	close(shard.subscriptions[query][id].channel)
	delete(shard.subscriptions[query], id)
	if len(shard.subscriptions[query]) == 0 {
		delete(shard.subscriptions, query)
	}
}

func (qs *QuerySubscription) getShardIdx(query string) int {
	h := fnv.New32a()
	h.Write([]byte(query))
	return int(h.Sum32() % NUM_SHARDS)
}

func (qs *QuerySubscription) checkAndSendUpdates() {
	ticker := time.NewTicker(qs.updateEvery)
	for range ticker.C {
		var wg sync.WaitGroup
		for i := range NUM_SHARDS {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				shard := qs.shards[idx]
				shard.mu.Lock()

				localCopy := make(map[string][]chan Result)
				for query, subs := range shard.subscriptions {
					list := []chan Result{}
					for _, sub := range subs {
						list = append(list, sub.channel)
					}
					localCopy[query] = list
				}

				shard.mu.Unlock()

				for query, subscriptionChannels := range localCopy {
					result := runQueryWithTimeout(query)
					if result == nil {
						continue
					}
					for _, channel := range subscriptionChannels {
						select {
						case channel <- *result:
						default:
							// too slow
						}
					}
				}
			}(i)
		}
		wg.Wait()
	}
}

func runQueryWithTimeout(query string) *Result {
	ch := make(chan *Result, 1)
	go func() {
		ch <- runQuery(query)
	}()

	select {
	case result := <-ch:
		return result
	case <-time.After(QUERY_TIME_LIMIT):
		return nil
	}
}

func runQuery(query string) *Result {
	noUpdateChance := rand.Float32()
	if noUpdateChance < float32(CHANCE_OF_NO_UPDATE) {
		return nil
	}
	sleepChance := rand.Float32()
	if sleepChance < float32(CHANCE_6_SEC_DELAY) {
		fmt.Printf("%f%% chance of seeing a 6 second delay has happened\n", CHANCE_6_SEC_DELAY*100)
		time.Sleep(6000 * time.Millisecond)
	} else {
		time.Sleep(30 * time.Millisecond)
	}
	return &Result{
		query: query,
	}
}
