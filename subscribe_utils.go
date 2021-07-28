package pubsubetcd

import (
	"log"
)

func Unsubscribe(subs []Subscription) {
	for _, sub := range subs {
		log.Printf("[INFO] - Unsubscribing to %v:%v\n", sub.ConsumerName, sub.Partition)
		sub.Unsubscribe()
	}
}

func WatchSubscription(subs []Subscription, fn func(Subscription, Message)) {
	for _, subscription := range subs {
		go func(subscription Subscription) {
			for {
				select {
				case <-subscription.Shutdown:
					return
				case msg := <-subscription.Messages:
					fn(subscription, msg)
				}
			}
		}(subscription)
	}
}
