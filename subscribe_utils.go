package pubsubetcd

func Unsubscribe(subs []Subscription, deletes ...bool) {
	var delete bool
	if len(deletes) > 0 {
		delete = deletes[0]
	}
	for _, sub := range subs {
		sub.Unsubscribe()
		if !delete {
			continue
		}
		sub.Delete()
	}
}

func DeleteSubscription(subs []Subscription) {
	for _, sub := range subs {
		sub.Delete()
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
