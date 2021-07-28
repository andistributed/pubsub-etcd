package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	pubsubetcd "github.com/andistributed/pubsub-etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {

	endpoint := []string{}
	if len(os.Getenv("ETCD_ADVERTISE_CLIENT_URLS")) != 0 {
		endpoint = append(endpoint, os.Getenv("ETCD_ADVERTISE_CLIENT_URLS"))
	} else {
		endpoint = append(endpoint, "localhost:2379")
	}
	// Connect to running etcd instance(s).
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	defer cli.Close()

	tn := `test-subscribe`
	mt, err := pubsubetcd.GetTopic(cli, tn)
	if err != nil {
		panic(err)
	}
	consumerName := "restart-consumer"

	// Subscribe to all available partitions on the topic, and print a sample.
	subs, err := mt.Subscribe(consumerName)
	if err != nil {
		panic(err)
	}
	pubsubetcd.WatchSubscription(subs, func(subscription pubsubetcd.Subscription, msg pubsubetcd.Message) {
		fmt.Println(`received:`, msg.Value)
	})

	// Wait here until user exists.
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan

	pubsubetcd.Unsubscribe(subs)
}
