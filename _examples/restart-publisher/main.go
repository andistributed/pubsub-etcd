package main

import (
	"fmt"
	"log"
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
	// Create topic
	top, err := pubsubetcd.CreateAndGetTopic(cli, tn, 1)
	if err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}
	go func() {
		var i int
		for {
			time.Sleep(1 * time.Second)
			fmt.Println(`put:`, i)
			top.Put(fmt.Sprintf("%d", i))
			i++
		}
	}()

	// Wait here until user exists.
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan
}
