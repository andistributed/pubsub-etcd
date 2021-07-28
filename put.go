package pubsubetcd

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (t *Topic) Put(data string) (string, string) {
	succeeds, faileds := t.PutBatch([]string{data})
	var successResult, failureResult string
	if len(succeeds) > 0 {
		successResult = succeeds[0]
	}
	if len(faileds) > 0 {
		failureResult = faileds[0]
	}
	return successResult, failureResult
}

func (t *Topic) ScramblePartitions() []int {
	o := []int{}
	for i := 0; i < t.Partitions; i++ {
		o = append(o, i)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(o), func(i, j int) { o[i], o[j] = o[j], o[i] })
	return o
}

// PutBatch sends values to a given topic. Returns the successful and failed requests.
func (t *Topic) PutBatch(data []string) ([]string, []string) {

	// Scramble partition order on each batch to distribute data across all available partitions.
	o := t.ScramblePartitions()

	success := make(chan []string)
	failed := make(chan []string)

	tx := t.etcd.Txn(context.TODO()).If()
	ops := []clientv3.Op{}
	ds := []string{}
	sends := 0
	for i, d := range data {

		partition := o[i%t.Partitions]
		path := fmt.Sprintf("%v/partition=%v/events", t.GetName().String(), partition)
		ops = append(ops, clientv3.OpPut(path, d))
		ds = append(ds, d)

		if (i+1)%t.Partitions == 0 || i+1 == len(data) {
			sends++
			tx.Then(clientv3.OpTxn(nil, ops, nil))
			go func(tx clientv3.Txn, ds []string) {
				_, err := tx.Commit()
				if err != nil {
					failed <- ds
				} else {
					success <- ds
				}
			}(tx, ds)
			tx = t.etcd.Txn(context.TODO()).If()
			ops = []clientv3.Op{}
			ds = []string{}
		}
	}

	succeeds := []string{}
	faileds := []string{}
	for range make([]struct{}, sends) {
		select {
		case msg := <-success:
			succeeds = append(succeeds, msg...)
		case msg := <-failed:
			faileds = append(faileds, msg...)
		}
	}

	return succeeds, faileds
}
