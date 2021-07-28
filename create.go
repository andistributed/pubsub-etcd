package pubsubetcd

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrInvalidTopicName = errors.New(`invalid topic name, must match regex [A-z0-9\-_]{3,}`)
var ErrPartitionsAtLeastOne = errors.New(`partitions must be at least 1`)
var ErrTopicAlreadyExists = errors.New(`topic already exists`)

func CreateAndGetTopic(etcd *clientv3.Client, name string, partitions int) (Topic, error) {
	top, err := CreateTopic(etcd, name, partitions)
	if err == nil {
		return top, nil
	}
	if !errors.Is(err, ErrTopicAlreadyExists) {
		return top, err
	}
	top.etcd = etcd
	err = top.GetTopicMetadata("/topic/" + name)
	return top, err
}

func GetTopic(etcd *clientv3.Client, name string) (Topic, error) {
	n := TopicName(name)
	if !n.IsValid() {
		return Topic{}, ErrInvalidTopicName
	}
	top := Topic{}
	top.etcd = etcd
	err := top.GetTopicMetadata("/topic/" + n.String())
	if err != nil {
		return Topic{}, err
	}
	return top, nil
}

func CreateTopic(etcd *clientv3.Client, name string, partitions int) (Topic, error) {
	n := TopicName(name)
	if !n.IsValid() {
		return Topic{}, ErrInvalidTopicName
	}

	if partitions < 0 {
		return Topic{}, ErrPartitionsAtLeastOne
	}

	tn := "/topic/" + n.String()
	re, err := etcd.Get(context.TODO(), tn+"/taken")
	if err != nil {
		return Topic{}, err
	}

	if len(re.Kvs) != 0 {
		return Topic{}, fmt.Errorf("%s: %w", name, ErrTopicAlreadyExists)
	}

	// Topic is available, occupy it and set default values
	tx := etcd.Txn(context.TODO())
	_, err = tx.If().
		Then(
			clientv3.OpPut(tn+"/taken", "true"),
			clientv3.OpPut(tn+"/partitions", fmt.Sprint(partitions)),
			clientv3.OpPut(tn+"/created", fmt.Sprint(time.Now().Unix())),
		).Commit()

	if err != nil {
		return Topic{}, err
	}

	t := Topic{
		Name:       TopicName(tn),
		Partitions: partitions,
		etcd:       etcd,
	}

	return t, nil
}
