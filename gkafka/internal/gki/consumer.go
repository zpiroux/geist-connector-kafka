package gki

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
)

type DefaultConsumerFactory struct{}

func (d DefaultConsumerFactory) NewConsumer(conf *kafka.ConfigMap) (ikafka.Consumer, error) {
	return kafka.NewConsumer(conf)
}

func (d DefaultConsumerFactory) NewAdminClientFromConsumer(c ikafka.Consumer) (ikafka.AdminClient, error) {
	ac, err := kafka.NewAdminClientFromConsumer(c.(*kafka.Consumer))
	if err != nil {
		return nil, err
	}
	return DefaultAdminClient{ac: ac}, nil
}
