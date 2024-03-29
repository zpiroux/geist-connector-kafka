package ikafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

type ProducerFactory interface {
	NewProducer(conf *kafka.ConfigMap) (Producer, error)
	NewAdminClientFromProducer(p Producer) (AdminClient, error)
	CloseProducer(p Producer)
}
