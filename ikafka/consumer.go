package ikafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Consumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	StoreOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Commit() ([]kafka.TopicPartition, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type ConsumerFactory interface {
	NewConsumer(conf *kafka.ConfigMap) (Consumer, error)
	NewAdminClientFromConsumer(c Consumer) (AdminClient, error)
}
