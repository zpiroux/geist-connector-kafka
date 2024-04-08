package ikafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type AdminClient interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
	Close()
}
