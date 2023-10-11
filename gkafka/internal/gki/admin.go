package gki

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DefaultAdminClient struct {
	ac *kafka.AdminClient
}

func (d DefaultAdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return d.ac.GetMetadata(topic, allTopics, timeoutMs)
}

func (d DefaultAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	return d.ac.CreateTopics(ctx, topics, options...)
}
