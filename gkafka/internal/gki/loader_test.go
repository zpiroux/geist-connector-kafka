package gki

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
)

func TestStreamLoad(t *testing.T) {

	var (
		retryable        bool
		eventFromSourceX = []byte(`{ "veryInterestingDataField": "omg" }`)
	)

	spec, err := entity.NewSpec(genericSourceToKafkaSinkSpec)
	assert.NoError(t, err)
	assert.NotNil(t, spec)
	loader, err := createMockLoader(spec, true)
	assert.NoError(t, err)
	transformer := transform.NewTransformer(spec)

	transformed, err := transformer.Transform(context.Background(), eventFromSourceX, &retryable)
	assert.NoError(t, err)
	assert.NotEqual(t, len(transformed), 0)
	fmt.Printf("event transformed into: %v\n", transformed)

	_, err, retryable = loader.StreamLoad(context.Background(), transformed)
	assert.NoError(t, err)
}

func createMockLoader(spec *entity.Spec, synchronous bool) (*Loader, error) {
	topicSpec := &entity.TopicSpecification{
		Name:              "coolTopic",
		NumPartitions:     3,
		ReplicationFactor: 3,
	}
	notifyChan := make(entity.NotifyChan, 128)
	go handleNotificationEvents(notifyChan)

	config := NewLoaderConfig(entity.Config{Spec: spec, ID: "mockInstanceId", NotifyChan: notifyChan}, topicSpec, &sync.Mutex{}, synchronous)
	loader, err := NewLoader(context.Background(), config, MockProducerFactory{})
	return loader, err
}

type MockProducerFactory struct{}

func (mpf MockProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return NewMockProducer(), nil
}

func (mpf MockProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

func NewMockProducer() Producer {
	p := &MockProducer{}
	p.events = make(chan kafka.Event, 10)
	return p
}

type MockProducer struct {
	events chan kafka.Event
}

func (p *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	nbPublishRequests++
	dChan := p.events
	if deliveryChan != nil {
		dChan = deliveryChan
	}
	msg.TopicPartition.Error = nil
	dChan <- msg
	return nil
}

func (p MockProducer) Events() chan kafka.Event {
	return p.events
}

func (p MockProducer) Flush(timeoutMs int) int {
	return 0
}

func (p MockProducer) Close() {
	// Nothing to close
}

var genericSourceToKafkaSinkSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtokafkaproxy",
   "description": "Generic spec for any source forwarding raw event into a Kafka topic",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "kafka"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "payload"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "kafka",
      "config": {
         "topic": [
            {
               "env": "all",
               "topicSpec": {
                  "name": "events_from_source_x",
                  "numPartitions": 6,
                  "replicationFactor": 3
               }
            }
         ],
         "properties": [
            {
               "key": "client.id",
               "value": "geist_xtokafkaproxy"
            }
         ],
         "message": {
            "payloadFromId": "payload"
         }
      }
   }
}`)
