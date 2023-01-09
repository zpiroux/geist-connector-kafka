package gki

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer interface is used to enable full unit testing
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

type ProducerFactory interface {
	NewProducer(conf *kafka.ConfigMap) (Producer, error)
	NewAdminClientFromProducer(p Producer) (AdminClient, error)
}

type DefaultProducerFactory struct{}

func (d DefaultProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return kafka.NewProducer(conf)
}

func (d DefaultProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return kafka.NewAdminClientFromProducer(p.(*kafka.Producer))
}

// SharedProducerFactory is a singleton per stream ID and creates and provides a shared
// Kafka producer for all stream instances of that stream.
type SharedProducerFactory struct {
	producer *kafka.Producer
	mux      *sync.Mutex
}

func NewSharedProducerFactory() *SharedProducerFactory {
	return &SharedProducerFactory{
		mux: &sync.Mutex{},
	}
}

// NewProducer provides a shared Kafka producer. Since this method is called concurrently
// (e.g. from extractors' init section of StreamLoad(), creating the DLQ producer), we need
// to protect its logic with a mutex.
func (s *SharedProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	var err error
	if s.producer == nil {
		s.producer, err = kafka.NewProducer(conf)
	}
	return s.producer, err
}

func (s *SharedProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return kafka.NewAdminClientFromProducer(p.(*kafka.Producer))
}
