package gki

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
)

type DefaultProducerFactory struct{}

func (d DefaultProducerFactory) NewProducer(conf *kafka.ConfigMap) (ikafka.Producer, error) {
	return kafka.NewProducer(conf)
}

func (d DefaultProducerFactory) NewAdminClientFromProducer(p ikafka.Producer) (ikafka.AdminClient, error) {
	return kafka.NewAdminClientFromProducer(p.(*kafka.Producer))
}

func (d DefaultProducerFactory) CloseProducer(p ikafka.Producer) {
	if !IsNil(p) {
		p.Close()
	}
}

// SharedProducerFactory is a singleton per stream ID and creates and provides a shared
// Kafka producer for all stream instances of that stream.
type SharedProducerFactory struct {
	pf       ikafka.ProducerFactory
	producer ikafka.Producer
	mux      *sync.Mutex
}

func NewSharedProducerFactory(pf ikafka.ProducerFactory) *SharedProducerFactory {
	return &SharedProducerFactory{
		pf:  pf,
		mux: &sync.Mutex{},
	}
}

// NewProducer provides a shared Kafka producer. Since this method is called concurrently
// (e.g. from extractors' init section of StreamLoad(), creating the DLQ producer), we need
// to protect its logic with a mutex.
func (s *SharedProducerFactory) NewProducer(conf *kafka.ConfigMap) (ikafka.Producer, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	var err error
	if IsNil(s.producer) {
		s.producer, err = s.pf.NewProducer(conf)
	}
	return s.producer, err
}

// CloseProducer for the non-shared case will close the Kafka producer. For the
// SharedProducerFactory this cannot be done since it might be used still concurrently
// by other goroutines. This will therefore not do anything here.
// For streams being shut down by disabling them, and later re-activated this will not
// leak any handles since the previous producer will just be re-used again based on
// the stream ID for that stream.
func (s *SharedProducerFactory) CloseProducer(p ikafka.Producer) {
	// Nothing to do here
}

// CloseSharedProducer is not part of the standard ProducerFactory interface and must
// only be used in cases where the caller has full control of the status of all running
// stream instances (extractors, including producers), which typically is the Geist's
// Supervisor.
func (s *SharedProducerFactory) CloseSharedProducer() {
	s.mux.Lock()
	defer s.mux.Unlock()

	if !IsNil(s.producer) {
		s.producer.Close()
		s.producer = nil
	}
}

func (s *SharedProducerFactory) NewAdminClientFromProducer(p ikafka.Producer) (ikafka.AdminClient, error) {
	return kafka.NewAdminClientFromProducer(p.(*kafka.Producer))
}
