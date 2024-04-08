package gki

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist-connector-kafka/gkafka/spec"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/pkg/notify"
)

const flushTimeoutSec = 10

type Loader struct {
	pf                            ikafka.ProducerFactory
	producer                      ikafka.Producer
	ac                            ikafka.AdminClient
	config                        *Config
	eventCount                    int64
	requestShutdown               bool
	sm                            sync.Mutex // shutdown mutex
	shutdownDeliveryReportHandler context.CancelFunc
	notifier                      *notify.Notifier
}

func NewLoader(ctx context.Context, config *Config, pf ikafka.ProducerFactory) (*Loader, error) {

	var err error
	if IsNil(pf) {
		pf = DefaultProducerFactory{}
	}

	l := &Loader{
		pf:     pf,
		config: config,
	}

	if config.sinkTopic == nil {
		return l, fmt.Errorf("no topic spec provided when creating loader: %+v", l)
	} else if config.sinkTopic.Name == "" {
		return l, fmt.Errorf("no topic name provided when creating loader: %+v", l)
	}

	var log *logger.Log
	if config.c.Log {
		log = logger.New()
	}
	l.notifier = notify.New(config.c.NotifyChan, log, 2, "gkafka.loader", l.config.c.ID, config.c.Spec.Id())

	if err = l.createProducer(); err != nil {
		return l, err
	}

	admcli, err := l.pf.NewAdminClientFromProducer(l.producer)
	if err != nil {
		return l, fmt.Errorf(l.lgprfx()+"couldn't create admin client, err: %v", err)
	}
	l.ac = admcli

	if err = l.createTopic(ctx, l.config.sinkTopic); err != nil {
		return l, err
	}

	if !l.config.synchronous {
		ctxDRH, cancel := context.WithCancel(ctx)
		if cancel == nil {
			return l, fmt.Errorf(l.lgprfx() + "cancel func for deliveryReportHandler could not be created")
		}
		l.shutdownDeliveryReportHandler = cancel
		go l.deliveryReportHandler(ctx, ctxDRH)
	}

	l.notifier.Notify(entity.NotifyLevelInfo, "Loader created with config: %s", l.config)
	return l, nil
}

func (l *Loader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	if l.requestShutdown {
		return "", entity.ErrEntityShutdownRequested, false
	}

	if data[0] == nil {
		return "", errors.New("streamLoad called without data to load (data[0] == nil)"), false
	}

	payloadKey := l.config.message.PayloadFromId

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &l.config.sinkTopic.Name, Partition: kafka.PartitionAny},
	}

	switch value := data[0].Data[payloadKey].(type) {
	case []byte:
		msg.Value = value
	case string:
		msg.Value = []byte(value)
	default:
		return "", fmt.Errorf("invalid payload data type used in spec, type = %T", value), false
	}

	return l.publishMessage(context.Background(), msg)
}

func (l *Loader) Shutdown(ctx context.Context) {
	l.sm.Lock()
	defer l.sm.Unlock()
	l.notifier.Notify(entity.NotifyLevelDebug, "Shutdown initiated")
	if l.producer != nil {
		if unflushed := l.producer.Flush(flushTimeoutSec * 1000); unflushed > 0 {
			l.notifier.Notify(entity.NotifyLevelError, "%d messages did not get flushed during shutdown, check for potential inconsistencies", unflushed)
		}
		if l.shutdownDeliveryReportHandler != nil {
			l.shutdownDeliveryReportHandler()
		}
		if !IsNil(l.ac) {
			l.ac.Close()
		}
		l.producer.Close()
		l.producer = nil
		l.notifier.Notify(entity.NotifyLevelInfo, "Shutdown completed, number of published events: %d", l.eventCount)
	}
}

func (l *Loader) createProducer() error {

	var err error
	kconfig := make(kafka.ConfigMap)
	for k, v := range l.config.configMap {
		kconfig[k] = v
	}

	l.producer, err = l.pf.NewProducer(&kconfig)

	if err != nil {
		return fmt.Errorf(l.lgprfx()+"Failed to create producer: %s", err.Error())
	}

	return nil

}

func (l *Loader) publishMessage(ctx context.Context, m *kafka.Message) (string, error, bool) {

	var (
		resourceId string // not assigned at the moment
		err        error
		retryable  bool
	)

	if l.config.c.Spec.Ops.LogEventData {
		l.notifier.Notify(entity.NotifyLevelDebug, "Sending event %+v with producer: %+v", string(m.Value), l.producer)
	}

	start := time.Now()
	err = l.producer.Produce(m, nil)

	if !l.config.synchronous {
		if l.config.c.Spec.Ops.LogEventData {
			duration := time.Since(start)
			l.notifier.Notify(entity.NotifyLevelInfo, "Event enqueued async [duration: %v] with err: %v", duration, err)
		}
		return resourceId, err, true
	}

	if err == nil {
		event := <-l.producer.Events()
		switch msg := event.(type) {
		case *kafka.Message:
			if msg.TopicPartition.Error != nil {
				err = fmt.Errorf("publish failed with err: %v", msg.TopicPartition.Error)
				retryable = true
			} else {
				l.eventCount++
				if l.config.c.Spec.Ops.LogEventData {
					duration := time.Since(start)
					l.notifier.Notify(entity.NotifyLevelInfo, "Event published [duration: %v] to %s [%d] at offset: %v, key: %v value: %s",
						duration, *msg.TopicPartition.Topic, msg.TopicPartition.Partition,
						msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))
				}
			}
		case kafka.Error:
			err = fmt.Errorf(l.lgprfx()+"Kafka error in producer, code: %v, event: %v", msg.Code(), msg)
			// In case of all brokers down, terminate (will be restarted with exponential backoff)
			if msg.Code() == kafka.ErrAllBrokersDown {
				err = entity.ErrEntityShutdownRequested
				retryable = false
			}
		default:
			// Docs don't say if this could happen in single event produce.
			// Probably not, but if so we don't know if Produce() succeeded, so need to retry
			err = fmt.Errorf(l.lgprfx()+"unexpected Kafka info event from Kafka Producer report: %v, treat as error and retry", msg)
			retryable = true
		}
	} else {
		l.notifier.Notify(entity.NotifyLevelError, "kafka.producer.Produce() failed with err: %v, msg: %+v, topic: %s. Operation will be retried.", err, m, l.config.topics[0])
		retryable = true // Treat all these kinds of errors as retryable for now
	}

	return resourceId, err, retryable
}

func (l *Loader) deliveryReportHandler(ctxParent context.Context, ctxThis context.Context) {

	for {
		select {

		case <-ctxParent.Done():
			l.notifier.Notify(entity.NotifyLevelInfo, "[DRH] parent ctx closed, requesting shutdown")
			l.requestShutdown = true

		case <-ctxThis.Done():
			l.notifier.Notify(entity.NotifyLevelInfo, "[DRH] ctx closed, shutting down")
			return

		case e := <-l.producer.Events():
			switch event := e.(type) {
			case *kafka.Message:
				m := event
				if m.TopicPartition.Error != nil {
					l.notifier.Notify(entity.NotifyLevelError, "[DRH] publish failed with err: %v", m.TopicPartition.Error)
				} else {
					l.eventCount++
					if l.config.c.Spec.Ops.LogEventData {
						l.notifier.Notify(entity.NotifyLevelInfo, "[DRH] event published to %s [%d] at offset: %v, key: %v value: %s",
							*m.TopicPartition.Topic, m.TopicPartition.Partition,
							m.TopicPartition.Offset, string(m.Key), string(m.Value))
					}
				}

			case kafka.Error:
				e := event
				if e.IsFatal() {
					l.notifier.Notify(entity.NotifyLevelError, "[DRH] fatal error: %v, requesting shutdown", e)
					l.requestShutdown = true
				} else {
					l.notifier.Notify(entity.NotifyLevelError, "[DRH] error: %v", e)
				}

			default:
				l.notifier.Notify(entity.NotifyLevelInfo, "[DRH] Ignored event: %s", event)
			}
		}
	}
}

func (l *Loader) createTopic(ctx context.Context, topicSpec *spec.TopicSpecification) error {

	if !l.config.createTopics {
		return nil
	}

	// Not really needed in current topic creation implementation, but keep for now
	l.config.topicCreationMutex.Lock()
	defer l.config.topicCreationMutex.Unlock()

	topic := kafka.TopicSpecification{
		Topic:             topicSpec.Name,
		NumPartitions:     topicSpec.NumPartitions,
		ReplicationFactor: topicSpec.ReplicationFactor,
		//Config:            topicSpec.Config, // TODO: add to test (if config needed in a future update)
	}

	res, err := l.ac.CreateTopics(ctx, []kafka.TopicSpecification{topic})

	if err == nil {
		l.notifier.Notify(entity.NotifyLevelInfo, "Topic created: %+v", res)
		return nil
	}

	if err.Error() == kafka.ErrTopicAlreadyExists.String() {
		l.notifier.Notify(entity.NotifyLevelInfo, "Topic %s for this stream already exists", topicSpec.Name)
		err = nil
	} else {
		l.notifier.Notify(entity.NotifyLevelError, "Could not create topic with spec: %+v, err: %v", topic, err)
	}

	return err
}

func (l *Loader) lgprfx() string {
	return "[" + l.notifier.Sender() + ":" + l.notifier.Instance() + "] "
}

func (l *Loader) KafkaConfig() map[string]any {
	return l.config.configMap
}
