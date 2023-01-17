package gki

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tidwall/sjson"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
	"github.com/zpiroux/geist/entity"
)

const (
	metadataRequestTimeoutMs        = 5000
	dlqMaxPublishBackoffTimeSec     = 30
	dlqTopicNumPartitions           = 6
	dlqTopicReplicationFactor       = 3
	dlqDeliveryChanSize             = 32
	dlqDeliveryReportTimeoutSeconds = 5
)

func NewDLQConfig(pconf map[string]any, enrichPath string, topic *entity.TopicSpecification) (DLQConfig, error) {
	var dlq DLQConfig
	if topic == nil {
		return dlq, errors.New("invalid DLQ config provided in stream spec")
	}

	if topic.Name == "" {
		return dlq, errors.New("no applicable DLQ topic name found from stream spec config")
	}

	dlq.Topic = topic
	dlq.ProducerConfig = pconf
	dlq.StreamIDEnrichmentPath = enrichPath
	return dlq, nil
}

type DLQConfig struct {
	// Topic specifies which topic to use for DLQ events. If the global extractor
	// setting createTopics is set to false, only Topic.Name is regarded. Otherwise,
	// NumPartitions and ReplicationFactor will be used as well if the topic is created
	// (if it doesn't exist already).
	Topic *entity.TopicSpecification

	ProducerConfig map[string]any

	// If StreamIDEnrichmentPath is not empty it specifies the JSON path (e.g.
	// "my.enrichment.streamId") including the JSON field name, which will hold the
	//  value of the injected stream ID for the current stream. That is, before the
	// event is sent to the DLQ the stream ID is added to a new field created in the
	// event, if this option is used.
	StreamIDEnrichmentPath string
}

func (e *Extractor) initDLQ(ctx context.Context) (err error) {

	if e.config.c.Spec.Ops.HandlingOfUnretryableEvents != entity.HoueDlq {
		return nil
	}

	if err = e.createDlqTopic(ctx); err != nil {
		return err
	}

	if err = e.createDlqProducer(e.pf); err != nil {
		return err
	}

	return err
}

func (e *Extractor) createDlqTopic(ctx context.Context) error {

	if !e.config.createTopics {
		return nil
	}

	e.config.topicCreationMutex.Lock()
	defer e.config.topicCreationMutex.Unlock()

	dlqTopicMetadata, err := e.ac.GetMetadata(nil, true, metadataRequestTimeoutMs)
	if err != nil {
		return fmt.Errorf(e.lgprfx()+"could not get metadata from Kafka cluster, err: %v", err)
	}

	topicConfig := e.config.dlqConfig.Topic

	if !topicExists(topicConfig.Name, dlqTopicMetadata.Topics) {

		e.notifier.Notify(entity.NotifyLevelInfo, "DLQ topic doesn't exist for stream %s, metadata: %+v, err: %v", e.config.c.Spec.Id(), dlqTopicMetadata, err)
		dlq := kafka.TopicSpecification{
			Topic:             topicConfig.Name,
			NumPartitions:     topicConfig.NumPartitions,
			ReplicationFactor: topicConfig.ReplicationFactor,
		}

		res, err := e.ac.CreateTopics(ctx, []kafka.TopicSpecification{dlq})

		if err != nil {
			return fmt.Errorf(e.lgprfx()+"could not create DLQ topic %+v, err: %v", dlq, err)
		}
		e.notifier.Notify(entity.NotifyLevelInfo, "DLQ topic created: %+v", res)
	} else {
		e.notifier.Notify(entity.NotifyLevelInfo, "DLQ topic for this stream already exists with name: %s", topicConfig.Name)
	}

	return nil
}

func (e *Extractor) createDlqProducer(pf ikafka.ProducerFactory) error {

	var err error
	kconfig := make(kafka.ConfigMap)

	// Add producer specific default props, overridable by the stream spec
	kconfig["enable.idempotence"] = true
	kconfig["compression.type"] = "lz4"

	for k, v := range e.config.configMap {
		if _, ok := commonConsumerProps[k]; ok {
			continue
		}
		kconfig[k] = v
	}

	for k, v := range e.config.dlqConfig.ProducerConfig {
		kconfig[k] = v
	}

	e.dlqProducer, err = e.pf.NewProducer(&kconfig)
	if err != nil {
		return fmt.Errorf(e.lgprfx()+"Failed to create DLQ producer: %s", err.Error())
	}

	e.dlqDeliveryChan = make(chan kafka.Event, dlqDeliveryChanSize)
	e.notifier.Notify(entity.NotifyLevelInfo, "DLQ Producer created")
	return nil
}

func (e *Extractor) dlqTopicName() (topicName string) {
	if e.config.dlqConfig.Topic != nil {
		topicName = e.config.dlqConfig.Topic.Name
	}
	return
}

func (e *Extractor) moveEventsToDLQ(ctx context.Context, msgs []*kafka.Message) (a action, err error) {

	for _, m := range msgs {
		a, err = e.moveEventToDLQ(ctx, m)
		if a == actionShutdown {
			break
		}
	}
	return a, err
}

func (e *Extractor) enrichWithStreamID(event []byte) ([]byte, error) {
	return sjson.SetBytes(event, e.config.dlqConfig.StreamIDEnrichmentPath, e.config.c.Spec.Id())
}

func (e *Extractor) moveEventToDLQ(ctx context.Context, m *kafka.Message) (a action, err error) {

	// Infinite loop (or shutdown) here to ensure we never loose a message. For long
	// disruptions this consumer's partitions will be reassigned to another consumer
	// after the session times out, which in worst case might lead to duplicates, but
	// no loss.
	backoffDuration := 1
	dlqTopic := e.dlqTopicName()

	if dlqTopic == "" {
		return actionShutdown, errors.New("invalid DLQ config, no DLQ topic name provided, shutting down stream")
	}

	mCopy := *m
	mCopy.TopicPartition.Topic = &dlqTopic
	mCopy.TopicPartition.Partition = kafka.PartitionAny
	for i := 0; ; i++ {

		if err != nil {
			e.notifier.Notify(entity.NotifyLevelError, "Failed (attempt #%d) to publish event (%+v) (from source event %+v) on DLQ topic '%s' with err: %v - next attempt in %d seconds",
				i, &mCopy, m, dlqTopic, err, backoffDuration)
			time.Sleep(time.Duration(backoffDuration) * time.Second)
			if backoffDuration < dlqMaxPublishBackoffTimeSec {
				backoffDuration *= 2
			}
		}

		if e.config.dlqConfig.StreamIDEnrichmentPath != "" {
			mCopy.Value, err = e.enrichWithStreamID(mCopy.Value)
			if err != nil {
				e.notifier.Notify(entity.NotifyLevelError, "Failed to enrich event (%+v) on DLQ topic '%s' with err: %v", m, dlqTopic, err)
			}
		}

		if ctx.Err() == context.Canceled {
			e.notifier.Notify(entity.NotifyLevelWarn, "Context canceled received in DLQ producer, shutting down")
			return actionShutdown, err
		}

		e.notifier.Notify(entity.NotifyLevelDebug, "Sending event to DLQ with producer: %+v", e.dlqProducer)
		err = e.dlqProducer.Produce(&mCopy, e.dlqDeliveryChan)

		if err != nil {
			continue
		}

		a, err = e.handleDeliveryReport(ctx, m, &mCopy)

		switch a {
		case actionContinue, actionShutdown:
			return a, err
		case actionRetry:
			continue
		default:
			return actionShutdown, fmt.Errorf("bug, invalid action (%d) returned by handleDeliveryReport, err=%s", a, err)
		}
	}
}

func (e *Extractor) handleDeliveryReport(ctx context.Context, m, mCopy *kafka.Message) (a action, err error) {
	for {
		select {

		case <-time.After(time.Second * dlqDeliveryReportTimeoutSeconds):
			// If not getting the report in time, we continue to wait for it until we're
			// told to shut down. We notify this as an error, even though it might be a
			// self-healing one, since a delivery report should not take this long to
			// produce and might need looking into.
			e.notifier.Notify(entity.NotifyLevelError, "Timeout while waiting for DLQ response for msg: %v", mCopy)
			if ctx.Err() == context.Canceled {
				e.notifier.Notify(entity.NotifyLevelWarn, "Context canceled while waiting for DLQ delivery report, shutting down")
				return actionShutdown, err
			}

		case event := <-e.dlqDeliveryChan:

			switch dlqMsg := event.(type) {
			case *kafka.Message:
				if dlqMsg.TopicPartition.Error == nil {
					e.handleEventPublishedToDLQ(m, dlqMsg)
					return actionContinue, err
				}

				err = fmt.Errorf("DLQ publish failed with err: %v", dlqMsg.TopicPartition.Error)
				return actionRetry, err

			case kafka.Error:
				err = fmt.Errorf("kafka error in dlq producer, code: %v, event: %v", dlqMsg.Code(), dlqMsg)
				// In case of all brokers down, terminate (will be restarted with exponential back-off)
				if dlqMsg.Code() == kafka.ErrAllBrokersDown {
					return actionShutdown, err
				}
				return actionRetry, err

			default:
				// Docs don't say if this could happen in single event produce.
				// Probably not, but if so, continue waiting for report.
				e.notifier.Notify(entity.NotifyLevelWarn, "Unexpected Kafka info event when waiting for DLQ delivery report, %v", dlqMsg)
			}
		}
	}
}

func (e *Extractor) handleEventPublishedToDLQ(msg, dlqMsg *kafka.Message) {
	e.notifier.Notify(entity.NotifyLevelInfo, "Event published to DLQ %s [%d] at offset %v, value: %s",
		*dlqMsg.TopicPartition.Topic, dlqMsg.TopicPartition.Partition,
		dlqMsg.TopicPartition.Offset, string(dlqMsg.Value))

	// TODO: When migrated to new common producer, move this out to Extractor
	if serr := e.storeOffsets([]*kafka.Message{msg}); serr != nil {
		// No need to handle this error, just log it
		e.notifier.Notify(entity.NotifyLevelError, "Error storing offsets after DLQ, event: %+v, err: %v", msg, serr)
	}
}

func topicExists(topicToFind string, existingTopics map[string]kafka.TopicMetadata) bool {
	for _, topic := range existingTopics {
		if topic.Topic == topicToFind {
			return true
		}
	}
	return false
}
