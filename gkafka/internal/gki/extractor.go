package gki

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/pkg/notify"
)

type action int

const (
	actionInvalid action = iota
	actionContinue
	actionShutdown
	actionRetry
)

// Kafka props which cannot be overridden, required by Extractor
const (
	PropAutoCommit      = "enable.auto.commit"
	PropAutoOffsetStore = "enable.auto.offset.store"
)

const DefaultPollTimeoutMs = 3000

type Extractor struct {
	cf              ConsumerFactory
	pf              ProducerFactory
	consumer        Consumer
	dlqProducer     Producer
	dlqDeliveryChan chan kafka.Event
	sourceProducer  Producer
	config          *Config
	ac              AdminClient
	notifier        *notify.Notifier
	eventCount      int64
}

func NewExtractor(config *Config, cf ConsumerFactory) (*Extractor, error) {

	if IsNil(cf) {
		cf = &DefaultConsumerFactory{}
	}

	e := &Extractor{
		cf:     cf,
		pf:     DefaultProducerFactory{},
		config: config,
	}
	if len(config.topics) == 0 {
		return e, fmt.Errorf("no topics provided when creating Extractor: %+v", e)
	}

	// Ensure required consumer props are set correctly.
	// With the two prop values below we ensure no message loss while maximizing throughput.
	// Messages verified written to the sink will have its offsets stored in-mem via
	// extractor.storeOffsets(), while the enabled auto-commit will commit all the stored
	// offsets at appropriate interval.
	e.config.configMap[PropAutoCommit] = true
	e.config.configMap[PropAutoOffsetStore] = false

	// Ensure appropriate final configurable props
	if e.config.pollTimeoutMs == 0 {
		e.config.pollTimeoutMs = DefaultPollTimeoutMs
	}

	var log *logger.Log
	if config.c.Log {
		log = logger.New()
	}
	e.notifier = notify.New(config.c.NotifyChan, log, 2, "gkafka.extractor", e.config.c.ID, config.c.Spec.Id())

	e.notifier.Notify(entity.NotifyLevelInfo, "Extractor created with config: %s", e.config)
	return e, nil
}

func (e *Extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var (
		events []entity.Event
		msgs   []*kafka.Message
	)

	e.notifier.Notify(entity.NotifyLevelInfo, "StreamExtract starting up with ops: %+v, config %s", e.config.c.Spec.Ops, e.config)
	defer e.closeStreamExtract()

	*retryable = true
	if *err = e.initStreamExtract(ctx); *err != nil {
		return
	}

	run := true
	microBatchStart := time.Now()
	microBatchBytes := 0

	for run {

		event := e.consumer.Poll(e.config.pollTimeoutMs)

		if ctx.Err() == context.Canceled {
			e.notifier.Notify(entity.NotifyLevelInfo, "Context canceled in StreamExtract")
			*retryable = false
			return
		}

		if event == nil {

			if e.config.c.Spec.Ops.MicroBatch {
				// Poll timeout events are used to check if the microbatch window has closed and therefore if
				// event batch should be sent to downstream processing.
				if len(events) > 0 && microBatchTimedOut(microBatchStart, e.config.c.Spec.Ops.MicroBatchTimeoutMs) {
					if e.config.c.Spec.Ops.LogEventData {
						e.notifier.Notify(entity.NotifyLevelInfo, "Microbatch window timed out (started: %v), processing unfilled batch of %d events", microBatchStart, len(events))
					}
					switch e.handleEventProcessingResult(ctx, msgs, reportEvent(ctx, events), err, retryable) {
					case actionShutdown:
						run = false
					case actionContinue:
						e.eventCount += int64(len(events))
						msgs = nil
						events = nil
						microBatchStart = time.Now()
						microBatchBytes = 0
					}
				}
			}
			continue
		}

		switch evt := event.(type) {
		case *kafka.Message:
			if evt.TopicPartition.Error != nil {
				// This should be a producer-only error. Might have received this here long ago, so keep for logging purposes.
				*err = evt.TopicPartition.Error
				e.notifier.Notify(entity.NotifyLevelError, "Topic partition error when consuming message, msg: %+v, msg value: %s, err: %s", evt, string(evt.Value), *err)

				// Using normal event processing for this undocumented behaviour
				// Alternatively, switch to 'continue' here, depending on test results.
			}
			if e.config.c.Spec.Ops.LogEventData {
				e.notifier.Notify(entity.NotifyLevelInfo, "Event consumed from %s:%s", evt.TopicPartition, string(evt.Value))
				if evt.Headers != nil {
					e.notifier.Notify(entity.NotifyLevelInfo, "Headers: %v", evt.Headers)
				}
			}

			msgs = append(msgs, evt)

			if e.config.c.Spec.Ops.MicroBatch {

				microBatchBytes += len(evt.Value)
				events = append(events, entity.Event{
					Key:  evt.Key,
					Ts:   evt.Timestamp,
					Data: evt.Value,
				})
				if len(events) < e.config.c.Spec.Ops.MicroBatchSize &&
					microBatchBytes < e.config.c.Spec.Ops.MicroBatchBytes &&
					!microBatchTimedOut(microBatchStart, e.config.c.Spec.Ops.MicroBatchTimeoutMs) {
					continue
				}
				if e.config.c.Spec.Ops.LogEventData {
					e.notifier.Notify(entity.NotifyLevelInfo, "Microbatch full, nb events: %d, bytes: %d", len(events), microBatchBytes)
				}

			} else {
				events = []entity.Event{{
					Key:  evt.Key,
					Ts:   evt.Timestamp,
					Data: evt.Value,
				}}
			}

			switch e.handleEventProcessingResult(ctx, msgs, reportEvent(ctx, events), err, retryable) {

			case actionShutdown:
				run = false
			case actionContinue:
				e.eventCount += int64(len(events))
				msgs = nil
				events = nil
				microBatchStart = time.Now()
				microBatchBytes = 0
				continue
			}

		case kafka.Error:
			str := fmt.Sprintf("(%s) Kafka error in consumer, code: %v, event: %v", e.config.c.Spec.Id(), evt.Code(), evt)
			e.notifier.Notify(entity.NotifyLevelWarn, str) // Most errors are recoverable

			// In case of all brokers down, terminate the Extractor and let Executor/Supervisor decide what to do.
			if evt.Code() == kafka.ErrAllBrokersDown {
				*err = errors.New(str)
				run = false
			}
		default:
			if strings.Contains(evt.String(), "OffsetsCommitted") {
				if e.config.c.Spec.Ops.LogEventData {
					e.notifier.Notify(entity.NotifyLevelDebug, "Kafka info event in consumer: %v", evt)
				}
			} else {
				e.notifier.Notify(entity.NotifyLevelInfo, "Kafka info event in consumer: %v", evt)
			}
		}
	}
}

// handleEventProcessingResult processes the result from the downstream event processing
// and returns actionContinue if extraction should continue or actionShutdown in case of
// fatal/unretryable errors or normal shutdown scenarios.
func (e *Extractor) handleEventProcessingResult(
	ctx context.Context,
	msgs []*kafka.Message,
	result entity.EventProcessingResult,
	err *error,
	retryable *bool) action {

	switch result.Status {

	case entity.ExecutorStatusSuccessful:
		if result.Error != nil {
			e.notifier.Notify(entity.NotifyLevelError, "Bug in executor, shutting down, result.Error should be nil if ExecutorStatusSuccessful, result: %+v", result)
			return actionShutdown
		}
		*err = e.storeOffsets(msgs)
		return actionContinue

	case entity.ExecutorStatusShutdown:
		e.notifier.Notify(entity.NotifyLevelWarn, "Shutting down Extractor due to executor shutdown, reportEvent result: %+v", result)
		return actionShutdown

	case entity.ExecutorStatusRetriesExhausted:
		*err = fmt.Errorf(e.lgprfx()+"executor failed all retries, shutting down Extractor, handing over to executor, reportEvent result: %+v", result)
		return actionShutdown

	case entity.ExecutorStatusError:
		*retryable = false
		if result.Retryable {
			*err = fmt.Errorf(e.lgprfx() + "bug, executor should handle all retryable errors, until retries exhausted, shutting down Extractor")
			return actionShutdown
		}
		str := e.lgprfx() + "executor had an unretryable error with this "
		if len(msgs) == 1 {
			str = fmt.Sprintf("%s"+"event: '%s', reportEvent result: %+v", str, string(msgs[0].Value), result)
		} else {
			str = fmt.Sprintf("%s"+"event batch, reportEvent result: %+v", str, result)
		}
		e.notifier.Notify(entity.NotifyLevelWarn, "%s", str)

		switch e.config.c.Spec.Ops.HandlingOfUnretryableEvents {

		case entity.HoueDefault:
			fallthrough
		case entity.HoueDiscard:
			e.notifier.Notify(entity.NotifyLevelWarn, "A Kafka event failed downstream processing with result %+v; "+
				" since this stream (%s) does not have DLQ enabled, the event will now be discarded, events: %+v",
				result, e.config.c.Spec.Id(), msgs)
			*err = e.storeOffsets(msgs)
			return actionContinue

		case entity.HoueDlq:
			var a action
			a, *err = e.moveEventsToDLQ(ctx, msgs)
			return a

		case entity.HoueFail:
			str += " - since this stream's houe mode is set to HoueFail, the stream will now be shut down, requiring manual/external restart"
			*err = errors.New(str)
			return actionShutdown
		}
	}
	*err = fmt.Errorf("encountered a 'should not happen' error in Extractor.handleEventProcessingResult, "+
		"shutting down stream, reportEvent result %+v, events: %v, spec: %v", result, msgs, e.config.c.Spec)
	*retryable = false
	return actionShutdown
}

func (e *Extractor) initStreamExtract(ctx context.Context) error {
	var err error

	if err = e.createConsumer(e.cf); err != nil {
		return err
	}

	admcli, err := e.cf.NewAdminClientFromConsumer(e.consumer)
	if err != nil {
		return fmt.Errorf(e.lgprfx()+"couldn't create admin client, err: %v", err)
	}
	e.ac = admcli

	if err = e.initDLQ(ctx); err != nil {
		return err
	}

	if err = e.createSourceProducer(e.pf); err != nil {
		return err
	}
	if err = e.consumer.SubscribeTopics(e.config.topics, nil); err != nil {
		return fmt.Errorf(e.lgprfx()+"failed subscribing to topics '%v' with err: %v", e.config.topics, err)
	}
	return nil
}

func (e *Extractor) closeStreamExtract() {
	if !IsNil(e.consumer) {
		e.notifier.Notify(entity.NotifyLevelInfo, "Closing Kafka consumer %+v, consumed events: %d", e.consumer, e.eventCount)
		if err := e.consumer.Close(); err != nil {
			e.notifier.Notify(entity.NotifyLevelError, "Error closing Kafka consumer, err: %v", err)
		} else {
			e.notifier.Notify(entity.NotifyLevelInfo, "Kafka consumer %+v closed successfully", e.consumer)
		}
	}
	e.pf.CloseProducer(e.dlqProducer)
}

func (e *Extractor) Extract(
	ctx context.Context,
	query entity.ExtractorQuery,
	result any) (error, bool) {

	// Specific Kafka queries, topic lists, etc, not yet supported
	return errors.New("not supported"), false
}

func (e *Extractor) ExtractFromSink(
	ctx context.Context,
	query entity.ExtractorQuery,
	result *[]*entity.Transformed) (error, bool) {

	return errors.New("not supported"), false
}

// SendToSource is meant for occasional event publish to the topic used by the extractor
// to consume from. The focus of the function is on resilient and synchronous event
// sending rather than high throughput scenarios.
// If the stream spec specifies multiple topics to consume from by the extractor, the
// first one will always be used as the one to use with SendToSource.
func (e *Extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	if !e.config.sendToSourceEnabled {
		return "", fmt.Errorf("disabled - set Config.SendToSource to true in extractor factory to enable it")
	}

	var err error
	deliveryChan := make(chan kafka.Event, 64)

	topic := &e.config.topics[0]
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
	}

	switch value := eventData.(type) {
	case []byte:
		msg.Value = value
	case string:
		msg.Value = []byte(value)
	default:
		return "", fmt.Errorf("invalid payload eventData type (%T), must be string or []byte", value)
	}

	err = e.sourceProducer.Produce(msg, deliveryChan)

	if err != nil {
		return "", err
	}

	for event := range deliveryChan {
		switch srcMsg := event.(type) {

		case *kafka.Message:
			if srcMsg.TopicPartition.Error != nil {
				err = fmt.Errorf("SendToSource publish failed with err: %v", srcMsg.TopicPartition.Error)
			}
			return "", err

		case kafka.Error:
			return "", fmt.Errorf("kafka error in SendToSource producer, code: %v, event: %v", srcMsg.Code(), srcMsg)

		default:
			e.notifier.Notify(entity.NotifyLevelWarn, "Unexpected Kafka info event in SendToSource, %+v, waiting for proper event response", srcMsg)
		}
	}

	return "", err
}

func (e *Extractor) SetConsumerFactory(cf ConsumerFactory) {
	e.cf = cf
}

func (e *Extractor) SetProducerFactory(pf ProducerFactory) {
	e.pf = pf
}

func (e *Extractor) createConsumer(cf ConsumerFactory) error {

	kconfig := make(kafka.ConfigMap)
	for k, v := range e.config.configMap {
		kconfig[k] = v
	}

	consumer, err := cf.NewConsumer(&kconfig)

	if err != nil {
		return fmt.Errorf(e.lgprfx()+"failed to create consumer, config: %+v, err: %v", kconfig, err.Error())
	}

	e.consumer = consumer

	e.notifier.Notify(entity.NotifyLevelInfo, "Created consumer %+v with config: %s", consumer, e.config)
	return nil
}

func (e *Extractor) storeOffsets(msgs []*kafka.Message) error {

	var offsets []kafka.TopicPartition

	for _, m := range msgs {
		tp := m.TopicPartition
		tp.Offset++
		offsets = append(offsets, tp)
	}

	if e.config.c.Spec.Ops.LogEventData {
		e.notifier.Notify(entity.NotifyLevelDebug, "Storing offsets for events: %v, offsets: %v", msgs, offsets)
	}
	offsets, err := e.consumer.StoreOffsets(offsets)

	if err != nil {
		// One example when this error could happen is during a redeployment of the
		// hosting service *and* spec.ops.streamsPerPod is set much lower than the
		// number of topic partitions *and* spec.ops.microBatch is set to true.
		// This will in worst case cause duplicates but no loss. There is no point
		// retrying, and no run-time fix, so just logging error.
		// To prevent this from happening, increase spec.ops.streamsPerPod so that the
		// total number across all pods is closer to the number of topic partitions,
		// and change from Kafka's default partition assignment strategy to cooperative
		// sticky.
		e.notifier.Notify(entity.NotifyLevelError, "Error storing offsets, event: %+v, tp: %v, err: %v", msgs, offsets, err)
	}
	return err
}

func (e *Extractor) createSourceProducer(pf ProducerFactory) error {

	if !e.config.sendToSourceEnabled {
		return nil
	}

	var err error
	kconfig := make(kafka.ConfigMap)
	kconfig["enable.idempotence"] = true

	for k, v := range e.config.configMap {
		if _, ok := commonConsumerProps[k]; ok {
			continue
		}
		kconfig[k] = v
	}

	e.sourceProducer, err = e.pf.NewProducer(&kconfig)

	if err != nil {
		return fmt.Errorf(e.lgprfx()+"failed to create source producer: %s", err.Error())
	}

	return nil
}

func (e *Extractor) lgprfx() string {
	return "[" + e.notifier.Sender() + ":" + e.notifier.Instance() + "] "
}

func microBatchTimedOut(start time.Time, mbTimeoutMs int) bool {
	return time.Since(start) > time.Duration(mbTimeoutMs)*time.Millisecond
}

func IsNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

func (e *Extractor) KafkaConfig() (pollTimeoutMs int, cfgMap map[string]any) {
	return e.config.pollTimeoutMs, e.config.configMap
}

func (e *Extractor) DLQConfig() DLQConfig {
	return e.config.dlqConfig
}
