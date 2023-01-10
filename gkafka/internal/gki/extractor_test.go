package gki

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist/entity"
)

const DLQTopicName = "my.dlq.topic"

var (
	eventsToConsume = 3
	eventCount      = 0
	allHoueModes    = []string{entity.HoueDefault, entity.HoueDiscard, entity.HoueDlq, entity.HoueFail}
)

func TestMicroBatchTimeout(t *testing.T) {

	mbTimeoutMs := 5000
	now := time.Now()
	timedOut := microBatchTimedOut(now, mbTimeoutMs)
	assert.False(t, timedOut)

	start := time.Now()
	time.Sleep(7 * time.Second)
	timedOut = microBatchTimedOut(start, mbTimeoutMs)
	assert.True(t, timedOut)
}

func handleNotificationEvents(notifyChan entity.NotifyChan) {
	for event := range notifyChan {
		fmt.Printf("%+v\n", event)
	}
}

func TestExtractor(t *testing.T) {

	var (
		err       error
		retryable bool
	)
	spec := GetMockSpec()
	spec.StreamIdSuffix = "happy-path"
	spec.Ops.HandlingOfUnretryableEvents = entity.HoueDefault
	extractor, err := createMockExtractor(false, nil, spec)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		extractor.StreamExtract(
			ctx,
			reportEvent,
			&err,
			&retryable)
		wg.Done()
	}()

	wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, eventsToConsume, eventCount)
	assert.Equal(t, eventCount, int(extractor.eventCount+1))
	assert.Equal(t, "value1", (*(extractor.consumer.(*MockConsumer)).conf)["prop1"])
	assert.Equal(t, "value2", (*(extractor.consumer.(*MockConsumer)).conf)["prop2"])
	assert.Equal(t, "value3", (*(extractor.consumer.(*MockConsumer)).conf)["prop3"])
	cancel()
}

func TestRetryableFailure(t *testing.T) {

	var retryable bool
	dlqConfig := DLQConfig{Topic: &entity.TopicSpecification{
		Name:              DLQTopicName,
		NumPartitions:     1,
		ReplicationFactor: 3}}

	for _, houeMode := range allHoueModes {
		eventCount = 0
		spec := GetMockSpec()
		spec.StreamIdSuffix = "retryable-failure"
		spec.Ops.HandlingOfUnretryableEvents = houeMode
		extractor, err := createMockExtractor(true, &dlqConfig, spec)
		assert.NoError(t, err)

		extractor.StreamExtract(
			context.Background(),
			reportEventWithRetryableFailure,
			&err,
			&retryable)

		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), ErrRetriesExhausted))
		assert.True(t, retryable)

		if houeMode == entity.HoueDlq {
			assert.NotNil(t, extractor.dlqProducer)
			expectedCreatedTopic := kafka.TopicSpecification{
				Topic:             DLQTopicName,
				NumPartitions:     1,
				ReplicationFactor: 3,
			}
			assert.Equal(t, expectedCreatedTopic, extractor.ac.(*MockAdminClient).LastCreatedTopic)
		} else {
			assert.Nil(t, extractor.dlqProducer)
		}
	}
}

func TestUnretryableFailure(t *testing.T) {

	var retryable bool
	dlqConfig := DLQConfig{Topic: &entity.TopicSpecification{Name: DLQTopicName}}

	for _, houeMode := range allHoueModes {
		eventCount = 0
		spec := GetMockSpec()
		spec.StreamIdSuffix = "unretryable-failure"
		spec.Ops.HandlingOfUnretryableEvents = houeMode
		extractor, err := createMockExtractor(false, &dlqConfig, spec)
		assert.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			extractor.StreamExtract(
				ctx,
				reportEventWithUnretryableFailure,
				&err,
				&retryable)
			wg.Done()
		}()

		time.Sleep(time.Duration(3) * time.Second)
		cancel()
		wg.Wait()

		if houeMode == entity.HoueFail {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		assert.False(t, retryable)

		if houeMode == entity.HoueDlq {
			assert.NotNil(t, extractor.dlqProducer)
			assert.Empty(t, extractor.ac.(*MockAdminClient).LastCreatedTopic)
		} else {
			assert.Nil(t, extractor.dlqProducer)
		}

	}
}

var (
	nbPublishToFail   int
	nbPublishRequests int
)

func TestMoveToDLQ(t *testing.T) {

	var (
		cf           MockConsumerFactory
		ctx          = context.Background()
		err          error
		a            action
		dlqTopicName = DLQTopicName
	)

	notifyChan := make(entity.NotifyChan, 128)
	go handleNotificationEvents(notifyChan)

	spec := GetMockSpec()
	spec.Ops.HandlingOfUnretryableEvents = entity.HoueDlq
	config := NewExtractorConfig(entity.Config{Spec: spec, ID: "mockInstanceId", NotifyChan: notifyChan}, []string{"coolTopic"}, &sync.Mutex{})

	// Also including validation of stream ID enrichment
	config.SetDLQConfig(DLQConfig{
		Topic:                  &entity.TopicSpecification{Name: dlqTopicName},
		StreamIDEnrichmentPath: "_myenrichment.streamId",
	})
	config.SetPollTimout(2000)
	extractor, err := NewExtractor(config)
	assert.NoError(t, err)
	extractor.SetConsumerFactory(cf)

	msg := &kafka.Message{
		Value: []byte(`{"some":"value"}`),
	}

	expectedMsginDLQ := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &dlqTopicName},
		Value:          []byte(`{"some":"value","_myenrichment":{"streamId":"extractor-mockspec"}}`),
	}

	nbPublishToFail = 0
	nbPublishRequests = 0
	extractor.SetProducerFactory(MockDlqProducerFactory{})
	err = extractor.initStreamExtract(ctx)
	assert.NoError(t, err)
	a, err = extractor.moveEventToDLQ(ctx, msg)
	if assert.NoError(t, err) {
		assert.Equal(t, actionContinue, a)
		assert.Equal(t, nbPublishToFail+1, nbPublishRequests)
	}
	assert.Equal(t, expectedMsginDLQ.Value, extractor.dlqProducer.(*MockDlqProducer).lastSuccessfulEvent.Value)
	assert.Equal(t, expectedMsginDLQ.TopicPartition.Topic, extractor.dlqProducer.(*MockDlqProducer).lastSuccessfulEvent.TopicPartition.Topic)

	nbPublishToFail = 1
	nbPublishRequests = 0
	extractor.dlqProducer.(*MockDlqProducer).nbFailedPublishReported = 0
	a, err = extractor.moveEventToDLQ(ctx, msg)
	if assert.NoError(t, err) {
		assert.Equal(t, actionContinue, a)
		assert.Equal(t, nbPublishToFail+1, nbPublishRequests)
	}

	nbPublishToFail = 3
	nbPublishRequests = 0
	extractor.dlqProducer.(*MockDlqProducer).nbFailedPublishReported = 0
	a, err = extractor.moveEventToDLQ(ctx, msg)
	if assert.NoError(t, err) {
		assert.Equal(t, actionContinue, a)
		assert.Equal(t, nbPublishToFail+1, nbPublishRequests)
	}
}

func TestSendToSource(t *testing.T) {

	nbPublishToFail = 0
	ctx := context.Background()
	spec := GetMockSpec()
	spec.StreamIdSuffix = "send-to-source"
	extractor, err := createMockExtractor(false, nil, spec)
	extractor.config.SetSendToSource(true)
	assert.NoError(t, err)
	err = extractor.initStreamExtract(ctx)
	assert.NoError(t, err)

	event := "Hi there!"
	_, err = extractor.SendToSource(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, event, string(extractor.sourceProducer.(*MockDlqProducer).lastSuccessfulEvent.Value))

	eventBytes := []byte("Hi again!")
	_, err = extractor.SendToSource(ctx, eventBytes)
	assert.NoError(t, err)
	assert.Equal(t, string(eventBytes), string(extractor.sourceProducer.(*MockDlqProducer).lastSuccessfulEvent.Value))
}

func createMockExtractor(createTopics bool, dlqConfig *DLQConfig, spec *entity.Spec) (*Extractor, error) {
	notifyChan := make(entity.NotifyChan, 128)
	go handleNotificationEvents(notifyChan)

	config := NewExtractorConfig(entity.Config{Spec: spec, ID: "mockInstanceId", NotifyChan: notifyChan}, []string{"coolTopic"}, &sync.Mutex{})

	config.createTopics = createTopics
	config.SetPollTimout(2000)
	config.SetProps(ConfigMap{
		"prop1": "value1",
	})
	config.SetKafkaProperty("prop2", "value2")
	config.SetProps(ConfigMap{
		"prop3": "value3",
	})

	if dlqConfig != nil {
		config.SetDLQConfig(*dlqConfig)
	}

	extractor, err := NewExtractor(config)
	extractor.SetConsumerFactory(MockConsumerFactory{})
	extractor.SetProducerFactory(MockDlqProducerFactory{})
	return extractor, err
}

type MockConsumer struct {
	conf *kafka.ConfigMap
}

func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (m *MockConsumer) Poll(timeoutMs int) kafka.Event {
	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
	eventCount += 1
	return &kafka.Message{
		Value:     []byte("foo " + strconv.Itoa(eventCount)),
		Timestamp: time.Now().UTC(),
	}
}
func (m *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) Close() error {
	return nil
}

type MockConsumerFactory struct{}

func (mcf MockConsumerFactory) NewConsumer(conf *kafka.ConfigMap) (Consumer, error) {
	return &MockConsumer{conf: conf}, nil
}

func (mcf MockConsumerFactory) NewAdminClientFromConsumer(c Consumer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

type MockDlqProducerFactory struct{}

func (mpf MockDlqProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return NewMockDlqProducer(conf), nil
}

func (mpf MockDlqProducerFactory) CloseProducer(p Producer) {
	if !IsNil(p) {
		p.Close()
	}
}

func (mpf MockDlqProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

func reportEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	if eventCount >= eventsToConsume {
		return entity.EventProcessingResult{
			Status: entity.ExecutorStatusShutdown,
			Error:  fmt.Errorf("event processing of event %v aborted due to shutdown signal", events),
		}
	}
	return entity.EventProcessingResult{
		Status:    entity.ExecutorStatusSuccessful,
		Retryable: true,
	}
}

const ErrRetriesExhausted = "executor reached max retry limit"

func reportEventWithRetryableFailure(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	return entity.EventProcessingResult{
		Status:    entity.ExecutorStatusRetriesExhausted,
		Error:     fmt.Errorf(ErrRetriesExhausted+", for event: %+v", events),
		Retryable: true,
	}
}

const ErrUnretryableError = "executor encountered an unretryable error"

func reportEventWithUnretryableFailure(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	return entity.EventProcessingResult{
		Status:    entity.ExecutorStatusError,
		Error:     fmt.Errorf(ErrUnretryableError+", for event: %+v", events),
		Retryable: false,
	}
}

type MockAdminClient struct {
	LastCreatedTopic kafka.TopicSpecification
}

func (m *MockAdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return &kafka.Metadata{}, nil
}

func (m *MockAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	var result kafka.TopicResult
	m.LastCreatedTopic = topics[0]
	result.Topic = topics[0].Topic
	return []kafka.TopicResult{result}, nil
}

func NewMockDlqProducer(conf *kafka.ConfigMap) Producer {
	return &MockDlqProducer{
		events: make(chan kafka.Event, 10),
		conf:   conf,
	}
}

type MockDlqProducer struct {
	conf                    *kafka.ConfigMap
	events                  chan kafka.Event
	nbFailedPublishReported int
	lastSuccessfulEvent     *kafka.Message
}

func (p *MockDlqProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	nbPublishRequests++

	dChan := p.events

	if deliveryChan != nil {
		dChan = deliveryChan
	}

	if p.nbFailedPublishReported < nbPublishToFail {
		p.nbFailedPublishReported++

		switch p.nbFailedPublishReported {
		case 1:
			return fmt.Errorf("publish enqueue failed, nb failures: %d", p.nbFailedPublishReported)
		case 2:
			msg.TopicPartition.Error = fmt.Errorf("publish kafka op failed, nb failures: %d", p.nbFailedPublishReported)
			dChan <- msg
			return nil
		case 3:
			errCodeCoordinatorNotAvailable := 15 // this value is no longer exported properly from go lib
			dChan <- kafka.NewError(kafka.ErrorCode(errCodeCoordinatorNotAvailable), "bad stuff", false)
			return nil
		default:
			return fmt.Errorf("random bad thing happened, nb failures: %d", p.nbFailedPublishReported)
		}
	}
	msg.TopicPartition.Error = nil
	p.lastSuccessfulEvent = msg
	dChan <- msg
	return nil
}

func (p MockDlqProducer) Events() chan kafka.Event {
	return p.events
}

func (p MockDlqProducer) Flush(timeoutMs int) int {
	return 0
}

func (p MockDlqProducer) Close() {
	// Nothing to close
}

func GetMockSpec() *entity.Spec {

	spec := entity.NewEmptySpec()
	spec.Namespace = "extractor"
	spec.StreamIdSuffix = "mockspec"
	spec.Version = 1
	spec.Description = "..."
	return spec
}
