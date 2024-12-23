package gkafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist-connector-kafka/gkafka/internal/gki"
	"github.com/zpiroux/geist-connector-kafka/gkafka/spec"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
	"github.com/zpiroux/geist/entity"
)

const (
	MockSpecID2 = "geisttest-mock-2"
	MockSpecID3 = "geisttest-mock-3"
)

func TestConfig(t *testing.T) {
	ctx := context.Background()

	// Test Extractor with empty external config
	spec, err := entity.NewSpec(kafkaToVoidStreamCommonEnvWithDLQ)
	assert.NoError(t, err)
	extractor := createExtractor(t, ctx, spec, &Config{}, nil)
	pollTimeout, cfgMap := extractor.(*gki.Extractor).KafkaConfig()
	assert.Equal(t, gki.DefaultPollTimeoutMs, pollTimeout)
	expectedConfigMap := map[string]any{
		PropQueuedMaxMessagesKb: DefaultQueuedMaxMessagesKb,
		PropMaxPollInterval:     DefaultMaxPollInterval,
		gki.PropAutoOffsetStore: false,
		gki.PropAutoCommit:      true,
		PropGroupID:             MockSpecID3,
	}
	assert.Equal(t, expectedConfigMap, cfgMap)

	// Test Extractor with non-empty external config
	config := &Config{
		PollTimeoutMs: 27000,
		KafkaProps: map[string]any{
			PropBootstrapServers:    "mybootstrapserver",
			PropSASLUsername:        "myusername",
			PropSASLPassword:        "mypassword",
			PropSASLMechanism:       "SCRAM-SHA-512",
			PropQueuedMaxMessagesKb: 5000,
			PropMaxPollInterval:     200000,
			gki.PropAutoOffsetStore: true,
			gki.PropAutoCommit:      false,
		},
	}
	extractor = createExtractor(t, ctx, spec, config, nil)
	pollTimeout, cfgMap = extractor.(*gki.Extractor).KafkaConfig()
	assert.Equal(t, 27000, pollTimeout)
	expectedConfigMap = map[string]any{
		PropQueuedMaxMessagesKb: 5000,
		PropMaxPollInterval:     200000,
		gki.PropAutoOffsetStore: false,
		gki.PropAutoCommit:      true,
		PropGroupID:             MockSpecID3,
		PropBootstrapServers:    "mybootstrapserver",
		PropSASLUsername:        "myusername",
		PropSASLPassword:        "mypassword",
		PropSASLMechanism:       "SCRAM-SHA-512",
	}
	assert.Equal(t, expectedConfigMap, cfgMap)

	// Test Loader with empty external config
	spec, err = entity.NewSpec(kafkaToKafkaDevOnly)
	assert.NoError(t, err)
	config = &Config{
		Env: "dev",
	}
	lf := NewLoaderFactory(config)
	loader, err := lf.NewLoader(ctx, NewEntityConfig(spec))
	assert.NoError(t, err)
	expectedConfigMap = map[string]any{
		PropIdempotence:     true,
		PropCompressionType: "lz4",
		"client.id":         "geisttest_mock-1",
	}
	cfgMap = loader.(*gki.Loader).KafkaConfig()
	assert.Equal(t, expectedConfigMap, cfgMap)

	// Test Loader with non-empty external config
	config = &Config{
		Env: "dev",
		KafkaProps: map[string]any{
			PropBootstrapServers: "mybootstrapserver",
			PropSASLUsername:     "myusername",
			PropSASLPassword:     "mypassword",
			PropSASLMechanism:    "PLAIN",
			PropIdempotence:      false,
			PropCompressionType:  "zstd",
		},
	}
	expectedConfigMap = map[string]any{
		PropBootstrapServers: "mybootstrapserver",
		PropSASLUsername:     "myusername",
		PropSASLPassword:     "mypassword",
		PropSASLMechanism:    "PLAIN",
		PropIdempotence:      false,
		PropCompressionType:  "zstd",
		"client.id":          "geisttest_mock-1",
	}
	lf = NewLoaderFactory(config)
	loader, err = lf.NewLoader(ctx, NewEntityConfig(spec))
	assert.NoError(t, err)
	cfgMap = loader.(*gki.Loader).KafkaConfig()
	assert.Equal(t, expectedConfigMap, cfgMap)
}

func TestDLQConfig(t *testing.T) {
	ctx := context.Background()

	// Validate correct DLQ config created with empty external config
	streamSpec, err := entity.NewSpec(kafkaToVoidStreamCommonEnvWithDLQ)
	require.NoError(t, err)

	extractor := createExtractor(t, ctx, streamSpec, &Config{}, nil)
	dlqConfig := extractor.(*gki.Extractor).DLQConfig()
	require.NotNil(t, dlqConfig.Topic)
	assert.Equal(t, "_myservice.metadata.streamId", dlqConfig.StreamIDEnrichmentPath)
	expectedDLQTopic := spec.TopicSpecification{
		Name:              "my.dlq.topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	assert.Equal(t, &expectedDLQTopic, dlqConfig.Topic)

	// Validate creating DLQ topic in custom region
	streamSpec, err = entity.NewSpec(kafkaToVoidStreamCustomEnvWithDLQ)
	require.NoError(t, err)
	extractor = createExtractor(t, ctx, streamSpec, &Config{Env: "my-custom-env", CreateTopics: true}, nil)
	dlqConfig = extractor.(*gki.Extractor).DLQConfig()
	require.NotNil(t, dlqConfig.Topic)
	assert.Equal(t, "_myservice.metadata.streamId", dlqConfig.StreamIDEnrichmentPath)
	expectedDLQTopic = spec.TopicSpecification{
		Name:              "my.dlq.topic",
		NumPartitions:     24,
		ReplicationFactor: 6,
	}
	assert.Equal(t, &expectedDLQTopic, dlqConfig.Topic)

	// Validate config of fully created DLQ producer
	pf := &MockDlqProducerFactory{}
	extractor = createExtractor(t, ctx, streamSpec, &Config{Env: "my-custom-env", CreateTopics: false}, pf)
	var retryable bool
	ctx, cancel := context.WithCancel(ctx)
	cancel() // We use a closed ctx to return early from StreamExtract
	extractor.StreamExtract(ctx, nil, &err, &retryable)
	assert.NoError(t, err)
	var pconf = []byte(`
	{
	  "queue.buffering.max.messages":  "100",
	  "queue.buffering.max.kbytes":    "12345",
	  "compression.type":              "lz4",
	  "enable.idempotence":            true
	}`)

	var expectedProducerConfig kafka.ConfigMap
	err = json.Unmarshal(pconf, &expectedProducerConfig)
	require.NoError(t, err)
	assertMapEqual(t, expectedProducerConfig, *pf.Producer.Conf)
}

func createExtractor(t *testing.T, ctx context.Context, spec *entity.Spec, config *Config, pf ikafka.ProducerFactory) entity.Extractor {
	ef := NewExtractorFactory(config, nil, pf)
	extractor, err := ef.NewExtractor(ctx, NewEntityConfig(spec))
	assert.NoError(t, err)
	return extractor
}

func assertMapEqual(t *testing.T, m1, m2 kafka.ConfigMap) {
	for k, v := range m1 {
		assert.Equal(t, v, m2[k])
	}
}

func TestMissingGroupID(t *testing.T) {
	ctx := context.Background()
	spec, err := entity.NewSpec(kafkaToVoidMissingGroupID)
	assert.NoError(t, err)
	ef := NewExtractorFactory(&Config{}, nil, nil)
	_, err = ef.NewExtractor(ctx, NewEntityConfig(spec))
	assert.True(t, errors.Is(err, ErrMissingGroupID))
}

func TestUniqueGroupID(t *testing.T) {

	tsLayout := "2006-01-02T15.04.05"

	groupId := uniqueGroupID(UniqueGroupIDWithPrefix+".foo", "extrid", tsLayout)
	assert.Equal(t, "foo-extrid-"+time.Now().UTC().Format(tsLayout), groupId)

	groupId = uniqueGroupID(UniqueGroupIDWithPrefix+"", "extrid", tsLayout)
	assert.Equal(t, "-extrid-"+time.Now().UTC().Format(tsLayout), groupId)

	groupId = uniqueGroupID(UniqueGroupIDWithPrefix+".foo.bar", "extrid", tsLayout)
	assert.Equal(t, "foo.bar-extrid-"+time.Now().UTC().Format(tsLayout), groupId)

	ctx := context.Background()
	spec, err := entity.NewSpec(kafkaToVoidStreamSplitEnv)
	assert.NoError(t, err)
	extractor := createExtractor(t, ctx, spec, &Config{Env: envs[envProd]}, nil)
	pollTimeout, cfgMap := extractor.(*gki.Extractor).KafkaConfig()
	assert.Equal(t, gki.DefaultPollTimeoutMs, pollTimeout)
	assert.True(t, strings.Contains(cfgMap[PropGroupID].(string), "my-groupid-prefix-some-ID-"+time.Now().UTC().Format(tsLayout)), "generated groupId: %s", cfgMap[PropGroupID])
}

type envType int

// Although the test here uses geist pre-provided stage names,
// any custom name is supported as string, matching the env given to the factory
// with the env specified in the stream spec.
const (
	envDev envType = iota
	envStage
	envProd
)

var envs = map[envType]string{
	envDev:   string(entity.EnvironmentDev),
	envStage: string(entity.EnvironmentStage),
	envProd:  string(entity.EnvironmentProd),
}

func TestTopicNamesFromSpec(t *testing.T) {

	ef := NewExtractorFactory(&Config{Env: envs[envDev]}, nil, nil)
	kef := ef.(*extractorFactory)

	streamSpec, err := entity.NewSpec(kafkaToVoidStreamSplitEnv)
	assert.NoError(t, err)
	sourceConfig, err := spec.NewSourceConfig(streamSpec)
	assert.NoError(t, err)
	topics := kef.topicNamesFromSpec(sourceConfig.Topics)
	assert.Equal(t, topics, []string{"foo.events.dev"})

	kef.config.Env = envs[envStage]
	topics = kef.topicNamesFromSpec(sourceConfig.Topics)
	assert.Equal(t, topics, []string{"foo.events.stage"})

	kef.config.Env = envs[envProd]
	topics = kef.topicNamesFromSpec(sourceConfig.Topics)
	assert.Equal(t, topics, []string{"foo.events"})

	streamSpec, err = entity.NewSpec(kafkaToVoidStreamCommonEnvWithDLQ)
	assert.NoError(t, err)
	sourceConfig, err = spec.NewSourceConfig(streamSpec)
	assert.NoError(t, err)
	for _, env := range envs {
		kef.config.Env = env
		topics = kef.topicNamesFromSpec(sourceConfig.Topics)
		assert.Equal(t, topics, []string{"foo.events", "bar.events"})
	}

	// Test handling of missing envs
	streamSpec, err = entity.NewSpec(kafkaToKafkaDevOnly)
	assert.NoError(t, err)
	sourceConfig, err = spec.NewSourceConfig(streamSpec)
	assert.NoError(t, err)
	kef.config.Env = envs[envProd]
	topics = kef.topicNamesFromSpec(sourceConfig.Topics)
	assert.Empty(t, topics)

	lf := NewLoaderFactory(&Config{Env: envs[envProd]})
	klf := lf.(*loaderFactory)
	sinkConfig, err := spec.NewSinkConfigFromLegacySpec(streamSpec)
	assert.NoError(t, err)
	topicSpec := topicSpecFromSpec(klf.config.Env, sinkConfig.Topic)
	assert.Nil(t, topicSpec)
}

type MockExtractorFactory struct {
	realExtractorFactory *extractorFactory
}

func NewMockExtractorFactory(ctx context.Context, config *Config) entity.ExtractorFactory {
	var mef MockExtractorFactory
	mef.realExtractorFactory = &extractorFactory{
		config: config,
	}
	return &mef
}

func (mef *MockExtractorFactory) SourceId() string {
	return mef.realExtractorFactory.SourceId()
}

func (mef *MockExtractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return &dummyExtractor{}, nil
}

func (mef *MockExtractorFactory) Close(ctx context.Context) error {
	return nil
}

// Proper testing for actual Extractor logic is done in internal/extractor_test.go
type dummyExtractor struct{}

func (d *dummyExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	// Not applicable
}

func (d *dummyExtractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}
func (d *dummyExtractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}
func (d *dummyExtractor) SendToSource(ctx context.Context, event any) (string, error) {
	return "", nil
}

type MockLoaderFactory struct {
	realLoaderFactory *loaderFactory
}

func NewMockLoaderFactory(ctx context.Context, config *Config) entity.LoaderFactory {
	var mlf MockLoaderFactory
	mlf.realLoaderFactory = &loaderFactory{
		config: config,
	}
	return &mlf
}

func (mlf *MockLoaderFactory) SinkId() string {
	return mlf.realLoaderFactory.SinkId()
}

func (mlf *MockLoaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {
	return &dummyLoader{}, nil
}

func (mlf *MockLoaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return &dummyExtractor{}, nil
}

func (mlf *MockLoaderFactory) Close(ctx context.Context) error {
	return nil
}

// Proper testing for actual Loader logic is done in internal/loader_test.go
type dummyLoader struct{}

func (d *dummyLoader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	return "", nil, false
}

func (d *dummyLoader) Shutdown(ctx context.Context) {
	// Nothing to shut down
}

var kafkaToVoidStreamSplitEnv = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-1",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "customConfig": {
            "topics": [
               {
                  "env": "dev",
                  "names": [
                     "foo.events.dev"
                  ]
               },
               {
                  "env": "stage",
                  "names": [
                     "foo.events.stage"
                  ]
               },
               {
                  "env": "prod",
                  "names": [
                     "foo.events"
                  ]
               }
            ],
            "properties": [
               {
                  "key": "group.id",
                  "value": "@UniqueWithPrefix.my-groupid-prefix"
               }
            ]
         }
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)

var kafkaToKafkaDevOnly = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-2",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "customConfig": {
            "topics": [
               {
                  "env": "dev",
                  "names": [
                     "foo.events.dev"
                  ]
               }
            ],
            "properties": [
               {
                  "key": "group.id",
                  "value": "geisttest-mock-2"
               }
            ]
         }
      }
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
         "customConfig": {
            "topic": [
               {
                  "env": "dev",
                  "topicSpec": {
                     "name": "geisttest.events.dev",
                     "numPartitions": 6,
                     "replicationFactor": 3
                  }
               }
            ],
            "properties": [
               {
                  "key": "client.id",
                  "value": "geisttest_mock-1"
               }
            ],
            "message": {
               "payloadFromId": "payload"
            }
         }
      }
   }
}
`)

var kafkaToVoidStreamCommonEnvWithDLQ = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-3",
   "version": 1,
   "description": "...",
   "ops": {
      "handlingOfUnretryableEvents": "dlq"
   },
   "source": {
      "type": "kafka",
      "config": {
         "customConfig": {
            "topics": [
               {
                  "env": "all",
                  "names": [
                     "foo.events",
                     "bar.events"
                  ]
               }
            ],
            "dlq": {
               "streamIDEnrichmentPath": "_myservice.metadata.streamId",
               "topic": [
                  {
                     "env": "all",
                     "topicSpec": {
                        "name": "my.dlq.topic"
                     }
                  }
               ]
            },
            "properties": [
               {
                  "key": "group.id",
                  "value": "geisttest-mock-3"
               }
            ]
         }
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)

var kafkaToVoidMissingGroupID = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-4",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "customConfig": {
            "topics": [
               {
                  "env": "all",
                  "names": [
                     "foo.events"
                  ]
               }
            ]
         }
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)

var kafkaToVoidStreamCustomEnvWithDLQ = []byte(`
{
    "namespace": "geisttest",
    "streamIdSuffix": "mock-5",
    "version": 1,
    "description": "...",
    "ops": {
        "handlingOfUnretryableEvents": "dlq"
    },
    "source": {
        "type": "kafka",
        "config": {
            "customConfig": {
                "topics": [
                    {
                        "env": "all",
                        "names": [
                            "foo.events",
                            "bar.events"
                        ]
                    }
                ],
                "dlq": {
                    "streamIDEnrichmentPath": "_myservice.metadata.streamId",
                    "producerConfig": {
                        "queue.buffering.max.messages": "100",
                        "queue.buffering.max.kbytes": "12345"
                    },
                    "topic": [
                        {
                            "env": "my-custom-env",
                            "topicSpec": {
                                "name": "my.dlq.topic",
                                "numPartitions": 24,
                                "replicationFactor": 6
                            }
                        }
                    ]
                },
                "properties": [
                    {
                        "key": "group.id",
                        "value": "geisttest-mock-5"
                    }
                ]
            }
        }
    },
    "transform": {},
    "sink": {
        "type": "void"
    }
}
`)

var kafkaToVoidStreamDLQE2E = []byte(`
{
    "namespace": "geisttest",
    "streamIdSuffix": "dlq-e2e",
    "version": 1,
    "description": "...",
    "ops": {
        "streamsPerPod": 3,
        "handlingOfUnretryableEvents": "dlq"
    },
    "source": {
        "type": "kafka",
        "config": {
            "customConfig": {
                "topics": [
                    {
                        "env": "all",
                        "names": [
                            "foo.events"
                        ]
                    }
                ],
                "dlq": {
                    "topic": [
                        {
                            "env": "all",
                            "topicSpec": {
                                "name": "my.dlq.topic"
                            }
                        }
                    ]
                },
                "properties": [
                    {
                        "key": "group.id",
                        "value": "geisttest-dlq-e2e"
                    }
                ]
            }
        }
    },
    "transform": {
        "extractFields": [
            {
                "fields": [
                    {
                        "id": "rawEvent",
                        "type": "string"
                    }
                ]
            }
        ]
    },
    "sink": {
        "type": "void",
        "config": {
            "properties": [
                {
                    "key": "simulateError",
                    "value": "alwaysUnretryable"
                }
            ]
        }
    }
}
`)

func NewEntityConfig(spec *entity.Spec) entity.Config {
	return entity.Config{Spec: spec, ID: "some-ID"}
}

type MockDlqProducerFactory struct {
	Producer *MockDlqProducer
}

func (mpf *MockDlqProducerFactory) NewProducer(conf *kafka.ConfigMap) (ikafka.Producer, error) {
	mpf.Producer = NewMockDlqProducer(conf)
	return mpf.Producer, nil
}

func (mpf *MockDlqProducerFactory) CloseProducer(p ikafka.Producer) {
	mpf.Producer.Close()
}

func (mpf *MockDlqProducerFactory) NewAdminClientFromProducer(p ikafka.Producer) (ikafka.AdminClient, error) {
	return &MockAdminClient{}, nil
}

func NewMockDlqProducer(conf *kafka.ConfigMap) *MockDlqProducer {
	return &MockDlqProducer{
		Conf: conf,
	}
}

type EventCounter struct {
	ProducedDLQEvents atomic.Int64
}

var eventCounter EventCounter

type MockDlqProducer struct {
	Conf *kafka.ConfigMap
}

func (p *MockDlqProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if deliveryChan != nil {
		eventCounter.ProducedDLQEvents.Add(1)
		deliveryChan <- msg
	}
	return nil
}

func (p *MockDlqProducer) Events() (ch chan kafka.Event) {
	return ch
}

func (p *MockDlqProducer) Flush(timeoutMs int) int {
	return 0
}

func (p *MockDlqProducer) Close() {
	// Nothing to close
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

func (m *MockAdminClient) Close() {
	// Nothing to close
}

type MockConsumer struct {
	conf             *kafka.ConfigMap
	eventsToSimulate int64
	eventsConsumed   int64
}

func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (m *MockConsumer) Poll(timeoutMs int) kafka.Event {
	if m.eventsConsumed == m.eventsToSimulate {
		return nil
	}
	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
	m.eventsConsumed++
	return &kafka.Message{
		Value:     []byte(`{"someField": "someData"}`),
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

type MockConsumerFactory struct {
	eventsToSimulate int64
}

func NewMockConsumerFactory(eventsToSimulate int64) *MockConsumerFactory {
	return &MockConsumerFactory{eventsToSimulate: eventsToSimulate}
}

func (mcf *MockConsumerFactory) NewConsumer(conf *kafka.ConfigMap) (ikafka.Consumer, error) {
	return &MockConsumer{conf: conf, eventsToSimulate: mcf.eventsToSimulate}, nil
}

func (mcf *MockConsumerFactory) NewAdminClientFromConsumer(c ikafka.Consumer) (ikafka.AdminClient, error) {
	return &MockAdminClient{}, nil
}
