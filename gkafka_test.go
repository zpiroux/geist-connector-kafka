package gkafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist/entity"
)

func TestGeistIntegration(t *testing.T) {

	ctx := context.Background()
	geistConfig := geist.NewConfig()
	kConfig := &Config{}

	err := geistConfig.RegisterLoaderType(NewMockLoaderFactory(ctx, kConfig))
	assert.NoError(t, err)
	err = geistConfig.RegisterExtractorType(NewMockExtractorFactory(ctx, kConfig))
	assert.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	assert.NoError(t, err)

	assert.True(t, geist.Entities()["loader"]["kafka"])
	assert.True(t, geist.Entities()["extractor"]["kafka"])
	assert.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, kafkaToKafkaDevOnly)
		assert.NoError(t, err)
		assert.Equal(t, "geisttest-mock-1", streamId)
		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.NoError(t, err)
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

func (mef *MockExtractorFactory) NewExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return &dummyExtractor{}, nil
}

func (mef *MockExtractorFactory) Close() error {
	return nil
}

// Proper testing for actual Extractor logic is done in internal/extractor_test.go
type dummyExtractor struct{}

func (d *dummyExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {
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

func (mlf *MockLoaderFactory) NewLoader(ctx context.Context, spec *entity.Spec, id string) (entity.Loader, error) {
	return &dummyLoader{}, nil
}

func (mlf *MockLoaderFactory) NewSinkExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return &dummyExtractor{}, nil
}

func (mlf *MockLoaderFactory) Close() error {
	return nil
}

// Proper testing for actual Loader logic is done in internal/loader_test.go
type dummyLoader struct{}

func (d *dummyLoader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	return "", nil, false
}
func (d *dummyLoader) Shutdown() {}

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

// TODO: Add test for validating Kafka config conversions from ext to internal

func TestTopicNamesFromSpec(t *testing.T) {

	ef := NewExtractorFactory(&Config{Env: envs[envDev]})
	kef := ef.(*extractorFactory)

	spec, err := entity.NewSpec(kafkaToVoidStreamSplitEnv)
	assert.NoError(t, err)
	topics := kef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events.dev"})

	kef.config.Env = envs[envStage]
	topics = kef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events.stage"})

	kef.config.Env = envs[envProd]
	topics = kef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events"})

	spec, err = entity.NewSpec(kafkaToVoidStreamCommonEnv)
	assert.NoError(t, err)
	for _, env := range envs {
		kef.config.Env = env
		topics = kef.topicNamesFromSpec(spec.Source.Config.Topics)
		assert.Equal(t, topics, []string{"foo.events", "bar.events"})
	}

	// Test handling of missing envs
	spec, err = entity.NewSpec(kafkaToKafkaDevOnly)
	assert.NoError(t, err)
	kef.config.Env = envs[envProd]
	topics = kef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Empty(t, topics)

	lf := NewLoaderFactory(&Config{Env: envs[envProd]})
	klf := lf.(*loaderFactory)
	topicSpec := klf.topicSpecFromSpec(spec.Sink.Config.Topic)
	assert.Nil(t, topicSpec)
}

var (
	kafkaToVoidStreamSplitEnv = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-1",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
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
               "value": "geisttest-mock-1"
            }
         ]
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)
	kafkaToKafkaDevOnly = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-1",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
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
               "value": "geisttest-mock-1"
            }
         ]
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
`)
	kafkaToVoidStreamCommonEnv = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-2",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
         "topics": [
            {
               "env": "all",
               "names": [
                  "foo.events",
                  "bar.events"
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
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)
)
