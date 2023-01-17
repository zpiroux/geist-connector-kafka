package gkafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
	"github.com/zpiroux/geist/entity"
)

// gkafka_ext_test.go tests connector functionality from an external perspective,
// without relying on anything not exported externally.

// TestGeistIntegration tests Geist's generic connector integration API.
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
		assert.Equal(t, MockSpecID2, streamId)
		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.NoError(t, err)
}

// TestDLQE2E creates a full real Geist with the public Geist Kafka connector factory
// and sends a number of events (generated by the injected mock Kafka consumer) via three
// concurrent stream instances. All events are deemed unretryable (from the Void sink
// config) and is thus sent to the DLQ, matching with the number of sent events.
func TestDLQE2E(t *testing.T) {

	ctx := context.Background()
	geistConfig := geist.NewConfig()
	geistConfig.Ops.Log = true
	kConfig := &Config{
		PollTimeoutMs: 250,
	}

	// Spec creation here is only needed to find out number of streams per pod
	spec, err := entity.NewSpec(kafkaToVoidStreamDLQE2E)
	require.NoError(t, err)
	eventsToSimulate := int64(3)

	consumerFactory := NewMockConsumerFactory(eventsToSimulate)
	dlqProducerFactory := &MockDlqProducerFactory{}

	ef := CreateExtractorFactory(kConfig, consumerFactory, dlqProducerFactory)

	err = geistConfig.RegisterExtractorType(ef)
	require.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	assert.NoError(t, err)

	go func() {
		streamId, err := geist.RegisterStream(ctx, kafkaToVoidStreamDLQE2E)
		assert.NoError(t, err)
		assert.Equal(t, "geisttest-dlq-e2e", streamId)
		time.Sleep(5 * time.Second) // TODO: Change this to proper sync
		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int64(spec.Ops.StreamsPerPod)*eventsToSimulate, eventCounter.ProducedDLQEvents.Load())
}

func CreateExtractorFactory(config *Config, cf ikafka.ConsumerFactory, pf ikafka.ProducerFactory) entity.ExtractorFactory {
	return NewExtractorFactory(config, cf, pf)
}
