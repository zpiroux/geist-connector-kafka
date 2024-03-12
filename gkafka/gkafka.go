package gkafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/zpiroux/geist-connector-kafka/gkafka/internal/gki"
	"github.com/zpiroux/geist-connector-kafka/gkafka/spec"
	"github.com/zpiroux/geist-connector-kafka/ikafka"
	"github.com/zpiroux/geist/entity"
)

// Errors
var (
	// ErrMissingGroupID is returned from NewExtractor() if a "group.id" value is not found
	ErrMissingGroupID = errors.New("group.id is missing in config")
)

// Kafka properties commonly used and for setting up default config.
// See https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
// for all properties that can be used for connector configuration.
const (
	// General
	PropBootstrapServers = "bootstrap.servers"
	PropSecurityProtocol = "security.protocol"
	PropSASLMechanism    = "sasl.mechanism"
	PropSASLUsername     = "sasl.username"
	PropSASLPassword     = "sasl.password"

	// Consumer
	PropGroupID             = "group.id"
	PropQueuedMaxMessagesKb = "queued.max.messages.kbytes"
	PropMaxPollInterval     = "max.poll.interval.ms"

	// Producer
	PropIdempotence     = "enable.idempotence"
	PropCompressionType = "compression.type"
)

// Special prop constructs
const (
	// If "group.id" property is assigned with this value on the format
	// "@UniqueWithPrefix.my-groupid-prefix" a unique group.id value will be generated
	// on the format "my-groupid-prefix.<unique extractor id>.<ISO UTC timestamp micros>"
	UniqueGroupIDWithPrefix = "@UniqueWithPrefix"
)

// Config is the external config provided by the Geist client to the factory when
// starting up, which is to be used during stream creations.
type Config struct {

	// KafkaProps is used to provide standard Kafka Properties to producer/consumer
	// entities. It should be filled in with required props such as "bootstrap.servers"
	// but can also be filled in with default properties common for intended streams.
	// All these properties can be overridden in each Stream Spec by using its
	// config.properties JSON object.
	KafkaProps map[string]any

	// PollTimeoutMs is the default value to use as the consumer poll timeout for
	// each Extractor's Kafka consumer. It can be overridden per stream in stream
	// specs. If not set the default value gki.DefaultPollTimeoutMs will be used.
	PollTimeoutMs int

	// Env is only required to be filled in if stream specs for this use of Geist
	// are using different topic specs for different environments, typically "dev",
	// "stage", and "prod". Any string is allowed as long as it matches the ones
	// used in the stream specs.
	Env string

	// CreateTopics specifies if the Extractor is allowed to create DLQ topics
	// (if configured in stream specs), and Loader is allowed to create specified
	// topics if they do not exists.
	CreateTopics bool

	// SendToSource specifies if the Extractor's SendToSource() method should be
	// available for use in certain stream scenarios. For example, if we have a
	// stream spec with Kafka set as source, having SendToSource set to true will
	// make it possible to send events to this stream with geist.Publish().
	// Having it disabled reduces memory footprint.
	SendToSource bool
}

// Default values if omitted in external config
const (
	// Interval increase from 5 min default to 10 min
	DefaultMaxPollInterval = 600000

	// Maximum number of kilobytes per topic+partition in the local consumer queue.
	// To not go OOM if big backlog, set this low. Default is 1 048 576 KB  = 1GB
	// per partition. A few MBs seems to give good enough throughput while keeping
	// memory requirements low.
	DefaultQueuedMaxMessagesKb = 2048
)

// Internal constants
const (
	entityTypeId          = "kafka"
	timestampLayoutMicros = "2006-01-02T15.04.05.000000Z"

	// useSharedDLQProducer is an internal feature flag specifying if the default behaviour
	// when creating DLQ producers should be to share the producer for all extractors/streams
	// instances per stream ID.
	useSharedDLQProducer = true
)

// kafkaTopicCreationMutex reduces the amount of unneeded requests for certain stream
// setup operations. If a stream is configured to operate with more than one concurrent
// instance (ops.streamsPerPod > 1), certain operations might be attempted by more than
// one of its stream entity instances (e.g. a stream's Kafka Extractors creating DLQ
// topics if requested in its spec). The mutex scope is per pod, but this is good enough
// in this case.
var kafkaTopicCreationMutex sync.Mutex

//
// Extractor
//

// extractorFactory is a singleton enabling extractors/sources to be handled as plug-ins to Geist
type extractorFactory struct {
	config *Config
	cf     ikafka.ConsumerFactory
	pf     ikafka.ProducerFactory
	pfs    map[string]*gki.SharedProducerFactory
}

// NewExtractorFactory creates a Geist source connector factory. cf and pf can normally
// be set to nil to use the default internal ones, unless special setups are needed.
func NewExtractorFactory(config *Config, cf ikafka.ConsumerFactory, pf ikafka.ProducerFactory) entity.ExtractorFactory {
	if gki.IsNil(pf) {
		pf = &gki.DefaultProducerFactory{}
	}
	return &extractorFactory{
		config: config,
		cf:     cf,
		pf:     pf,
		pfs:    make(map[string]*gki.SharedProducerFactory),
	}
}

func (ef *extractorFactory) SourceId() string {
	return entityTypeId
}

// NewExtractor is called from the same single goroutine (Supervisor) so no need to protect
// shared resource checks with a mutex.
func (ef *extractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	cfg, err := ef.createKafkaExtractorConfig(c)
	if err != nil {
		return nil, err
	}

	e, err := gki.NewExtractor(cfg, ef.cf)
	if err != nil {
		return nil, err
	}

	if useSharedDLQProducer {
		if _, found := ef.pfs[c.Spec.Id()]; !found {
			ef.pfs[c.Spec.Id()] = gki.NewSharedProducerFactory(ef.pf)
		}
		e.SetProducerFactory(ef.pfs[c.Spec.Id()])
	}
	return e, nil
}

func (ef *extractorFactory) createKafkaExtractorConfig(c entity.Config) (*gki.Config, error) {
	var (
		sourceConfig spec.SourceConfig
		err          error
	)

	if c.Spec.Source.Config.CustomConfig == nil {
		sourceConfig, err = spec.NewSourceConfigFromLegacySpec(c.Spec)
		if err != nil {
			return nil, err
		}
	} else {
		sourceConfig, err = spec.NewSourceConfig(c.Spec)
		if err != nil {
			return nil, err
		}
	}
	return ef.createExtractorConfig(c, sourceConfig)
}

func (ef *extractorFactory) createExtractorConfig(c entity.Config, sourceConfig spec.SourceConfig) (*gki.Config, error) {
	var err error
	ec := gki.NewExtractorConfig(
		c,
		ef.topicNamesFromSpec(sourceConfig.Topics),
		&kafkaTopicCreationMutex)

	// Deployment defaults - will be overridden if set in external config or stream spec
	props := gki.ConfigMap{
		PropMaxPollInterval:     DefaultMaxPollInterval,
		PropQueuedMaxMessagesKb: DefaultQueuedMaxMessagesKb,
	}

	// Add all props from provided external config (will override deployment defaults if set)
	for k, v := range ef.config.KafkaProps {
		props[k] = v
	}

	// Add all props from Geist stream spec (will override previously set ones).
	// Apply Geist-specific assignments.
	for _, prop := range sourceConfig.Properties {
		if prop.Key == PropGroupID && strings.Contains(prop.Value, UniqueGroupIDWithPrefix) {
			prop.Value = uniqueGroupID(prop.Value, c.ID, timestampLayoutMicros)
		}
		props[prop.Key] = prop.Value
	}

	groupId, ok := props[PropGroupID]
	if !ok || groupId == "" {
		return ec, ErrMissingGroupID
	}

	ec.SetProps(props)

	if sourceConfig.PollTimeoutMs != nil {
		ec.SetPollTimout(*sourceConfig.PollTimeoutMs)
	} else {
		ec.SetPollTimout(ef.config.PollTimeoutMs)
	}

	if sourceConfig.SendToSource != nil {
		ec.SetSendToSource(*sourceConfig.SendToSource)
	} else {
		ec.SetSendToSource(ef.config.SendToSource)
	}

	// This is currently not possible to override in stream specs
	ec.SetCreateTopics(ef.config.CreateTopics)

	if dlqEnabled(c.Spec) {
		dlqConfig, err := gki.NewDLQConfig(
			sourceConfig.DLQ.ProducerConfig,
			sourceConfig.DLQ.StreamIDEnrichmentPath,
			getDLQTopicFromSpec(ef.config.Env, sourceConfig))
		if err != nil {
			return ec, err
		}
		ec.SetDLQConfig(dlqConfig)
	}

	return ec, err
}

func dlqEnabled(spec *entity.Spec) bool {
	return spec.Ops.HandlingOfUnretryableEvents == entity.HoueDlq
}

func getDLQTopicFromSpec(env string, sourceConfig spec.SourceConfig) *spec.TopicSpecification {
	if sourceConfig.DLQ == nil {
		return nil
	}
	return topicSpecFromSpec(env, sourceConfig.DLQ.Topic)
}

func uniqueGroupID(groupIDSpec, extractorID, tsLayout string) string {
	str := strings.TrimPrefix(groupIDSpec, UniqueGroupIDWithPrefix) + "-" + extractorID + "-" + time.Now().UTC().Format(tsLayout)
	return strings.TrimPrefix(str, ".")
}

func (ef *extractorFactory) topicNamesFromSpec(topicsInSpec []spec.Topics) []string {
	var topicNames []string
	for _, topics := range topicsInSpec {
		if topics.Env == string(entity.EnvironmentAll) {
			topicNames = topics.Names
			break
		}
		if string(topics.Env) == ef.config.Env {
			topicNames = topics.Names
		}
	}
	return topicNames
}

func (lf *extractorFactory) Close(ctx context.Context) error {
	return nil
}

//
// Loader
//

// LoaderFactory is a singleton enabling loaders/sinks to be handled as plug-ins to Geist
type loaderFactory struct {
	config *Config
}

func NewLoaderFactory(config *Config) entity.LoaderFactory {
	return &loaderFactory{
		config: config,
	}
}

func (lf *loaderFactory) SinkId() string {
	return entityTypeId
}

func (lf *loaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {

	config, err := lf.createKafkaLoaderConfig(c)
	if err != nil {
		return nil, err
	}

	return gki.NewLoader(ctx, config, nil)
}

func (lf *loaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return nil, nil
}

func (lf *loaderFactory) createKafkaLoaderConfig(c entity.Config) (*gki.Config, error) {
	var (
		sinkConfig spec.SinkConfig
		err        error
	)

	if c.Spec.Sink.Config.CustomConfig == nil {
		sinkConfig, err = spec.NewSinkConfigFromLegacySpec(c.Spec)
		if err != nil {
			return nil, err
		}
	} else {
		sinkConfig, err = spec.NewSinkConfig(c.Spec)
		if err != nil {
			return nil, err
		}
	}
	return lf.createLoaderConfig(c, sinkConfig)
}

func (lf *loaderFactory) createLoaderConfig(c entity.Config, sinkConfig spec.SinkConfig) (*gki.Config, error) {
	var sync bool

	if c.Spec.Sink.Config == nil {
		return nil, errors.New("no sink config provided")
	}
	if sinkConfig.Synchronous != nil {
		sync = *sinkConfig.Synchronous
	}

	lc := gki.NewLoaderConfig(
		c,
		topicSpecFromSpec(lf.config.Env, sinkConfig.Topic),
		sinkConfig.Message,
		&kafkaTopicCreationMutex,
		sync)

	// Deployment defaults - will be overridden if set in external config or stream spec
	props := gki.ConfigMap{
		PropIdempotence:     true,
		PropCompressionType: "lz4",
	}

	// Add all props from provided external config (will override deployment defaults if set)
	for k, v := range lf.config.KafkaProps {
		props[k] = v
	}

	// Add all props from Geist spec (will override previously set ones)
	for _, prop := range sinkConfig.Properties {
		props[prop.Key] = prop.Value
	}

	lc.SetLoaderProps(props)

	// This is currently not possible to override in stream specs
	lc.SetCreateTopics(lf.config.CreateTopics)

	return lc, nil
}

func topicSpecFromSpec(env string, topicsInSpec []spec.SinkTopic) *spec.TopicSpecification {
	var topicSpec *spec.TopicSpecification
	for _, topic := range topicsInSpec {
		if topic.Env == string(entity.EnvironmentAll) {
			topicSpec = topic.TopicSpec
			break
		}
		if string(topic.Env) == env {
			topicSpec = topic.TopicSpec
		}
	}
	if topicSpec != nil {
		if topicSpec.NumPartitions == 0 {
			topicSpec.NumPartitions = 1
		}
		if topicSpec.ReplicationFactor == 0 {
			topicSpec.ReplicationFactor = 1
		}
	}
	return topicSpec
}

func (lf *loaderFactory) Close(ctx context.Context) error {
	return nil
}
