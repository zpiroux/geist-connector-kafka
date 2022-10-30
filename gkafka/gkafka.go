package gkafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/zpiroux/geist-connector-kafka/gkafka/internal/gki"
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

// Config is the external config provided by the geist client to the factory when
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
}

func NewExtractorFactory(config *Config) entity.ExtractorFactory {
	return &extractorFactory{
		config: config,
	}
}

func (ef *extractorFactory) SourceId() string {
	return entityTypeId
}

func (ef *extractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	cfg, err := ef.createKafkaExtractorConfig(c)
	if err != nil {
		return nil, err
	}
	return gki.NewExtractor(cfg)
}

func (ef *extractorFactory) createKafkaExtractorConfig(c entity.Config) (*gki.Config, error) {

	var err error

	ec := gki.NewExtractorConfig(
		c,
		ef.topicNamesFromSpec(c.Spec.Source.Config.Topics),
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

	// Add all props from GEIST stream spec (will override previously set ones).
	// Apply Geist-specific assignments.
	for _, prop := range c.Spec.Source.Config.Properties {
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

	if c.Spec.Source.Config.PollTimeoutMs != nil {
		ec.SetPollTimout(*c.Spec.Source.Config.PollTimeoutMs)
	} else {
		ec.SetPollTimout(ef.config.PollTimeoutMs)
	}

	if c.Spec.Source.Config.SendToSource != nil {
		ec.SetSendToSource(*c.Spec.Source.Config.SendToSource)
	} else {
		ec.SetSendToSource(ef.config.SendToSource)
	}

	// This is currently not possible to override in stream specs
	ec.SetCreateTopics(ef.config.CreateTopics)

	return ec, err
}

func uniqueGroupID(groupIDSpec, extractorID, tsLayout string) string {
	str := strings.TrimPrefix(groupIDSpec, UniqueGroupIDWithPrefix) + "-" + extractorID + "-" + time.Now().UTC().Format(tsLayout)
	return strings.TrimPrefix(str, ".")
}

func (ef *extractorFactory) topicNamesFromSpec(topicsInSpec []entity.Topics) []string {
	var topicNames []string
	for _, topics := range topicsInSpec {
		if topics.Env == entity.EnvironmentAll {
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

	var sync bool
	if c.Spec.Sink.Config == nil {
		return nil, errors.New("no sink config provided")
	}
	if c.Spec.Sink.Config.Synchronous != nil {
		sync = *c.Spec.Sink.Config.Synchronous
	}

	lc := gki.NewLoaderConfig(
		c,
		lf.topicSpecFromSpec(c.Spec.Sink.Config.Topic),
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

	// Add all props from GEIST spec (will override previously set ones)
	for _, prop := range c.Spec.Sink.Config.Properties {
		props[prop.Key] = prop.Value
	}

	lc.SetLoaderProps(props)

	// This is currently not possible to override in stream specs
	lc.SetCreateTopics(lf.config.CreateTopics)

	return lc, nil
}

func (s *loaderFactory) topicSpecFromSpec(topicsInSpec []entity.SinkTopic) *entity.TopicSpecification {
	var topicSpec *entity.TopicSpecification
	for _, topic := range topicsInSpec {
		if topic.Env == entity.EnvironmentAll {
			topicSpec = topic.TopicSpec
			break
		}
		if string(topic.Env) == s.config.Env {
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
