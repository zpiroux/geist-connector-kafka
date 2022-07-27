package gkafka

import (
	"context"
	"sync"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist-connector-kafka/internal"
)

const entityTypeId = "kafka"

const (
	kafkaProviderConfluent = "confluent"
	kafkaProviderNative    = "native"
)

// Config is the external config provided by the geist client to the factory when starting up,
// which is to be used during stream creations.
type Config struct {
	BootstrapServers         string // Default servers, can be overriden in GEIST specs
	ConfluentBootstrapServer string // Default server, can be overriden in GEIST specs
	ConfluentApiKey          string
	ConfluentApiSecret       string `json:"-"`
	PollTimeoutMs            int    // Default poll timeout, can be overridden in GEIST specs
	QueuedMaxMessagesKb      int    // Events consumed and processed before commit

	// Env is only required to be filled in if stream specs for this use of Geist are using different
	// topic specs for different environments, typically "dev", "stage", and "prod".
	// Any string is allowed as long as it matches the ones used in the stream specs.
	Env string
}

// Convenience function for easy default config
func (k Config) DefaultBootstrapServers(provider string) string {
	if provider == kafkaProviderConfluent {
		return k.ConfluentBootstrapServer
	}
	return k.BootstrapServers
}

// kafkaTopicCreationMutex reduces the amount of unneeded requests for certain stream setup operations.
// If a stream is configured to operate with more than one concurrent instance (ops.streamsPerPod > 1),
// certain operations might be attempted by more than one of its stream entity instances (e.g. a stream's
// Kafka Extractors creating DLQ topics if requested in its spec).
// The mutex scope is per pod, but this is good enough in this case.
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

func (ef *extractorFactory) NewExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return gki.NewExtractor(ef.createKafkaExtractorConfig(spec), id)
}

func (ef *extractorFactory) createKafkaExtractorConfig(spec *entity.Spec) *gki.Config {

	c := gki.NewExtractorConfig(
		spec,
		ef.topicNamesFromSpec(spec.Source.Config.Topics),
		&kafkaTopicCreationMutex)

	// Deployment defaults
	props := gki.ConfigMap{
		"bootstrap.servers":        ef.config.DefaultBootstrapServers(spec.Source.Config.Provider),
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,
		"max.poll.interval.ms":     600000, // increase from 5 min default to 10 min

		// Maximum number of kilobytes per topic+partition in the local consumer queue.
		// To not go OOM if big backlog, set this low. Default is 1 048 576 KB  = 1GB per partition!
		// A few MBs seems to give good enough throughput while keeping memory requirements low.
		"queued.max.messages.kbytes": ef.config.QueuedMaxMessagesKb,

		// Possibly add fetch.message.max.bytes as well. Default is 1 048 576.
		// Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.
		// "fetch.message.max.bytes": 1048576,
	}

	if spec.Source.Config.Provider == kafkaProviderConfluent {
		props["bootstrap.servers"] = ef.config.ConfluentBootstrapServer
		props["security.protocol"] = "SASL_SSL"
		props["sasl.mechanisms"] = "PLAIN"
		props["sasl.username"] = ef.config.ConfluentApiKey
		props["sasl.password"] = ef.config.ConfluentApiSecret
	}

	// Add all props from GEIST spec (could override deployment defaults)
	for _, prop := range spec.Source.Config.Properties {
		props[prop.Key] = prop.Value
	}

	c.SetProps(props)

	if spec.Source.Config.PollTimeoutMs != nil {
		c.SetPollTimout(*spec.Source.Config.PollTimeoutMs)
	} else {
		c.SetPollTimout(ef.config.PollTimeoutMs)
	}
	return c
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

func (lf *extractorFactory) Close() error {
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

func (lf *loaderFactory) NewLoader(ctx context.Context, spec *entity.Spec, id string) (entity.Loader, error) {
	return gki.NewLoader(ctx, lf.createKafkaLoaderConfig(spec), id, nil)
}

func (lf *loaderFactory) NewSinkExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return nil, nil
}

func (lf *loaderFactory) createKafkaLoaderConfig(spec *entity.Spec) *gki.Config {

	var sync bool
	if spec.Sink.Config.Synchronous != nil {
		sync = *spec.Sink.Config.Synchronous
	}

	c := gki.NewLoaderConfig(
		spec,
		lf.topicSpecFromSpec(spec.Sink.Config.Topic),
		&kafkaTopicCreationMutex,
		sync)

	// Deployment defaults
	props := gki.ConfigMap{
		"bootstrap.servers":                     lf.config.DefaultBootstrapServers(spec.Sink.Config.Provider),
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"compression.type":                      "lz4",
		"auto.offset.reset":                     "earliest",
	}

	if spec.Sink.Config.Provider == kafkaProviderConfluent {
		props["bootstrap.servers"] = lf.config.ConfluentBootstrapServer
		props["security.protocol"] = "SASL_SSL"
		props["sasl.mechanisms"] = "PLAIN"
		props["sasl.username"] = lf.config.ConfluentApiKey
		props["sasl.password"] = lf.config.ConfluentApiSecret
	}

	// Add all props from GEIST spec (could override deployment defaults)
	for _, prop := range spec.Sink.Config.Properties {
		props[prop.Key] = prop.Value
	}

	c.SetProps(props)
	return c
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

func (lf *loaderFactory) Close() error {
	return nil
}