// Package gki contains internal Kafka connector implementations.
package gki

import (
	"fmt"
	"sync"

	"github.com/zpiroux/geist-connector-kafka/gkafka/spec"
	"github.com/zpiroux/geist/entity"
)

type ConfigMap map[string]any

// Config is the internal config used by each extractor/loader, combining config
// from external Config with config from what is inside the stream spec for this
// specific stream
type Config struct {
	c                    entity.Config
	topics               []string                 // list of topics to consume from by Extractor
	sinkTopic            *spec.TopicSpecification // topic to (create and) publish to by Loader
	message              *spec.Message            // message spec
	pollTimeoutMs        int                      // timeoutMs in Consumer Poll function
	configMap            ConfigMap                // supports all possible Kafka consumer properties
	topicCreationMutex   *sync.Mutex
	synchronous          bool
	createTopics         bool
	sendToSourceEnabled  bool
	dlqConfig            DLQConfig
	schemaRegistryConfig *spec.SchemaRegistryConfig
}

func (c *Config) String() string {
	return fmt.Sprintf("topics: %v, pollTimeoutMs: %d, synchronous: %v, createTopics: %v, sendToSourceEnabled: %v props: %+v, schemaRegistry: %+v",
		c.topics, c.pollTimeoutMs, c.synchronous, c.createTopics, c.sendToSourceEnabled, displayConfig(c.configMap), c.schemaRegistryConfig)
}

func NewExtractorConfig(
	c entity.Config,
	topics []string,
	topicCreationMutex *sync.Mutex) *Config {
	return &Config{
		c:                  c,
		topics:             topics,
		configMap:          make(ConfigMap),
		topicCreationMutex: topicCreationMutex,
	}
}

func NewLoaderConfig(
	c entity.Config,
	topic *spec.TopicSpecification,
	message *spec.Message,
	topicCreationMutex *sync.Mutex,
	sync bool) *Config {
	return &Config{
		c:                  c,
		sinkTopic:          topic,
		message:            message,
		configMap:          make(ConfigMap),
		topicCreationMutex: topicCreationMutex,
		synchronous:        sync,
	}
}

func (c *Config) SetPollTimout(timeout int) {
	c.pollTimeoutMs = timeout
}

func (c *Config) SetCreateTopics(value bool) {
	c.createTopics = value
}

func (c *Config) SetDLQConfig(dlqConfig DLQConfig) {
	c.dlqConfig = dlqConfig
}

func (c *Config) SetSendToSource(value bool) {
	c.sendToSourceEnabled = value
}

func (c *Config) SetKafkaProperty(prop string, value any) {
	c.configMap[prop] = value
}

func (c *Config) SetProps(props ConfigMap) {
	for k, v := range props {
		c.configMap[k] = v
	}
}

func (c *Config) SetLoaderProps(props ConfigMap) {

	// Clean out most common consumer props (if present) to reduce warning logs
	for k, v := range props {
		if _, ok := commonConsumerProps[k]; ok {
			continue
		}
		c.configMap[k] = v
	}
}

func (c *Config) UseSchemaRegistry() bool {
	return c.schemaRegistryConfig != nil
}

func (c *Config) ValidateSchemaRegistryConfig() error {
	if c.schemaRegistryConfig == nil {
		return nil
	}
	if c.schemaRegistryConfig.DataFormat != "avro" {
		return fmt.Errorf("unsupported schema registry data format: %s", c.schemaRegistryConfig.DataFormat)
	}
	if c.schemaRegistryConfig.OnDeserializationError != "" &&
		c.schemaRegistryConfig.OnDeserializationError != spec.OnDeserErrorDiscardMessage &&
		c.schemaRegistryConfig.OnDeserializationError != spec.OnDeserErrorShutdownStream &&
		c.schemaRegistryConfig.OnDeserializationError != spec.OnDeserErrorRestartStream {
		return fmt.Errorf("invalid schema registry onDeserializationError value: %s", c.schemaRegistryConfig.OnDeserializationError)
	}
	return nil
}

func (c *Config) OnDeserError() string {
	if c.schemaRegistryConfig == nil {
		return ""
	}
	return c.schemaRegistryConfig.OnDeserializationError
}

func (c *Config) UseSchemaRegistryWithAvro() bool {
	if c.schemaRegistryConfig != nil {
		return c.schemaRegistryConfig.DataFormat == "avro"
	}
	return false
}

func (c *Config) SchemaRegistryConfig() *spec.SchemaRegistryConfig {
	return c.schemaRegistryConfig
}

func (c *Config) SetSchemaRegistryConfig(config *spec.SchemaRegistryConfig) {
	c.schemaRegistryConfig = config
}

var commonConsumerProps = map[string]bool{
	"group.id":                      true,
	"session.timeout.ms":            true,
	"max.poll.interval.ms":          true,
	"enable.auto.commit":            true,
	"auto.commit.interval.ms":       true,
	"auto.offset.reset":             true,
	"enable.auto.offset.store":      true,
	"queued.min.messages":           true,
	"queued.max.messages.kbytes":    true,
	"fetch.wait.max.ms":             true,
	"fetch.message.max.bytes":       true,
	"max.partition.fetch.bytes":     true,
	"fetch.min.bytes":               true,
	"fetch.max.bytes":               true,
	"isolation.level":               true,
	"partition.assignment.strategy": true,
}

func displayConfig(in ConfigMap) ConfigMap {
	out := make(ConfigMap)
	for k, v := range in {
		if k != "sasl.password" {
			out[k] = v
		}
	}
	return out
}
