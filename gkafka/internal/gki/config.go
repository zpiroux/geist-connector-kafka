package gki

import (
	"fmt"
	"sync"

	"github.com/zpiroux/geist/entity"
)

type ConfigMap map[string]any

// Config is the internal config used by each extractor/loader, combining config
// from external Config with config from what is inside the stream spec for this
// specific stream
type Config struct {
	spec                *entity.Spec
	topics              []string                   // list of topics to consume from by Extractor
	sinkTopic           *entity.TopicSpecification // topic to (create and) publish to by Loader
	pollTimeoutMs       int                        // timeoutMs in Consumer Poll function
	configMap           ConfigMap                  // supports all possible Kafka consumer properties
	topicCreationMutex  *sync.Mutex
	synchronous         bool
	createTopics        bool
	sendToSourceEnabled bool
}

func (c *Config) String() string {
	return fmt.Sprintf("topics: %v, pollTimeoutMs: %d, synchronous: %v, props: %+v",
		c.topics, c.pollTimeoutMs, c.synchronous, displayConfig(c.configMap))
}

func NewExtractorConfig(
	spec *entity.Spec,
	topics []string,
	topicCreationMutex *sync.Mutex) *Config {
	return &Config{
		spec:               spec,
		topics:             topics,
		configMap:          make(ConfigMap),
		topicCreationMutex: topicCreationMutex,
	}
}

func NewLoaderConfig(
	spec *entity.Spec,
	topic *entity.TopicSpecification,
	topicCreationMutex *sync.Mutex,
	sync bool) *Config {
	return &Config{
		spec:               spec,
		sinkTopic:          topic,
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

func displayConfig(in ConfigMap) ConfigMap {
	out := make(ConfigMap)
	for k, v := range in {
		if k != "sasl.password" {
			out[k] = v
		}
	}
	return out
}
