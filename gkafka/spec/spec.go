package spec

import (
	"encoding/json"
	"errors"

	"github.com/zpiroux/geist/entity"
)

// Errors
var (
	ErrMissingConfig = errors.New("required config is missing in stream spec")
)

// SourceConfig specifies the schema for the "customConfig" field in the "source" section
// of the stream spec. It enables arbitrary connector specific fields to be present in
// the stream spec. Any config provided in the stream spec will override the config provided
// in the factory creation.
type SourceConfig struct {
	// Topics (required) specifies which topics to use.
	Topics []Topics `json:"topics,omitempty"`

	// PollTimeoutMs (optional) specifies after how long time to return from the Poll() call if no
	// messages are available for consumption. Normally this is not needed to be provided in the
	// stream spec. It has no impact on throughput. A higher value will lower the cpu load on idle
	// streams. If omitted in the spec and in the factory config, the value gki.DefaultPollTimeoutMs
	// will be used.
	PollTimeoutMs *int `json:"pollTimeoutMs,omitempty"`

	// Properties (optional) can be used to provide standard Kafka properties.
	Properties []Property `json:"properties,omitempty"`

	// SendToSource (optional) specifies how extractors should handle send-to-source logic. See
	// gkafka.Config.SendToSource for more information on this feature.
	// Available options in the stream spec are:
	// 		* If set to true: The extractors SendToSource() interface method is enabled for this particualar stream.
	//		* If set to false: The extractors SendToSource() interface method is disabled for this particualar stream
	// 		* If omitted: The value to use will be the default value as set when constructing the connector.
	// One reason to have this config availble per stream is to reduce memory allocation when it's not needed.
	SendToSource *bool

	// DLQ (optional) details the options for DLQ handling and is required if Ops.HandlingOfUnretryableEvents
	// is set to "dlq".
	DLQ *DLQ `json:"dlq,omitempty"`
}

func NewSourceConfig(spec *entity.Spec) (sc SourceConfig, err error) {
	if spec.Source.Config.CustomConfig == nil {
		return sc, ErrMissingConfig
	}
	sinkConfigIn, err := json.Marshal(spec.Source.Config.CustomConfig)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	if err == nil {
		err = sc.Validate()
	}
	return sc, err
}

func (sc SourceConfig) Validate() error {
	return nil
}

// SinkConfig specifies the schema for the "customConfig" field in the "sink" section
// of the stream spec. It enables arbitrary connector specific fields to be present in
// the stream spec.
type SinkConfig struct {
	Topic   []SinkTopic `json:"topic,omitempty"`
	Message *Message    `json:"message,omitempty"`

	// Synchronous is used to specify if each event should be guaranteed to be persisted to the
	// broker (Synchronous: true), giving lower throughput (without not yet provided batch option),
	// or if verifying delivery report asynchronously (Synchronous: false), giving much higher
	// throughput, but could lead to message loss if GEIST host crashes.
	Synchronous *bool `json:"synchronous,omitempty"`

	// Direct low-level entity properties like Kafka producer props
	Properties []Property `json:"properties,omitempty"`
}

func NewSinkConfig(spec *entity.Spec) (sc SinkConfig, err error) {
	if spec.Sink.Config.CustomConfig == nil {
		return sc, ErrMissingConfig
	}
	sinkConfigIn, err := json.Marshal(spec.Sink.Config.CustomConfig)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	if err == nil {
		err = sc.Validate()
	}
	return sc, err
}

func NewSinkConfigFromLegacySpec(spec *entity.Spec) (sc SinkConfig, err error) {
	sinkConfigIn, err := json.Marshal(spec.Sink.Config)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	if err == nil {
		err = sc.Validate()
	}
	return sc, err
}

func (sc SinkConfig) Validate() error {
	return nil
}

type Topics struct {
	// Env specifies for which environment/stage the topic names config should be used.
	// Allowed values are "all" or any string matching the config provided to registered entity factories.
	// For example "dev", "staging", "prod" etc.
	Env   string   `json:"env,omitempty"`
	Names []string `json:"names,omitempty"`
}

type Property struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DLQ specifies how the extractor should handle DLQ logic. This config is required if
// "ops.handlingOfUnretryableEvents" field in the spec should support the "dlq" option,
// which activates when the downstream processing returns unretryable errors.
type DLQ struct {
	// Topic specifies which topic to use for DLQ events. If the extractor config does not
	// allow topic creation (gkafka.Config.CreateTopics is set to false), only Topic[].Name
	// is regarded. Otherwise, additional properties such as NumPartitions and ReplicationFactor
	// will be used as well if the topic is created (if it doesn't exist already). Since this
	// is regarded as a sink mechanism the same type is used here as for a standard sink.
	Topic []SinkTopic `json:"topic,omitempty"`

	// Generic config map for DLQ producers
	ProducerConfig map[string]any `json:"producerConfig,omitempty"`

	// If StreamIDEnrichmentPath is not empty it specifies the JSON path (e.g.
	// "my.enrichment.streamId") including the JSON field name, which will hold the
	// value of the injected stream ID for the current stream. That is, before the
	// event is sent to the DLQ the stream ID is added to a new field created in the
	// event, if this option is used.
	StreamIDEnrichmentPath string `json:"streamIDEnrichmentPath,omitempty"`
}

type SinkTopic struct {
	Env       string              `json:"env,omitempty"`
	TopicSpec *TopicSpecification `json:"topicSpec,omitempty"`
}

// Name, NumPartitions and ReplicationFactor are required.
// If sink topic is referring to an existing topic only Name will be used.
// TODO: Remove when the Kafka connector has migrated to customConfig based config
type TopicSpecification struct {
	Name              string            `json:"name"`
	NumPartitions     int               `json:"numPartitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Config            map[string]string `json:"config,omitempty"` // not yet supported
}

// Message specifies how the message should be published
type Message struct {
	// PayloadFromId is the key/field ID in the Transformed output map, which contains the actual message payload
	PayloadFromId string `json:"payloadFromId,omitempty"`
}
