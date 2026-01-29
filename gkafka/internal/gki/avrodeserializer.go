package gki

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

// AvroDeserializer decodes Confluent-wire-format Avro bytes to JSON bytes
// using goavro and the provided Schema Registry client.
type AvroDeserializer struct {
	sr         schemaregistry.Client
	codecCache map[int]*goavro.Codec
	mu         sync.RWMutex
}

func NewAvroDeserializer(sr schemaregistry.Client) (*AvroDeserializer, error) {
	if sr == nil {
		return nil, fmt.Errorf("schema registry client is required when creating Avro deserializer")
	}
	return &AvroDeserializer{
		sr:         sr,
		codecCache: make(map[int]*goavro.Codec),
	}, nil
}

// Deserialize decodes the given payload bytes (in Confluent wire format)
// to JSON bytes.
func (d *AvroDeserializer) Deserialize(topic string, payload []byte) (any, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("payload too short to contain schema id")
	}

	// Confluent wire format: 0 (magic) + 4 bytes schema id (big-endian) + avro payload
	if payload[0] != 0 {
		return nil, fmt.Errorf("unknown magic byte: %d", payload[0])
	}
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))

	codec, err := d.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("create codec: %w", err)
	}

	native, _, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return nil, fmt.Errorf("native from binary: %w", err)
	}

	// Convert native to JSON (textual) representation
	jsonBytes, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("textual from native: %w", err)
	}
	return jsonBytes, nil
}

// getCodec retrieves or creates a goavro.Codec for the given schema ID.
func (d *AvroDeserializer) getCodec(id int) (*goavro.Codec, error) {
	d.mu.RLock()
	c, ok := d.codecCache[id]
	d.mu.RUnlock()
	if ok {
		return c, nil
	}

	schemaStr, err := d.fetchSchema(id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema id %d: %w", id, err)
	}

	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("new codec: %w", err)
	}

	d.mu.Lock()
	d.codecCache[id] = codec
	d.mu.Unlock()
	return codec, nil
}

// fetchSchema retrieves the Avro schema string for the given schema ID from Schema Registry.
func (d *AvroDeserializer) fetchSchema(id int) (string, error) {
	sv, err := d.sr.GetSubjectsAndVersionsByID(id)
	if err != nil {
		return "", err
	}
	if len(sv) == 0 {
		return "", fmt.Errorf("no subject found for schema id %d", id)
	}
	subject := sv[0].Subject
	schemaInfo, err := d.sr.GetBySubjectAndID(subject, id)
	if err != nil {
		return "", err
	}
	return schemaInfo.Schema, nil
}
