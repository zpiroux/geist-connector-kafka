package ikafka

type Deserializer interface {
	Deserialize(topic string, payload []byte) (any, error)
}
