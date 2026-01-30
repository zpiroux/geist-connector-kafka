package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist-connector-kafka/gkafka"
)

// A simple integration test app to be run locally against a real (local) Kafka cluster.
//
// Steps to run the test:
//
//  1. Start local Kafka cluster with
//     docker compose up -d
//
//  2. Create a topic named 'test-topic' in the local Kafka cluster:
//     docker exec -it test-kafka-1 kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
//
//  3. Register an Avro schema for the test messages in the local Schema Registry:
//     jq -Rs '{schema: .}' test-message-schema.avsc | curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @- http://localhost:8081/subjects/test-topic-value/versions
//
//  4. Run this app: go run .
//
//  5. Produce Avro test messages with:
//     jq -c . test-message.json | docker exec -i test-schema-registry-1 kafka-avro-console-producer --broker-list kafka:29092 --topic test-topic --property schema.registry.url=http://schema-registry:8081 --property value.schema="$(cat test-message-schema.avsc)"
//
//     or use the following to send a plain message without using schema registry:
//     jq -c . test-message.json | docker exec -i test-kafka-1 kafka-console-producer --broker-list localhost:9092 --topic test-topic
//     This will fail deserialization if the stream is configured to use avro schema registry deserializer.
//     Depending on the "onDeserializationError" config in the stream spec, the extractor will either restart the stream,
//     shutdown the stream, or discard the message.
//
//  6. Observe the log output of consumed messages
//
//  7. Stop the app with Ctrl+C
//
//  8. Tear down the local Kafka cluster with
//     docker compose down -v
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go ensureGracefulShutdown(cancel)
	RunStream(ctx)
}

func ensureGracefulShutdown(cancel context.CancelFunc) {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Initiating wait for shutdown (SIGINT/SIGTERM) signal")

	<-shutdown

	log.Println("Received shutdown (SIGINT/SIGTERM) signal - initiating service cancellation.")
	cancel()
}

func RunStream(ctx context.Context) {
	var (
		g   *geist.Geist
		err error
	)

	config := geist.NewConfig()
	config.Ops.Log = true // For production it's better to have this set to false and instead use geist.NotifyChannel()
	config.Registry.Env = "local"

	// Configure and register Kafka extractor
	srConfig := schemaregistry.NewConfig("http://localhost:8081")
	kConfig := &gkafka.Config{
		Env:                        "local",
		PollTimeoutMs:              250,
		SchemaRegistryClientConfig: srConfig,
		KafkaProps: map[string]any{
			"bootstrap.servers": "localhost:9092",
		},
	}
	if err = config.RegisterExtractorType(gkafka.NewExtractorFactory(kConfig, nil, nil, nil)); err != nil {
		log.Fatalf("geistConfig.RegisterExtractorType() error: %v", err)
	}

	if g, err = geist.New(ctx, config); err != nil {
		log.Fatalf("geist.New() error: %v", err)
	}

	// Avro stream spec testing
	streamSpecBytes, err := os.ReadFile("stream-spec-local-kafka-test-avro.json")

	// Plain stream spec testing
	//streamSpecBytes, err := os.ReadFile("stream-spec-local-kafka-test.json")

	if err != nil {
		log.Fatalf("error reading stream spec file: %v", err)
	}

	go func() {
		streamId, err := g.RegisterStream(ctx, streamSpecBytes)
		if err != nil {
			log.Fatalf("geist.RegisterStream() error: %v", err)
		}
		log.Printf("Stream registered with streamId: %s", streamId)
	}()

	err = g.Run(ctx)

	if err != nil {
		log.Fatalf("Run() error: %v", err)
	}

	_ = g.Shutdown(ctx)
}
