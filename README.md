# GEIST Kafka Connector
<div>

[![Go Report Card](https://goreportcard.com/badge/github.com/zpiroux/geist-connector-kafka)](https://goreportcard.com/report/github.com/zpiroux/geist-connector-kafka)
[![Go Reference](https://pkg.go.dev/badge/github.com/zpiroux/geist-connector-kafka.svg)](https://pkg.go.dev/github.com/zpiroux/geist-connector-kafka)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-kafka&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-kafka)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-kafka&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-kafka)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-kafka&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-kafka)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-kafka&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-kafka)

</div>

Geist Kafka Connector enables Kafka as source and sink type in stream specs when using Geist.
## Usage
See [GEIST core repo](https://github.com/zpiroux/geist) for general information.

Install with:
```sh
go get github.com/zpiroux/geist-connector-kafka
```

### Geist Integration

Register connector prior to starting up Geist with (error handling omitted):
```go
import (
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist-connector-kafka/gkafka"
)

...
geistConfig := geist.NewConfig()

kafkaConfig := &gkafka.Config{ /* add config */ }

err = geistConfig.RegisterExtractorType(gkafka.NewExtractorFactory(kafkaConfig))
err = geistConfig.RegisterLoaderType(gkafka.NewLoaderFactory(kafkaConfig))

g, err := geist.New(ctx, geistConfig)
...
```

### Stream Spec Configuration

Common default Kafka properties for the streams can be provided when creating the factory using its `Config` fields.

Any Kafka property can also be provided in the Stream Spec itself, which then overrides the factory provided ones.

The only special config option provided in addition to the Kafka-native ones is the config for generating unique group IDs.

If the `group.id` property is assigned with a value on the format `"@UniqueWithPrefix.my-groupid-prefix"` a unique `group.id` value will be generated on the format `"my-groupid-prefix.<unique extractor id>.<ISO UTC timestamp micros>"`.

In addition to config as part of Kafka config properties, the following are available for a Kafka source in the stream spec:

* Poll timeout: standard Kafka consumer config.
* DLQ config:  If the stream is configured to use DLQ for unprocessable events (causing unretryable errors), the name of the DLQ topic needs to be provided here. If the extractor connector's `createTopics` config is set to true, the DLQ topic will be created automatically. See the DLQ struct in Geist Spec definition for more info.


## Limitations and improvement areas

### Kafka Sink entity (Loader)
Although the Kafka _Source_ entity (Extractor) exhibits high performance and guaranteed delivery, the Kafka _Sink_ entity has lower throughput if the stream is configured for at-least once guarantees. 

There is an option in the Stream Spec to increase the sink throughput for a given stream (set `sink.config.synchronous` to `false`), but that could in rare cases lead to message loss, e.g. in case of Geist host crashing, so should only be used for non-critical streams.

The underlying Kafka library ([librdkafka](https://github.com/edenhill/librdkafka)) did not have support for batch publish/delivery reports, that could improve this, when this connector was developed, but newer versions might have added this.


## Contact
info @ zpiroux . com

## License
Geist Kafka Connector source code is available under the MIT License.
