# GEIST Kafka Connector
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


## Limitations and improvement areas

### Kafka Sink entity (Loader)
Although the Kafka _Source_ entity (Extractor) exhibits high performance and guaranteed delivery, the Kafka _Sink_ entity has lower throughput if the stream is configured for at-least once guarantees. 

There is an option in the Stream Spec to increase the sink throughput for a given stream (set `sink.config.synchronous` to `false`), but that could in rare cases lead to message loss, e.g. in case of Geist host crashing, so should only be used for non-critical streams.

The underlying Kafka library ([librdkafka](https://github.com/edenhill/librdkafka)) did not have support for batch publish/delivery reports, that could improve this, when this connector was developed, but newer versions might have added this.


## Contact
info @ zpiroux . com

## License
Geist Kafka Connector source code is available under the MIT License.
