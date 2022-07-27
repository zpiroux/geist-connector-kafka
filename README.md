# GEIST Kafka Connector
## Usage
See [GEIST core repo](https://github.com/zpiroux/geist).

## Limitations and improvement areas

### Kafka Sink entity (Loader)
Although the Kafka _Source_ entity (Extractor) exhibits high performance and guaranteed delivery, the Kafka _Sink_ entity has lower throughput if the stream is configured for at-least once guarantees. 

There is an option in the Stream Spec to increase the sink throughput for a given stream (set `sink.config.synchronous` to `false`), but that could in rare cases lead to message loss, e.g. in case of Geist host crashing, so should only be used for non-critical streams.

The underlaying Kafka library ([librdkafka](https://github.com/edenhill/librdkafka)) did not have support for these batch guarantees when this connector was developed, but newer versions might have added this.

### Kafka providers
Secure access is enabled when using spec provider option `"confluent"`, where API key and secret are provided in `geist.Config.Kafka` when creating Geist with `geist.New()`.

If using vanilla Kafka with spec option `"native"` it currently only supports plain access without authentication.

## Contact
info @ zpiroux . com

## License
Geist Kafka Connector source code is available under the MIT License.
