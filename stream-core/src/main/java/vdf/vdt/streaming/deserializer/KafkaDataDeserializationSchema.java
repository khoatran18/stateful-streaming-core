package vdf.vdt.streaming.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.header.Header;
import vdf.vdt.streaming.model.event.RawKafkaDataEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaDataDeserializationSchema implements KafkaRecordDeserializationSchema<RawKafkaDataEvent>{
    private static final String HEADER_SOURCE = "source";
    private static final String HEADER_VERSION = "schema_version";

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaDataEvent> out) throws IOException {
        String source = null;
        String version = null;

        if (record.headers() != null) {
            for (Header header : record.headers()) {
                if (HEADER_SOURCE.equalsIgnoreCase(header.key())) {
                    source = new String(header.value(), StandardCharsets.UTF_8);
                } else if (HEADER_VERSION.equalsIgnoreCase(header.key())) {
                    version = new String(header.value(), StandardCharsets.UTF_8);
                }
            }
        }

        byte[] payload = record.value();
        if (payload != null) {
            out.collect(new RawKafkaDataEvent(source, version, payload));
        }
    }

    @Override
    public TypeInformation<RawKafkaDataEvent> getProducedType() {
        return TypeInformation.of(RawKafkaDataEvent.class);
    }
}
