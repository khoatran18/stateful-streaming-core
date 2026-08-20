package vdf.vdt.streaming.generator.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerClient {
    private final KafkaProducer<String, String> producer;

    public KafkaProducerClient(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // batch

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Send a plain message without custom headers.
     */
    public void send(String topic, String key, String jsonValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message to Kafka topic " + topic + ": " + exception.getMessage());
            } else {
                System.out.println("Sent message to Kafka topic " + topic + ": " + jsonValue);
            }
        });
    }

    /**
     * Send a message with additional Kafka headers (e.g. schema-version).
     * Headers are encoded as UTF-8 bytes.
     */
    public void sendWithHeader(String topic, String key, String jsonValue, Map<String, String> headers) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        headers.forEach((headerKey, headerValue) ->
                record.headers().add(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message to Kafka topic " + topic + ": " + exception.getMessage());
            } else {
                System.out.println("Sent message to Kafka topic " + topic + " [headers=" + headers + "]: " + jsonValue);
            }
        });
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}