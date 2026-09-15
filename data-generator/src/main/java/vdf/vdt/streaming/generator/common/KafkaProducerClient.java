package vdf.vdt.streaming.generator.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerClient {
    private final List<KafkaProducer<String, String>> producers;
    private final AtomicInteger rrCounter = new AtomicInteger(0);
    private ThroughputTracker throughputTracker;
    private boolean enableDebugLog = false;

    public KafkaProducerClient(String bootstrapServers) {
        this(bootstrapServers, 1);
    }

    public KafkaProducerClient(String bootstrapServers, int producerCount) {
        producerCount = Math.max(1, producerCount);
        this.producers = new ArrayList<>(producerCount);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Batching window for high TPS
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144); // 256 KB batch size
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728L); // 128 MB producer buffer
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        for (int i = 0; i < producerCount; i++) {
            this.producers.add(new KafkaProducer<>(props));
        }
        System.out.println(">>> Initialized KafkaProducerClient pool with " + producerCount + " KafkaProducer instance(s).");
    }

    private KafkaProducer<String, String> getNextProducer() {
        if (producers.size() == 1) {
            return producers.get(0);
        }
        int index = Math.abs(rrCounter.getAndIncrement() % producers.size());
        return producers.get(index);
    }

    public void setThroughputTracker(ThroughputTracker tracker) {
        this.throughputTracker = tracker;
    }

    public void setEnableDebugLog(boolean enableDebugLog) {
        this.enableDebugLog = enableDebugLog;
    }

    /**
     * Send a plain message without custom headers using round-robin producer pool.
     */
    public void send(String topic, String key, String jsonValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        getNextProducer().send(record, (metadata, exception) -> {
            if (exception != null) {
                if (throughputTracker != null) throughputTracker.recordFailure();
                System.err.println("Failed to send message to Kafka topic " + topic + ": " + exception.getMessage());
            } else {
                if (throughputTracker != null) throughputTracker.recordSuccess();
                if (enableDebugLog) {
                    System.out.println("Sent message to Kafka topic " + topic + ": " + jsonValue);
                }
            }
        });
    }

    /**
     * Send a message with additional Kafka headers (e.g. schema-version) using round-robin producer pool.
     * Headers are encoded as UTF-8 bytes.
     */
    public void sendWithHeader(String topic, String key, String jsonValue, Map<String, String> headers) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        headers.forEach((headerKey, headerValue) ->
                record.headers().add(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));

        getNextProducer().send(record, (metadata, exception) -> {
            if (exception != null) {
                if (throughputTracker != null) throughputTracker.recordFailure();
                System.err.println("Failed to send message to Kafka topic " + topic + ": " + exception.getMessage());
            } else {
                if (throughputTracker != null) throughputTracker.recordSuccess();
                if (enableDebugLog) {
                    System.out.println("Sent message to Kafka topic " + topic + " [headers=" + headers + "]: " + jsonValue);
                }
            }
        });
    }

    public void close() {
        for (KafkaProducer<String, String> p : producers) {
            if (p != null) {
                try {
                    p.close();
                } catch (Exception e) {
                    // Ignore close exceptions on shutdown
                }
            }
        }
    }
}