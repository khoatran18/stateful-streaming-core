package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        // Throughput and ID pool
        int    reqPerSecond     = 10;
        int    idRange          = 100;

        // Skew configuration:
        //   skewIdCount IDs (1..k) each receive skewPctPerSkewId% of traffic.
        //   Constraint: skewIdCount * skewPctPerSkewId <= 80.
        int    skewIdCount      = 2;
        double skewPctPerSkewId = 30.0;  // 5 * 10.0 = 50% total skew traffic

        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) props.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String bootstrapServers = props.getProperty("kafka.bootstrap-servers", "localhost:9092");
        String rawEventTopic    = props.getProperty("kafka.topic.raw-event",   "source.event");
        String schemaTopic      = props.getProperty("kafka.topic.schema",      "source.schema");
        String version          = props.getProperty("schema.version",          "v2");

        KafkaProducerClient kafkaClient = new KafkaProducerClient(bootstrapServers);

        // ── 1. Publish both schema definitions on startup ─────────────────────
        SchemaPublisher schemaPublisher = new SchemaPublisher(kafkaClient);
        try {
            schemaPublisher.publishSchemas(schemaTopic, version);
        } catch (Exception e) {
            System.err.println("Failed to publish schemas: " + e.getMessage());
            e.printStackTrace();
        }

        // ── 2. Start continuous dual-schema event generation ──────────────────
        DataGenerator dataGenerator = new DataGenerator(kafkaClient);
        System.out.println(">>> Starting Data Generator | topic: " + rawEventTopic
                + " | schema-version: " + version);
        dataGenerator.startGenerating(reqPerSecond, idRange, skewIdCount, skewPctPerSkewId,
                rawEventTopic, version);
    }
}