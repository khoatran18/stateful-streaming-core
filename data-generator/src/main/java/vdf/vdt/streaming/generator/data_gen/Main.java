package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        int reqPerSecond = 10;
        int idRange = 100;

        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) props.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String bootstrapServers = props.getProperty("kafka.bootstrap-servers", "localhost:9092");
        String kafkaTopic       = props.getProperty("kafka.topic",             "stream-input-events");
        String schemaTopic      = props.getProperty("kafka.schema-topic",      "stream-schema-registry");
        String version          = props.getProperty("schema.version",          "v1");

        KafkaProducerClient kafkaClient = new KafkaProducerClient(bootstrapServers);

        // ── 1. Publish schema definition on startup ───────────────────────────
        SchemaPublisher schemaPublisher = new SchemaPublisher(kafkaClient);
        try {
            schemaPublisher.publishSchema(schemaTopic, version);
        } catch (Exception e) {
            System.err.println("Failed to publish schema: " + e.getMessage());
            e.printStackTrace();
        }

        // ── 2. Start continuous data event generation ─────────────────────────
        DataGenerator dataService = new DataGenerator(kafkaClient);
        System.out.println(">>> Starting Data Generator | topic: " + kafkaTopic + " | schema-version: " + version);
        dataService.startGenerating(reqPerSecond, idRange, kafkaTopic, version);
    }
}