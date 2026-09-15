package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        // Throughput, worker threads, producer pool count, and ID pool
        int    reqPerSecond     = 1000000;
        int    numThreads       = 1;        // Configurable worker threads count for multi-threaded generation
        int    producerCount    = 1;        // Configurable KafkaProducer pool size (number of parallel producer instances)
        int    idRange          = 1000000;

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

//        reqPerSecond     = Integer.parseInt(props.getProperty("generator.req-per-second", String.valueOf(reqPerSecond)));
//        numThreads       = Integer.parseInt(props.getProperty("generator.num-threads", String.valueOf(numThreads)));
//        idRange          = Integer.parseInt(props.getProperty("generator.id-range", String.valueOf(idRange)));

        String bootstrapServers = props.getProperty("kafka.bootstrap-servers", "localhost:9092");
        String rawEventTopic    = props.getProperty("kafka.topic.raw-event",   "source.event");
        String schemaTopic      = props.getProperty("kafka.topic.schema",      "source.schema");
        String version          = props.getProperty("schema.version",          "v2");

        KafkaProducerClient kafkaClient = new KafkaProducerClient(bootstrapServers, producerCount);

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
                + " | schema-version: " + version
                + " | target TPS: " + reqPerSecond
                + " | numThreads: " + numThreads
                + " | producerCount: " + producerCount);
        dataGenerator.startGenerating(reqPerSecond, numThreads, idRange, skewIdCount, skewPctPerSkewId,
                rawEventTopic, version);
    }
}