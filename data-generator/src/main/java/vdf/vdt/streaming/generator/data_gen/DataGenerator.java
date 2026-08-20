package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;
import vdf.vdt.streaming.generator.model.FieldDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

// Continuously generates CDP customer events for the 200-field schema and pushes them to Kafka.
// Every message carries a schema-version header so downstream consumers can look up the
// matching schema definition from the schema topic.
public class DataGenerator {

    private final KafkaProducerClient kafkaClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DataGenerator(KafkaProducerClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    // Runs the event-generation loop indefinitely.
    //   reqPerSecond - target event throughput
    //   idRange      - pool size of distinct customer IDs
    //   kafkaTopic   - destination Kafka topic for data events
    //   version      - active schema version (e.g. "v1"); written into the Kafka header
    public void startGenerating(int reqPerSecond, int idRange, String kafkaTopic, String version) {
        System.out.println("Starting CDP Data Generator | schema: " + version
                + " | throughput: " + reqPerSecond + " req/s | ID pool: " + idRange);

        Random random        = new Random();
        long intervalMillis  = 1000L / Math.max(1, reqPerSecond);
        Map<String, String> headers = Map.of("schema-version", version);

        while (true) {
            long startTime = System.currentTimeMillis();
            try {
                Map<String, Object> event = generateEvent(idRange, random, version);
                String jsonStr = objectMapper.writeValueAsString(event);
                kafkaClient.sendWithHeader(kafkaTopic, event.get("id").toString(), jsonStr, headers);
            } catch (Exception e) {
                e.printStackTrace();
            }

            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed < intervalMillis) {
                try { Thread.sleep(intervalMillis - elapsed); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }
    }

    // Builds one customer event.
    // Static fields (categorical + numeric) use a seeded Random derived from the entity ID,
    // so the same customer ID always produces the same static attribute values.
    // Dynamic fields use the global (unseeded) Random and vary each event.
    private Map<String, Object> generateEvent(int idRange, Random globalRandom, String version) {
        Map<String, Object> event = new LinkedHashMap<>();

        long entityId = globalRandom.nextInt(idRange) + 1;
        event.put("id",             "ID_" + entityId);
        event.put("timestamp",      System.currentTimeMillis());
        event.put("schema_version", version);

        // Seeded per-entity random - static attributes are deterministic for a given ID
        Random idRandom = new Random(entityId * 31L);

        for (FieldDefinition fd : Constants.STATIC_CATEGORICAL_FIELDS) {
            event.put(fd.getName(), generateFieldValue(fd, idRandom));
        }
        for (FieldDefinition fd : Constants.DYNAMIC_CATEGORICAL_FIELDS) {
            event.put(fd.getName(), generateFieldValue(fd, globalRandom));
        }
        for (FieldDefinition fd : Constants.STATIC_NUMERIC_FIELDS) {
            event.put(fd.getName(), generateFieldValue(fd, idRandom));
        }
        for (FieldDefinition fd : Constants.DYNAMIC_NUMERIC_FIELDS) {
            event.put(fd.getName(), generateFieldValue(fd, globalRandom));
        }

        return event;
    }

    // Generates a valid value for a field based on its constraint.
    //   ENUM  -> random pick from enum_values
    //   INT   -> random integer in [min, max]
    //   FLOAT -> random double in [min, max)
    private Object generateFieldValue(FieldDefinition fd, Random random) {
        if ("ENUM".equals(fd.getConstraintKind())) {
            List<String> values = fd.getEnumValues();
            return values.get(random.nextInt(values.size()));
        }

        double min = fd.getMinValue();
        double max = fd.getMaxValue();

        if ("INT".equals(fd.getType())) {
            int intMin = (int) min;
            int intMax = (int) max;
            return random.nextInt(intMax - intMin + 1) + intMin;
        } else {
            return min + (max - min) * random.nextDouble();
        }
    }
}