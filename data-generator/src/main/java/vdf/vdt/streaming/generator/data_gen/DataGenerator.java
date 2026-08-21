package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;
import vdf.vdt.streaming.generator.model.FieldDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

// Continuously generates CDP customer events for Schema A (transactions) and Schema B
// (access logs) and pushes them to Kafka.
//
// Each event tick picks one customer ID (subject to skew weights), then randomly selects
// schema A or B (50/50). The Kafka message carries both "schema-version" and "source"
// headers so downstream consumers can route without parsing the body.
//
// Skew configuration:
//   skewIdCount      - number of IDs that receive disproportionate traffic (IDs 1..k).
//   skewPctPerSkewId - percentage of total traffic each skew ID individually receives.
//   Constraint: skewIdCount * skewPctPerSkewId <= 80. Remaining traffic is split evenly
//   across the non-skew IDs.
public class DataGenerator {

    private final KafkaProducerClient kafkaClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DataGenerator(KafkaProducerClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    // Validates skew config, then runs the event-generation loop indefinitely.
    //   reqPerSecond     - target event throughput
    //   idRange          - pool size of distinct customer IDs
    //   skewIdCount      - number of IDs at the top of the pool that receive heavy traffic
    //   skewPctPerSkewId - individual traffic share (%) for each skew ID
    //   kafkaTopic       - destination Kafka topic for data events
    //   version          - active schema version (e.g. "v2"); written into the Kafka header
    public void startGenerating(int reqPerSecond, int idRange,
                                int skewIdCount, double skewPctPerSkewId,
                                String kafkaTopic, String version) {
        validateSkewConfig(skewIdCount, skewPctPerSkewId, idRange);

        double totalSkewPct = skewIdCount * skewPctPerSkewId;
        System.out.println("Starting CDP Data Generator"
                + " | schema: " + version
                + " | throughput: " + reqPerSecond + " req/s"
                + " | ID pool: " + idRange
                + " | skew IDs: " + skewIdCount
                + " | skew pct/ID: " + skewPctPerSkewId + "%"
                + " | total skew traffic: " + totalSkewPct + "%");

        Random random       = new Random();
        long intervalMillis = 1000L / Math.max(1, reqPerSecond);

        while (true) {
            long startTime = System.currentTimeMillis();
            try {
                long entityId = pickEntityId(random, idRange, skewIdCount, skewPctPerSkewId);
                // 50/50 schema selection per tick
                String source = random.nextBoolean() ? "A" : "B";

                Map<String, Object> event = generateEvent(entityId, source, random, version);
                String jsonStr = objectMapper.writeValueAsString(event);

                Map<String, String> headers = Map.of("version", version, "source", source);
                kafkaClient.sendWithHeader(kafkaTopic, "ID_" + entityId, jsonStr, headers);
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

    // Checks skewIdCount * skewPctPerSkewId <= 80, and that skewIdCount is within [0, idRange].
    // Throws IllegalArgumentException with a descriptive message if either constraint is violated.
    private void validateSkewConfig(int skewIdCount, double skewPctPerSkewId, int idRange) {
        if (skewIdCount < 0 || skewIdCount > idRange) {
            throw new IllegalArgumentException(
                    "INVALID skew config: skewIdCount(" + skewIdCount
                    + ") must be in [0, idRange=" + idRange + "]");
        }
        double totalSkewPct = skewIdCount * skewPctPerSkewId;
        if (totalSkewPct > 80.0) {
            throw new IllegalArgumentException(
                    "INVALID skew config: skewIdCount(" + skewIdCount
                    + ") * skewPctPerSkewId(" + skewPctPerSkewId
                    + "%) = " + totalSkewPct + "% exceeds the 80% cap");
        }
    }

    // Picks a customer entity ID using skew-weighted selection.
    // IDs 1..skewIdCount each hold skewPctPerSkewId% of traffic.
    // The remaining IDs share the leftover traffic uniformly.
    private long pickEntityId(Random random, int idRange,
                              int skewIdCount, double skewPctPerSkewId) {
        double r = random.nextDouble() * 100.0;

        // Walk through skew ID slots — each slot covers [i * pct, (i+1) * pct)
        for (int i = 0; i < skewIdCount; i++) {
            if (r < (i + 1) * skewPctPerSkewId) {
                return i + 1; // skew IDs are 1-indexed
            }
        }

        // Non-skew IDs: uniform random from [skewIdCount+1, idRange]
        int nonSkewCount = idRange - skewIdCount;
        return skewIdCount + 1 + random.nextInt(nonSkewCount);
    }

    // Builds one customer event for the given entity ID and schema source (A or B).
    // Static fields use a seeded Random derived from entityId — same ID always yields the same values.
    // Dynamic fields use the global (unseeded) Random and vary each event.
    private Map<String, Object> generateEvent(long entityId, String source,
                                              Random globalRandom, String version) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("id",             "ID_" + entityId);
        event.put("timestamp",      System.currentTimeMillis());
        event.put("schema_version", version);
        event.put("source",         source);

        // Seeded per-entity random for static attributes — deterministic per ID
        Random idRandom = new Random(entityId * 31L);

        if ("A".equals(source)) {
            for (FieldDefinition fd : Constants.SCHEMA_A_STATIC_CATEGORICAL_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, idRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_A_DYNAMIC_CATEGORICAL_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, globalRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_A_STATIC_NUMERIC_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, idRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_A_DYNAMIC_NUMERIC_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, globalRandom));
            }
        } else {
            for (FieldDefinition fd : Constants.SCHEMA_B_STATIC_CATEGORICAL_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, idRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_B_DYNAMIC_CATEGORICAL_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, globalRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_B_STATIC_NUMERIC_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, idRandom));
            }
            for (FieldDefinition fd : Constants.SCHEMA_B_DYNAMIC_NUMERIC_FIELDS) {
                event.put(fd.getName(), generateFieldValue(fd, globalRandom));
            }
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