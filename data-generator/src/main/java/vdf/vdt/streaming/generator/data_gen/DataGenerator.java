package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;
import vdf.vdt.streaming.generator.model.FieldDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
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

    // Timestamp formatter for metadata — ISO-8601 with milliseconds and zone offset
    // (e.g. "2026-08-24T09:55:05.123+07:00").
    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");

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

                int salt = random.nextInt(1001);
                String saltedKey = "ID_" + entityId + "_" + salt;
                Map<String, String> headers = Map.of("version", version, "source", source);
                kafkaClient.sendWithHeader(kafkaTopic, saltedKey, jsonStr, headers);
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
    //
    // Event structure (26 flat + 4 nested = 30 leaf fields):
    //   metadata                     — customer_id, schema_version, source, timestamp
    //   <all static + most dynamic>  — flat at event root
    //   debt (A) / risk_signals (B)  — nested object with 4 dynamic numeric fields
    private Map<String, Object> generateEvent(long entityId, String source,
                                              Random globalRandom, String version) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("customer_id",    "ID_" + entityId);
        metadata.put("schema_version", version);
        metadata.put("source",         source);
        metadata.put("event_time",     OffsetDateTime.now().format(TIMESTAMP_FMT));

        // Seeded per-entity random for static attributes — deterministic per ID.
        Random idRandom = new Random(entityId * 31L);

        boolean isA = "A".equals(source);
        List<FieldDefinition> staticCat  = isA ? Constants.SCHEMA_A_STATIC_CATEGORICAL_FIELDS
                                               : Constants.SCHEMA_B_STATIC_CATEGORICAL_FIELDS;
        List<FieldDefinition> staticNum  = isA ? Constants.SCHEMA_A_STATIC_NUMERIC_FIELDS
                                               : Constants.SCHEMA_B_STATIC_NUMERIC_FIELDS;
        List<FieldDefinition> staticTs   = isA ? Constants.SCHEMA_A_STATIC_TIMESTAMP_FIELDS
                                               : Constants.SCHEMA_B_STATIC_TIMESTAMP_FIELDS;
        List<FieldDefinition> staticBool = isA ? Constants.SCHEMA_A_STATIC_BOOLEAN_FIELDS
                                               : Constants.SCHEMA_B_STATIC_BOOLEAN_FIELDS;
        List<FieldDefinition> dynCat     = isA ? Constants.SCHEMA_A_DYNAMIC_CATEGORICAL_FIELDS
                                               : Constants.SCHEMA_B_DYNAMIC_CATEGORICAL_FIELDS;
        List<FieldDefinition> dynNum     = isA ? Constants.SCHEMA_A_DYNAMIC_NUMERIC_FIELDS
                                               : Constants.SCHEMA_B_DYNAMIC_NUMERIC_FIELDS;
        List<FieldDefinition> dynTs      = isA ? Constants.SCHEMA_A_DYNAMIC_TIMESTAMP_FIELDS
                                               : Constants.SCHEMA_B_DYNAMIC_TIMESTAMP_FIELDS;
        List<FieldDefinition> dynBool    = isA ? Constants.SCHEMA_A_DYNAMIC_BOOLEAN_FIELDS
                                               : Constants.SCHEMA_B_DYNAMIC_BOOLEAN_FIELDS;
        String    nestedGroupName  = isA ? Constants.SCHEMA_A_NESTED_DYNAMIC_GROUP
                                        : Constants.SCHEMA_B_NESTED_DYNAMIC_GROUP;
        Set<String> nestedFieldNames = isA ? Constants.SCHEMA_A_NESTED_DYNAMIC_FIELD_NAMES
                                          : Constants.SCHEMA_B_NESTED_DYNAMIC_FIELD_NAMES;

        Map<String, Object> event       = new LinkedHashMap<>();
        Map<String, Object> nestedGroup = new LinkedHashMap<>();

        event.put("metadata", metadata);

        // Static fields are flat at event root — deterministic per customer ID.
        for (FieldDefinition fd : staticCat)  { event.put(fd.getName(), generateFieldValue(fd, idRandom)); }
        for (FieldDefinition fd : staticNum)  { event.put(fd.getName(), generateFieldValue(fd, idRandom)); }
        for (FieldDefinition fd : staticTs)   { event.put(fd.getName(), generateFieldValue(fd, idRandom)); }
        for (FieldDefinition fd : staticBool) { event.put(fd.getName(), generateFieldValue(fd, idRandom)); }

        // Dynamic categorical fields: flat at root unless part of nested group.
        for (FieldDefinition fd : dynCat) {
            Object value = generateFieldValue(fd, globalRandom);
            if (nestedFieldNames.contains(fd.getName())) {
                nestedGroup.put(fd.getName(), value);
            } else {
                event.put(fd.getName(), value);
            }
        }
        for (FieldDefinition fd : dynTs) { event.put(fd.getName(), generateFieldValue(fd, globalRandom)); }

        // Dynamic boolean fields: flat at root unless part of nested group.
        for (FieldDefinition fd : dynBool) {
            Object value = generateFieldValue(fd, globalRandom);
            if (nestedFieldNames.contains(fd.getName())) {
                nestedGroup.put(fd.getName(), value);
            } else {
                event.put(fd.getName(), value);
            }
        }

        // Dynamic numeric: flat at root unless the field belongs to the nested group.
        for (FieldDefinition fd : dynNum) {
            Object value = generateFieldValue(fd, globalRandom);
            if (nestedFieldNames.contains(fd.getName())) {
                nestedGroup.put(fd.getName(), value);
            } else {
                event.put(fd.getName(), value);
            }
        }

        event.put(nestedGroupName, nestedGroup);
        return event;
    }

    // Generates a valid value for a field based on its constraint kind and type.
    //   ENUM      -> random pick from enum_values
    //   TIMESTAMP -> random ISO-8601 string with milliseconds within [minEpoch, maxEpoch] at UTC+7
    //   BOOLEAN   -> random boolean (true/false)
    //   INT       -> random integer in [min, max]
    //   LONG      -> random long in [min, max] (range fits within double precision)
    //   FLOAT     -> random double in [min, max) rounded to 2 decimal places
    //   DOUBLE    -> same as FLOAT (distinct semantic type; same generation logic)
    private Object generateFieldValue(FieldDefinition fd, Random random) {
        if ("ENUM".equals(fd.getConstraintKind())) {
            List<String> values = fd.getEnumValues();
            return values.get(random.nextInt(values.size()));
        }

        if ("BOOLEAN".equals(fd.getType())) {
            return random.nextBoolean();
        }

        if ("TIMESTAMP".equals(fd.getType())) {
            long minEpoch = fd.getMinValue().longValue();
            long maxEpoch = fd.getMaxValue().longValue();
            long epoch = minEpoch + (long)(random.nextDouble() * (maxEpoch - minEpoch));
            return java.time.Instant.ofEpochSecond(epoch)
                    .atOffset(java.time.ZoneOffset.ofHours(7))
                    .format(TIMESTAMP_FMT);
        }

        double min = fd.getMinValue();
        double max = fd.getMaxValue();

        if ("INT".equals(fd.getType())) {
            return (int) min + random.nextInt((int)(max - min) + 1);
        }

        if ("LONG".equals(fd.getType())) {
            // long range fits safely in double precision for the field ranges in Constants.
            return Math.round(min + (max - min) * random.nextDouble());
        }

        // FLOAT and DOUBLE — two decimal places for readability.
        double raw = min + (max - min) * random.nextDouble();
        return Double.parseDouble(String.format(java.util.Locale.US, "%.2f", raw));
    }
}