package vdf.vdt.streaming.generator.data_gen;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

// End-to-end schema validation demo for the dual-schema setup.
//
// Flow:
//   Step 1 – Load schema registry from source.schema topic (from earliest).
//             Key format in topic: "<version>:<source>" (e.g. "v2:A", "v2:B").
//             Build: Map<"version:source", Map<fieldName, FieldDefinition>>
//
//   Step 2 – Consume real data events from source.event.
//             For each record:
//               a. Read Kafka headers "version" and "source"
//               b. Parse JSON body -> Map<String, Object>
//               c. Look up schema by "version:source"
//               d. Validate every field (type + constraint)
//               e. Print result
//
// Schema topic setup (log compaction keeps latest schema per version:source key):
//   kafka-topics.sh --create --bootstrap-server localhost:9092 \
//     --topic source.schema --partitions 1 \
//     --config cleanup.policy=compact \
//     --config min.compaction.lag.ms=0 \
//     --config delete.retention.ms=100
public class SchemaConsumerExample {

    private static final String SCHEMA_TOPIC      = "source.schema";
    private static final String DATA_TOPIC        = "source.event";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // Key format: "<version>:<source>" (e.g. "v2:A", "v2:B") -> fieldName -> FieldDefinition
    private final Map<String, Map<String, FieldDefinition>> schemaRegistry = new HashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    // ═════════════════════════════════════════════════════════════════════════
    // STEP 1: Load schema registry from the schema topic
    // ═════════════════════════════════════════════════════════════════════════

    // Reads all messages from the schema topic (from earliest) and populates schemaRegistry.
    // With log compaction, each "version:source" key has exactly one message.
    // Call once at startup before consuming data events.
    public void loadSchemasFromKafka() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "schema-loader-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(SCHEMA_TOPIC)
                    .forEach(pi -> partitions.add(new TopicPartition(pi.topic(), pi.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Snapshot end offsets to know when we've read everything currently in topic
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            boolean emptyTopic = endOffsets.values().stream().allMatch(o -> o == 0L);
            if (emptyTopic) {
                System.err.println("[WARN] Schema topic is empty. Run SchemaPublisher first.");
                return;
            }

            boolean allConsumed = false;
            while (!allConsumed) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    // key = "version:source" (e.g. "v2:A")
                    parseAndRegisterSchema(record.key(), record.value());
                }
                allConsumed = partitions.stream().allMatch(tp ->
                        consumer.position(tp) >= endOffsets.getOrDefault(tp, 0L));
            }
        }

        System.out.println("Schema registry loaded. Known schemas: " + schemaRegistry.keySet());
    }

    // Parses one schema Kafka message and registers it in the local registry.
    // Expected JSON body:
    //   { "version": "v2", "source": "A", "total_fields": 30,
    //     "fields": {
    //       "static_categorical":  [ { "name": "...", "type": "STRING", "constraint_kind": "ENUM",
    //                                  "enum_values": [...] } ],
    //       "dynamic_categorical": [ ... ],
    //       "static_numeric":      [ { "name": "...", "type": "INT", "constraint_kind": "RANGE",
    //                                  "min_value": 18, "max_value": 100 } ],
    //       "dynamic_numeric":     [ ... ]
    //     }
    //   }
    private void parseAndRegisterSchema(String key, String jsonBody) {
        try {
            JsonNode root    = mapper.readTree(jsonBody);
            JsonNode vNode   = root.get("version");
            JsonNode srcNode = root.get("source");
            JsonNode fNode   = root.get("fields");

            if (vNode == null || srcNode == null || fNode == null || !fNode.isObject()) {
                System.err.println("[WARN] Skipping schema message (key=" + key
                        + "): missing 'version', 'source', or 'fields' object. "
                        + "May be old-format message — re-publish via SchemaPublisher.");
                return;
            }

            String registryKey = vNode.asText() + ":" + srcNode.asText();  // e.g. "v2:A"
            TypeReference<List<FieldDefinition>> fdListType = new TypeReference<>() {};
            Map<String, FieldDefinition> fieldMap = new LinkedHashMap<>();

            for (String group : List.of("static_categorical", "dynamic_categorical",
                                        "static_numeric",     "dynamic_numeric")) {
                JsonNode groupNode = fNode.get(group);
                if (groupNode == null || !groupNode.isArray() || groupNode.isEmpty()) continue;

                // Detect old format (plain strings instead of FieldDefinition objects)
                if (groupNode.get(0).isTextual()) {
                    System.err.println("[WARN] Skipping schema (key=" + key + "): group '"
                            + group + "' contains plain strings. Re-run SchemaPublisher.");
                    return;
                }

                List<FieldDefinition> defs = mapper.convertValue(groupNode, fdListType);
                defs.forEach(fd -> fieldMap.put(fd.getName(), fd));
            }

            schemaRegistry.put(registryKey, fieldMap);
            System.out.println("  Registered schema [" + registryKey + "] with "
                    + fieldMap.size() + " fields.");

        } catch (Exception e) {
            System.err.println("[WARN] Failed to parse schema (key=" + key
                    + "): " + e.getMessage() + " — skipped.");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // STEP 2: Validate a data event against its schema
    // ═════════════════════════════════════════════════════════════════════════

    // Validates a parsed data event against the schema for the given version and source.
    // version - from Kafka header "version" (e.g. "v2")
    // source  - from Kafka header "source" (e.g. "A" or "B")
    // Returns list of validation error messages; empty means valid.
    public List<String> validate(Map<String, Object> event, String version, String source) {
        List<String> errors = new ArrayList<>();

        checkRequired(event, "id",             errors);
        checkRequired(event, "timestamp",      errors);
        checkRequired(event, "schema_version", errors);
        checkRequired(event, "source",         errors);

        String registryKey = version + ":" + source;
        Map<String, FieldDefinition> schema = schemaRegistry.get(registryKey);
        if (schema == null) {
            errors.add("Unknown schema [" + registryKey + "]. Known: " + schemaRegistry.keySet());
            return errors;
        }

        for (Map.Entry<String, FieldDefinition> entry : schema.entrySet()) {
            String          fieldName = entry.getKey();
            FieldDefinition fd        = entry.getValue();
            Object          value     = event.get(fieldName);

            if (value == null) {
                errors.add("Missing field: " + fieldName);
                continue;
            }

            if ("ENUM".equals(fd.getConstraintKind())) {
                validateEnum(fieldName, value, fd, errors);
            } else if ("RANGE".equals(fd.getConstraintKind())) {
                validateRange(fieldName, value, fd, errors);
            } else {
                errors.add("Unknown constraintKind for field: " + fieldName);
            }
        }

        return errors;
    }

    // ── Field-level validators ─────────────────────────────────────────────────

    private void validateEnum(String name, Object value, FieldDefinition fd, List<String> errors) {
        String strVal = String.valueOf(value);
        if (!fd.getEnumValues().contains(strVal)) {
            errors.add("Field '" + name + "': '" + strVal + "' not in " + fd.getEnumValues());
        }
    }

    private void validateRange(String name, Object value, FieldDefinition fd, List<String> errors) {
        double numVal;
        try {
            numVal = ((Number) value).doubleValue();
        } catch (ClassCastException e) {
            errors.add("Field '" + name + "': expected "
                    + fd.getType() + " but got '" + value + "' ("
                    + value.getClass().getSimpleName() + ")");
            return;
        }

        if (numVal < fd.getMinValue() || numVal > fd.getMaxValue()) {
            errors.add(String.format("Field '%s': %.4g out of range [%s, %s]",
                    name, numVal, fd.getMinValue(), fd.getMaxValue()));
        }

        if ("INT".equals(fd.getType()) && numVal != Math.floor(numVal)) {
            errors.add(String.format("Field '%s': expected integer but got %.6f", name, numVal));
        }
    }

    private void checkRequired(Map<String, Object> event, String key, List<String> errors) {
        if (!event.containsKey(key) || event.get(key) == null) {
            errors.add("Missing required field: " + key);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Main: load schemas → consume real data events → validate each one
    // ═════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) throws Exception {
        SchemaConsumerExample validator = new SchemaConsumerExample();

        // ── 1. Load all schema versions from schema topic ─────────────────────
        System.out.println("=== [Step 1] Loading schemas from: " + SCHEMA_TOPIC + " ===");
        validator.loadSchemasFromKafka();

        if (validator.schemaRegistry.isEmpty()) {
            System.err.println("No schemas loaded. Aborting.");
            return;
        }

        // ── 2. Consume real events from data topic and validate ───────────────
        System.out.println("\n=== [Step 2] Consuming real events from: " + DATA_TOPIC + " ===");
        System.out.println("Press Ctrl+C to stop.\n");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        // Unique group ID each run → always starts from earliest
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "schema-validator-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");

        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<Map<String, Object>> mapType = new TypeReference<>() {};

        long totalEvents   = 0;
        long validEvents   = 0;
        long invalidEvents = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(DATA_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    totalEvents++;

                    // a. Read "version" and "source" from Kafka headers
                    String version = extractHeader(record, "version");
                    String source  = extractHeader(record, "source");

                    if (version == null || source == null) {
                        System.err.printf("[Event #%d] Missing 'version' or 'source' header — skipped%n",
                                totalEvents);
                        continue;
                    }

                    // b. Parse JSON body
                    Map<String, Object> event;
                    try {
                        event = objectMapper.readValue(record.value(), mapType);
                    } catch (Exception e) {
                        System.err.printf("[Event #%d] Failed to parse JSON: %s%n",
                                totalEvents, e.getMessage());
                        continue;
                    }

                    // c. Validate against loaded schema
                    List<String> errors = validator.validate(event, version, source);

                    // d. Print result
                    if (errors.isEmpty()) {
                        validEvents++;
                        System.out.printf("[Event #%d] id=%-12s version=%-4s source=%s → ✓ VALID%n",
                                totalEvents, event.get("id"), version, source);
                    } else {
                        invalidEvents++;
                        System.out.printf("[Event #%d] id=%-12s version=%-4s source=%s → ✗ INVALID (%d errors)%n",
                                totalEvents, event.get("id"), version, source, errors.size());
                        errors.forEach(err -> System.out.println("          " + err));
                    }

                    // Print summary every 100 events
                    if (totalEvents % 100 == 0) {
                        System.out.printf("%n--- Summary: total=%d  valid=%d  invalid=%d ---%n%n",
                                totalEvents, validEvents, invalidEvents);
                    }
                }
            }
        }
    }

    // Extracts a single header value as a UTF-8 string. Returns null if header is absent.
    private static String extractHeader(ConsumerRecord<String, String> record, String headerKey) {
        Header header = record.headers().lastHeader(headerKey);
        if (header == null) return null;
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
