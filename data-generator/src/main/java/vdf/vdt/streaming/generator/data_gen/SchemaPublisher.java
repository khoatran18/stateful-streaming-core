package vdf.vdt.streaming.generator.data_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

// Publishes both schema definitions (A and B) on startup to:
//   1. A dedicated Kafka schema topic — two messages, one per schema, keyed by "<version>:A"
//      and "<version>:B". Each message body contains "version" and "source" so consumers
//      are self-describing. Headers carry the same fields for compact routing.
//   2. Local files at:
//        data/schema/30/<version>/<timestamp>/schema_a.json
//        data/schema/30/<version>/<timestamp>/schema_b.json
//      The timestamp folder groups both files from the same publish run together.
public class SchemaPublisher {

    private static final String SCHEMA_BASE_DIR = "data/schema";
    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

    private final KafkaProducerClient kafkaClient;
    private final ObjectMapper jsonMapper;

    public SchemaPublisher(KafkaProducerClient kafkaClient) {
        this.kafkaClient = kafkaClient;
        this.jsonMapper  = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    // Builds and publishes both schemas for the given version string (e.g. "v2").
    // Creates a timestamped folder under data/schema/30/<version>/ and writes both JSON files.
    public void publishSchemas(String schemaTopic, String version) throws IOException {
        Map<String, Object> schemaA = buildSchemaPayload(version, "A");
        Map<String, Object> schemaB = buildSchemaPayload(version, "B");

        // Publish to Kafka. Key encodes both version and source for easy filtering.
        publishToKafka(schemaTopic, version, "A", schemaA);
        publishToKafka(schemaTopic, version, "B", schemaB);

        // Write local files into a single timestamped folder
        String timestamp = TIMESTAMP_FMT.format(LocalDateTime.now());
        Path schemaDir = Path.of(SCHEMA_BASE_DIR,
                String.valueOf(Constants.SCHEMA_A_TOTAL_FIELDS), version, timestamp)
                .toAbsolutePath();
        Files.createDirectories(schemaDir);

        writeJsonFile(schemaA, schemaDir.resolve("schema_a.json").toFile());
        writeJsonFile(schemaB, schemaDir.resolve("schema_b.json").toFile());

        System.out.println(">>> Schema files written to: " + schemaDir);
    }

    // Sends one schema message to Kafka.
    // Headers carry source and version for compact consumer routing.
    private void publishToKafka(String schemaTopic, String version, String source,
                                Map<String, Object> schema) throws IOException {
        String jsonBody = jsonMapper.writeValueAsString(schema);
        String key = version + ":" + source;
        Map<String, String> headers = Map.of("version", version, "source", source);
        kafkaClient.sendWithHeader(schemaTopic, key, jsonBody, headers);
        System.out.println(">>> Published schema [" + key + "] -> topic: " + schemaTopic);
    }

    // Assembles the schema payload for one source (A or B).
    // Fields list contains full FieldDefinition objects so downstream consumers can reconstruct
    // all metadata from the message alone.
    private Map<String, Object> buildSchemaPayload(String version, String source) {
        Map<String, Object> fields = new LinkedHashMap<>();

        if ("A".equals(source)) {
            fields.put("static_categorical",  Constants.SCHEMA_A_STATIC_CATEGORICAL_FIELDS);
            fields.put("dynamic_categorical", Constants.SCHEMA_A_DYNAMIC_CATEGORICAL_FIELDS);
            fields.put("static_numeric",      Constants.SCHEMA_A_STATIC_NUMERIC_FIELDS);
            fields.put("dynamic_numeric",     Constants.SCHEMA_A_DYNAMIC_NUMERIC_FIELDS);
        } else {
            fields.put("static_categorical",  Constants.SCHEMA_B_STATIC_CATEGORICAL_FIELDS);
            fields.put("dynamic_categorical", Constants.SCHEMA_B_DYNAMIC_CATEGORICAL_FIELDS);
            fields.put("static_numeric",      Constants.SCHEMA_B_STATIC_NUMERIC_FIELDS);
            fields.put("dynamic_numeric",     Constants.SCHEMA_B_DYNAMIC_NUMERIC_FIELDS);
        }

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("version",      version);
        schema.put("source",       source);
        schema.put("total_fields", Constants.SCHEMA_A_TOTAL_FIELDS); // same count for both
        schema.put("fields",       fields);
        return schema;
    }

    private void writeJsonFile(Map<String, Object> schema, File target) throws IOException {
        jsonMapper.writeValue(target, schema);
        System.out.println("    [JSON] " + target.getAbsolutePath());
    }
}
