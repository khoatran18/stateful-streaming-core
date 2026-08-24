package vdf.vdt.streaming.generator.data_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    // The "structure" key mirrors the event JSON exactly so consumers can reconstruct field paths:
    //   - flat fields appear as  { "fieldName": { "type": "STRING|INT|FLOAT|TIMESTAMP" } }
    //   - the nested group appears as { "debt"|"risk_signals": { fieldName: { "type": ... }, ... } }
    // key_field names the standalone customer identifier (lives in metadata, not in structure).
    private Map<String, Object> buildSchemaPayload(String version, String source) {
        Map<String, Object> schemaMeta = new LinkedHashMap<>();
        schemaMeta.put("schema_version", version);
        schemaMeta.put("source",         source);
        schemaMeta.put("timestamp",      OffsetDateTime.now().format(TIMESTAMP_FMT));

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
        String     nestedGroupName   = isA ? Constants.SCHEMA_A_NESTED_DYNAMIC_GROUP
                                          : Constants.SCHEMA_B_NESTED_DYNAMIC_GROUP;
        Set<String> nestedFieldNames = isA ? Constants.SCHEMA_A_NESTED_DYNAMIC_FIELD_NAMES
                                          : Constants.SCHEMA_B_NESTED_DYNAMIC_FIELD_NAMES;

        // Build structure map in the same insertion order as the event JSON.
        Map<String, Object> structure    = new LinkedHashMap<>();
        Map<String, Object> nestedStruct = new LinkedHashMap<>();

        // Metadata block structure
        Map<String, Object> metadataStruct = new LinkedHashMap<>();
        metadataStruct.put("customer_id",    Map.of("type", "STRING",    "category", "static_categorical"));
        metadataStruct.put("schema_version", Map.of("type", "STRING",    "category", "static_categorical"));
        metadataStruct.put("source",         Map.of("type", "STRING",    "category", "static_categorical"));
        metadataStruct.put("event_time",     Map.of("type", "TIMESTAMP", "category", "dynamic_timestamp"));
        structure.put("metadata", metadataStruct);

        for (FieldDefinition fd : staticCat)  { structure.put(fd.getName(), fieldSpec(fd)); }
        for (FieldDefinition fd : staticNum)  { structure.put(fd.getName(), fieldSpec(fd)); }
        for (FieldDefinition fd : staticTs)   { structure.put(fd.getName(), fieldSpec(fd)); }
        for (FieldDefinition fd : staticBool) { structure.put(fd.getName(), fieldSpec(fd)); }
        for (FieldDefinition fd : dynCat) {
            if (nestedFieldNames.contains(fd.getName())) {
                nestedStruct.put(fd.getName(), fieldSpec(fd));
            } else {
                structure.put(fd.getName(), fieldSpec(fd));
            }
        }
        for (FieldDefinition fd : dynTs) {
            if (nestedFieldNames.contains(fd.getName())) {
                nestedStruct.put(fd.getName(), fieldSpec(fd));
            } else {
                structure.put(fd.getName(), fieldSpec(fd));
            }
        }
        for (FieldDefinition fd : dynBool) {
            if (nestedFieldNames.contains(fd.getName())) {
                nestedStruct.put(fd.getName(), fieldSpec(fd));
            } else {
                structure.put(fd.getName(), fieldSpec(fd));
            }
        }
        for (FieldDefinition fd : dynNum) {
            if (nestedFieldNames.contains(fd.getName())) {
                nestedStruct.put(fd.getName(), fieldSpec(fd));
            } else {
                structure.put(fd.getName(), fieldSpec(fd));
            }
        }
        structure.put(nestedGroupName, nestedStruct);

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("metadata",     schemaMeta);
        schema.put("key_field",    "customer_id");
        schema.put("total_fields", Constants.SCHEMA_A_TOTAL_FIELDS); // same count for both
        schema.put("structure",    structure);
        return schema;
    }

    private Map<String, Object> fieldSpec(FieldDefinition fd) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("type",     fd.getType());
        spec.put("category", fd.getCategory());
        return spec;
    }

    private void writeJsonFile(Map<String, Object> schema, File target) throws IOException {
        jsonMapper.writeValue(target, schema);
        System.out.println("    [JSON] " + target.getAbsolutePath());
    }
}
