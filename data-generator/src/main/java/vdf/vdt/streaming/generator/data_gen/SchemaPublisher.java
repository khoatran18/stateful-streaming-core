package vdf.vdt.streaming.generator.data_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

// Publishes the schema definition on startup to:
//   1. A dedicated Kafka schema topic (key = version, body = full JSON)
//   2. Local files at data/schema/<totalFields>/<version>/<timestamp>.json and .yaml
//
// Design notes:
//   - The schema topic uses long-term retention without log compaction. On restart,
//     downstream consumers read from earliest to rebuild the full version map.
//   - Version is embedded in the message body (self-describing) and also used as the
//     Kafka key so consumers can filter or trace per version.
//   - Local files mirror the rule output convention: data/rules/<totalRules>/<timestamp>.json
//     becomes data/schema/<totalFields>/<version>/<timestamp>.json|yaml.
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

    // Builds and publishes the schema for the given version string (e.g. "v1").
    public void publishSchema(String schemaTopic, String version) throws IOException {
        Map<String, Object> schema = buildSchemaPayload(version);
        String jsonBody = jsonMapper.writeValueAsString(schema);

        // 1. Publish to Kafka. Key = version so consumers can filter per version.
        kafkaClient.send(schemaTopic, version, jsonBody);
        System.out.println(">>> Published schema [" + version + "] -> topic: " + schemaTopic);

        // 2. Write local files under data/schema/<totalFields>/<version>/<timestamp>.*
        String timestamp = TIMESTAMP_FMT.format(LocalDateTime.now());
        Path schemaDir = Path.of(SCHEMA_BASE_DIR, String.valueOf(Constants.TOTAL_FIELDS), version)
                .toAbsolutePath();
        Files.createDirectories(schemaDir);

        writeJsonFile(schema, schemaDir.resolve(timestamp + ".json").toFile());
        writeYamlFile(schema, schemaDir.resolve(timestamp + ".yaml").toFile());

        System.out.println(">>> Schema files written to: " + schemaDir);
    }

    // Builds the schema payload. Each field list contains full FieldDefinition objects
    // (including category) so downstream consumers can reconstruct all metadata from the
    // message alone without needing the source code.
    private Map<String, Object> buildSchemaPayload(String version) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("static_categorical",  Constants.STATIC_CATEGORICAL_FIELDS);
        fields.put("dynamic_categorical", Constants.DYNAMIC_CATEGORICAL_FIELDS);
        fields.put("static_numeric",      Constants.STATIC_NUMERIC_FIELDS);
        fields.put("dynamic_numeric",     Constants.DYNAMIC_NUMERIC_FIELDS);

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("version",      version);
        schema.put("total_fields", Constants.TOTAL_FIELDS);
        schema.put("fields",       fields);
        return schema;
    }

    private void writeJsonFile(Map<String, Object> schema, File target) throws IOException {
        jsonMapper.writeValue(target, schema);
        System.out.println("    [JSON] " + target.getAbsolutePath());
    }

    private void writeYamlFile(Map<String, Object> schema, File target) throws IOException {
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setPrettyFlow(true);
        Yaml yaml = new Yaml(opts);
        try (FileWriter writer = new FileWriter(target)) {
            yaml.dump(schema, writer);
        }
        System.out.println("    [YAML] " + target.getAbsolutePath());
    }
}
