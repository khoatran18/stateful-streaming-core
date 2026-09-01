package vdf.vdt.streaming.parser.schema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vdf.vdt.streaming.model.schema.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// Parses raw JSON schema definitions (from Kafka bytes or string) into TableSchema objects.
// Uses DFS to flatten nested column structures into dot-separated field paths
// (e.g., "risk_signals.fraud_probability_score").
public class DynamicSchemaJsonParser {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaJsonParser.class);

    // Shared, thread-safe ObjectMapper (Jackson ObjectMapper is thread-safe after configuration)
    private static final ObjectMapper mapper = new ObjectMapper();

    // Parses raw JSON bytes into a TableSchema.
    // jsonBytes - raw binary payload from Kafka
    public static TableSchema parse(byte[] jsonBytes) throws IOException {
        LOG.debug("Parsing schema from raw bytes ({} bytes)", jsonBytes.length);
        JsonNode root = mapper.readTree(jsonBytes);
        return parse(root);
    }

    // Parses a JSON string into a TableSchema.
    // jsonString - serialized JSON text
    public static TableSchema parse(String jsonString) throws IOException {
        LOG.debug("Parsing schema from JSON string ({} chars)", jsonString.length());
        JsonNode root = mapper.readTree(jsonString);
        return parse(root);
    }

    // Core parsing logic: extracts source/version identity from metadata,
    // then flattens the "structure" node into a flat ColumnDefinition map.
    // root - root JSON tree node
    public static TableSchema parse(JsonNode root) throws IOException {
        // Extract schema identity (source system + version)
        JsonNode metadata = root.path("metadata");
        String source = metadata.path("source").asText();
        String version = metadata.path("schema_version").asText();
        SourceVersionKey key = SourceVersionKey.of(source, version);
        LOG.debug("Parsing schema for key={}", key);

        String keyField = root.path("key_field").asText("metadata.customer_id");
        int totalFields = root.path("total_fields").asInt(0);

        // Recursively flatten the nested "structure" node into a dot-path column map
        Map<String, ColumnDefinition> columns = new HashMap<>();
        JsonNode structureNode = root.path("structure");
        flattenStructure("", structureNode, columns);

        LOG.info("Schema parsed: key={} keyField='{}' totalFields={} resolvedColumns={}",
                key, keyField, totalFields, columns.size());

        return new TableSchema(key, keyField, totalFields, columns);
    }

    // Recursively (DFS) traverses a JSON structure node, building dot-separated paths
    // and registering leaf nodes as ColumnDefinition entries.
    // A leaf is identified by having both "type" and "category" fields.
    // prefix - current hierarchical path prefix, node - current node, columns - accumulator map
    public static void flattenStructure(String prefix, JsonNode node, Map<String, ColumnDefinition> columns) {
        if (!node.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode childNode = field.getValue();

            String fullPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            if (childNode.has("type") && childNode.has("category")) {
                // Leaf column: parse type and category, register in map
                DataType dataType = DataType.valueOf(childNode.path("type").asText().toUpperCase());
                FieldCategory category = FieldCategory.fromString(childNode.get("category").asText());
                columns.put(fullPath, new ColumnDefinition(fullPath, dataType, category));
                LOG.trace("Registered column: path={} type={} category={}", fullPath, dataType, category);
            } else if (childNode.isObject()) {
                // Inner nested object: recurse deeper
                LOG.trace("Descending into nested schema node: {}", fullPath);
                flattenStructure(fullPath, childNode, columns);
            }
        }
    }
}