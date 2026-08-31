package vdf.vdt.streaming.parser.schema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import vdf.vdt.streaming.model.schema.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Parser for dynamic JSON schema definitions in streaming pipelines.
 * Deserializes JSON payloads into strongly typed TableSchema instances and
 * flattens nested schema hierarchies into dot-delimited field paths.
 */
public class DynamicSchemaJsonParser {

    // Thread-safe reusable Jackson ObjectMapper instance to minimize GC and allocation overhead
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Parses raw JSON bytes into a TableSchema.
     *
     * @param jsonBytes Raw binary payload from Kafka/Storage
     * @return Fully materialized TableSchema
     * @throws IOException If parsing fails or payload is malformed
     */
    public static TableSchema parse(byte[] jsonBytes) throws IOException {
        JsonNode root = mapper.readTree(jsonBytes);
        return parse(root);
    }

    /**
     * Parses a JSON string into a TableSchema.
     *
     * @param jsonString Serialized JSON text
     * @return Fully materialized TableSchema
     * @throws IOException If JSON structure is invalid
     */
    public static TableSchema parse(String jsonString) throws IOException {
        JsonNode root = mapper.readTree(jsonString);
        return parse(root);
    }

    /**
     * Core parsing method that extracts metadata and flattens column structures.
     *
     * @param root Root JSON tree node
     * @return Strongly typed TableSchema object
     * @throws IOException If required metadata fields or data types cannot be resolved
     */
    public static TableSchema parse(JsonNode root) throws IOException {
        // Extract metadata layer (source system and schema version)
        JsonNode metadata = root.path("metadata");
        String source = metadata.path("source").asText();
        String version = metadata.path("schema_version").asText();
        SourceVersionKey key = SourceVersionKey.of(source, version);

        // Extract root-level configurations with sensible fallbacks
        String keyField = root.path("key_field").asText("customer_id");
        int totalFields = root.path("total_fields").asInt(0);

        // Recursively flatten nested column structures
        Map<String, ColumnDefinition> columns = new HashMap<>();
        JsonNode structureNode = root.path("structure");
        flattenStructure("", structureNode, columns);

        return new TableSchema(key, keyField, totalFields, columns);
    }

    /**
     * Recursively traverses a nested JSON node tree (DFS) to flatten nested structures
     * into dot-separated paths (e.g., "risk_signals.fraud_probability_score").
     *
     * @param prefix  Current hierarchical path prefix (e.g., "parent.child")
     * @param node    Current JSON node under inspection
     * @param columns Accumulator map storing field paths mapped to ColumnDefinition
     */
    public static void flattenStructure(String prefix, JsonNode node, Map<String, ColumnDefinition> columns) {
        // Guard clause: abort traversal if the node is not a valid JSON object
        if (!node.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode childNode = field.getValue();

            // Construct dot-separated full path
            String fullPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            // Base case: Node is a leaf column definition containing type and category
            if (childNode.has("type") && childNode.has("category")) {
                DataType dataType = DataType.valueOf(childNode.path("type").asText().toUpperCase());
                FieldCategory category = FieldCategory.fromString(childNode.get("category").asText());
                columns.put(fullPath, new ColumnDefinition(fullPath, dataType, category));
            }
            // Recursive step: Node represents an inner nested object
            else if (childNode.isObject()) {
                flattenStructure(fullPath, childNode, columns);
            }
        }
    }
}