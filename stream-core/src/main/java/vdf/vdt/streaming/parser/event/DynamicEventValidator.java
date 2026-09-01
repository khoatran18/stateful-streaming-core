package vdf.vdt.streaming.parser.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vdf.vdt.streaming.model.event.GenericEvent;
import vdf.vdt.streaming.model.schema.ColumnDefinition;
import vdf.vdt.streaming.model.schema.DataType;
import vdf.vdt.streaming.model.schema.SourceVersionKey;
import vdf.vdt.streaming.model.schema.TableSchema;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// Validates and parses raw JSON event payloads against a dynamic TableSchema.
// Performs DFS flattening of nested JSON, enforces required fields (routing key and event_time),
// and type-casts leaf values per schema definitions.
public class DynamicEventValidator {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicEventValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Parses raw JSON bytes into a GenericEvent.
    // eventBytes - raw Kafka payload, headerKey - source/version from headers, schema - target schema
    public static GenericEvent validateAndParse(byte[] eventBytes, SourceVersionKey headerKey, TableSchema schema) throws IOException {
        LOG.debug("Parsing event bytes ({} bytes) for schema key={}", eventBytes.length, headerKey);
        JsonNode root = MAPPER.readTree(eventBytes);
        return validateAndParse(root, headerKey, schema);
    }

    // Main validation and parsing pipeline for a pre-parsed JsonNode.
    // Extracts routing key and event_time, then DFS-flattens and type-casts all fields.
    // root - parsed JSON tree, sourceVersionKey - schema lookup key, schema - target schema
    public static GenericEvent validateAndParse(JsonNode root, SourceVersionKey sourceVersionKey, TableSchema schema) {
        if (schema == null) {
            LOG.error("Schema is null for key={}, cannot parse event", sourceVersionKey);
            throw new IllegalArgumentException("Schema not found for: " + sourceVersionKey);
        }

        // Extract mandatory metadata
        JsonNode metadata = root.path("metadata");
        String eventTimeStr = metadata.path("event_time").asText(null);

        String keyFieldPath = schema.getKeyField();
        String customerId = extractFieldByPath(root, keyFieldPath);

        // Enforce required routing key
        if (customerId == null || customerId.isBlank()) {
            LOG.warn("Missing routing key at path '{}' for schema key={}", keyFieldPath, sourceVersionKey);
            throw new IllegalArgumentException("Missing required key field: " + keyFieldPath);
        }

        // Enforce event_time for watermark assignment
        if (eventTimeStr == null || eventTimeStr.isBlank()) {
            LOG.warn("Missing event_time for customerId={} schema={}", customerId, sourceVersionKey);
            throw new IllegalArgumentException("Missing required metadata.event_time");
        }

        Instant eventTime = parseIsoTimestamp(eventTimeStr);
        LOG.debug("Validated metadata: customerId={} eventTime={} schema={}", customerId, eventTime, sourceVersionKey);

        // DFS-flatten nested JSON and type-cast leaf values against schema
        Map<String, Object> parsedFields = new HashMap<>();
        flattenAndCast("", root, schema, keyFieldPath, parsedFields);
        LOG.debug("Flattened event: customerId={} fieldCount={}", customerId, parsedFields.size());

        return new GenericEvent(customerId, eventTime, sourceVersionKey, parsedFields);
    }

    // Traverses a dot-notation path (e.g. "metadata.customer_id") on a JsonNode tree.
    // Returns the leaf value as String, or null if any path segment is missing.
    public static String extractFieldByPath(JsonNode root, String dotPath) {
        if (root == null || dotPath == null || dotPath.isBlank()) {
            return null;
        }

        String[] tokens = dotPath.split("\\.");
        JsonNode currentNode = root;
        for (String token : tokens) {
            if (currentNode == null || !currentNode.isObject()) {
                return null;
            }
            currentNode = currentNode.path(token);
        }

        return (currentNode.isMissingNode() || currentNode.isNull()) ? null : currentNode.asText();
    }

    // Recursively (DFS) traverses a JSON object, building dot-separated field paths
    // and casting leaf values to native types defined in the schema.
    // Skips the "metadata" block and the routing key field to avoid duplication in the output map.
    // prefix - current dot-path prefix, node - current node, keyFieldPath - path to skip
    private static void flattenAndCast(String prefix, JsonNode node, TableSchema schema,
                                       String keyFieldPath, Map<String, Object> output) {
        if (!node.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode valueNode = entry.getValue();

            String fullPath = prefix.isEmpty() ? key : prefix + "." + key;

            if (valueNode.isObject()) {
                // Skip metadata block and routing key subtree
                if ("metadata".equals(fullPath) || fullPath.equals(keyFieldPath)) {
                    LOG.trace("Skipping reserved path: {}", fullPath);
                    continue;
                }
                flattenAndCast(fullPath, valueNode, schema, keyFieldPath, output);
            } else {
                ColumnDefinition colDef = schema.getColumn(fullPath);
                if (colDef != null && !valueNode.isNull()) {
                    // Schema match: cast to declared type
                    Object casted = castValue(valueNode, colDef.getDataType());
                    output.put(fullPath, casted);
                    LOG.trace("Field cast: path={} type={} value={}", fullPath, colDef.getDataType(), casted);
                } else if (!valueNode.isNull()) {
                    // No schema definition for this field: preserve as raw string
                    LOG.trace("Unknown field stored as raw string: path={}", fullPath);
                    output.put(fullPath, valueNode.asText());
                }
            }
        }
    }

    // Casts a JsonNode to the Java native type declared in the schema.
    // TIMESTAMP fields are parsed from ISO-8601 strings into Instant.
    private static Object castValue(JsonNode node, DataType dataType) {
        return switch (dataType) {
            case INT       -> node.asInt();
            case LONG      -> node.asLong();
            case FLOAT     -> (float) node.asDouble();
            case DOUBLE    -> node.asDouble();
            case BOOLEAN   -> node.asBoolean();
            case STRING    -> node.asText();
            case TIMESTAMP -> parseIsoTimestamp(node.asText());
            case OBJECT    -> node.toString();
        };
    }

    // Parses ISO-8601 timestamp strings into UTC Instants.
    // Supports offset-based formats (e.g., "2026-08-31T15:00:00+07:00")
    // and UTC zulu strings (e.g., "2026-08-31T08:00:00Z").
    private static Instant parseIsoTimestamp(String timestampStr) {
        try {
            return OffsetDateTime.parse(timestampStr, DateTimeFormatter.ISO_DATE_TIME).toInstant();
        } catch (Exception e) {
            LOG.trace("OffsetDateTime parse failed for '{}', retrying with Instant.parse", timestampStr);
            return Instant.parse(timestampStr);
        }
    }
}