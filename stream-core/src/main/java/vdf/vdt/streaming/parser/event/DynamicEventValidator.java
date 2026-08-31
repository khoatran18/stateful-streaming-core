package vdf.vdt.streaming.parser.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/**
 * Validates, flattens, and type-casts incoming JSON event payloads against a dynamic TableSchema.
 */
public class DynamicEventValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses and validates a raw JSON byte array against the provided TableSchema.
     */
    public static GenericEvent validateAndParse(byte[] eventBytes, SourceVersionKey headerKey, TableSchema schema) throws IOException {
        JsonNode root = MAPPER.readTree(eventBytes);
        return validateAndParse(root, headerKey, schema);
    }

    /**
     * Main validation and parsing pipeline for a pre-parsed JsonNode tree.
     *
     * @param root              Root JSON node of the event
     * @param sourceVersionKey  Source/version key from message headers (null if embedded in payload)
     * @param schema            Target schema for type casting and validation
     * @return Materialized and strongly typed GenericEvent
     */
    public static GenericEvent validateAndParse(JsonNode root, SourceVersionKey sourceVersionKey, TableSchema schema) {
        // Fail-fast guard: A valid schema definition is required to process the event
        if (schema == null) {
            throw new IllegalArgumentException("Schema not found for: " + sourceVersionKey);
        }

        String dynamicKeyName = schema.getKeyField();

        // 1. Mandatory metadata extraction and validation
        JsonNode metadata = root.path("metadata");
        String customerId = metadata.path(dynamicKeyName).asText(null);
        String eventTimeStr = metadata.path("event_time").asText(null);

        // Enforce required primary routing key (customer_id)
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("Missing required metadata.customer_id");
        }
        // Enforce event-time watermark timestamp
        if (eventTimeStr == null || eventTimeStr.isBlank()) {
            throw new IllegalArgumentException("Missing required metadata.event_time");
        }

        Instant eventTime = parseIsoTimestamp(eventTimeStr);

        // 2. Recursively flatten nested JSON properties and cast values according to schema
        Map<String, Object> parsedFields = new HashMap<>();
        flattenAndCast("", root, schema, parsedFields);

        return new GenericEvent(customerId, eventTime, sourceVersionKey, parsedFields);
    }

    /**
     * Recursively traverses JSON objects (DFS) to flatten nested keys into dot-separated paths
     * and applies type conversions matching the schema definition.
     *
     * @param prefix Current nested path prefix (e.g., "order.payment")
     * @param node   Current JSON node under inspection
     * @param schema Target schema used for data type lookups
     * @param output Destination map accumulating flattened field paths and cast values
     */
    private static void flattenAndCast(String prefix, JsonNode node, TableSchema schema, Map<String, Object> output) {
        if (!node.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode valueNode = entry.getValue();

            // Construct dot-delimited field path
            String fullPath = prefix.isEmpty() ? key : prefix + "." + key;

            if (valueNode.isObject()) {
                // Skip the metadata block as it is parsed separately in Step 1
                if ("metadata".equals(fullPath)) {
                    continue;
                }
                flattenAndCast(fullPath, valueNode, schema, output);
            } else {
                ColumnDefinition colDef = schema.getColumn(fullPath);
                if (colDef != null && !valueNode.isNull()) {
                    // Match found in schema: cast to declared technical data type
                    output.put(fullPath, castValue(valueNode, colDef.getDataType()));
                } else if (!valueNode.isNull()) {
                    // Schema-less field fallback: preserve value as raw text
                    output.put(fullPath, valueNode.asText());
                }
            }
        }
    }

    /**
     * Maps and converts a Jackson JsonNode to the appropriate Java native type.
     *
     * @param node     JSON value node
     * @param dataType Target data type defined in ColumnDefinition
     * @return Strongly typed Java primitive wrapper or object
     */
    private static Object castValue(JsonNode node, DataType dataType) {
        return switch (dataType) {
            case INT -> node.asInt();
            case LONG -> node.asLong();
            case FLOAT -> (float) node.asDouble();
            case DOUBLE -> node.asDouble();
            case BOOLEAN -> node.asBoolean();
            case STRING -> node.asText();
            case TIMESTAMP -> parseIsoTimestamp(node.asText());
            case OBJECT -> node.toString();
        };
    }

    /**
     * Parses ISO-8601 timestamp strings into UTC Instants.
     * Supports formats with zone offsets (e.g., "2026-08-31T15:00:00+07:00")
     * and fallback UTC standard strings (e.g., "2026-08-31T08:00:00Z").
     */
    private static Instant parseIsoTimestamp(String timestampStr) {
        try {
            // Parse timestamps containing timezone/offset information
            return OffsetDateTime.parse(timestampStr, DateTimeFormatter.ISO_DATE_TIME).toInstant();
        } catch (Exception e) {
            // Fallback for standard UTC zulu timestamps
            return Instant.parse(timestampStr);
        }
    }
}