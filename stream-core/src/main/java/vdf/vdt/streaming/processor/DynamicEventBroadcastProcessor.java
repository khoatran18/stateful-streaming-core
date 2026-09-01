package vdf.vdt.streaming.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vdf.vdt.streaming.model.event.GenericEvent;
import vdf.vdt.streaming.model.event.RawKafkaDataEvent;
import vdf.vdt.streaming.model.schema.SourceVersionKey;
import vdf.vdt.streaming.model.schema.TableSchema;
import vdf.vdt.streaming.parser.event.DynamicEventValidator;
import vdf.vdt.streaming.parser.schema.DynamicSchemaJsonParser;
import vdf.vdt.streaming.state.StateDescriptors;

// BroadcastProcessFunction that manages schema state and validates incoming events.
//
// Broadcast side: parses raw schema bytes into TableSchema and stores in broadcast state
// keyed by SourceVersionKey.
// Data side: resolves the matching schema for each RawKafkaDataEvent and delegates
// parsing to DynamicEventValidator. Invalid or unresolvable events go to DLQ side-output.
public class DynamicEventBroadcastProcessor extends
        BroadcastProcessFunction<RawKafkaDataEvent, byte[], GenericEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicEventBroadcastProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Side-output tag for events that fail validation or schema lookup
    public static final OutputTag<String> DLQ_TAG = new OutputTag<String>("event-dlq-side-output") {};

    // Processes a single data-stream event.
    // Resolves source/version from Kafka headers (with metadata fallback),
    // looks up the schema in broadcast state, then validates and parses the event.
    // On any failure the raw payload is forwarded to DLQ.
    @Override
    public void processElement(
            RawKafkaDataEvent rawEvent,
            ReadOnlyContext ctx,
            Collector<GenericEvent> collector) throws Exception {

        byte[] eventBytes = rawEvent.getPayload();
        try {
            JsonNode root = MAPPER.readTree(eventBytes);
            JsonNode metadata = root.path("metadata");

            // Prefer header values; fall back to embedded metadata fields
            String source = rawEvent.getSource();
            if (source == null || source.isBlank()) {
                source = metadata.path("source").asText(null);
                LOG.debug("Source not in header, resolved from metadata: {}", source);
            }
            String version = rawEvent.getVersion();
            if (version == null || version.isBlank()) {
                version = metadata.path("schema_version").asText("v1.0");
                LOG.debug("Version not in header, resolved from metadata: {}", version);
            }

            if (source == null || source.isBlank()) {
                LOG.warn("Event missing source identifier, routing to DLQ");
                ctx.output(DLQ_TAG, "Missing metadata.source: " + new String(eventBytes));
                return;
            }

            SourceVersionKey key = SourceVersionKey.of(source, version);
            TableSchema schema = ctx.getBroadcastState(StateDescriptors.SCHEMA_BROADCAST_STATE).get(key);

            if (schema == null) {
                LOG.warn("No schema found in broadcast state for key={}, routing to DLQ", key);
                ctx.output(DLQ_TAG, "Schema missing for key: " + key);
                return;
            }

            LOG.debug("Processing event: customerId={} schemaKey={}",
                    DynamicEventValidator.extractFieldByPath(root, schema.getKeyField()), key);

            GenericEvent event = DynamicEventValidator.validateAndParse(root, key, schema);
            collector.collect(event);

        } catch (Exception e) {
            LOG.error("Failed to process event, routing to DLQ. Error: {}", e.getMessage(), e);
            ctx.output(DLQ_TAG, "Corrupted event payload: " + new String(eventBytes));
        }
    }

    // Processes an incoming schema config message from the broadcast stream.
    // Parses raw bytes into a TableSchema and stores it in broadcast state.
    @Override
    public void processBroadcastElement(
            byte[] schemaBytes,
            Context ctx,
            Collector<GenericEvent> collector) throws Exception {

        try {
            TableSchema schema = DynamicSchemaJsonParser.parse(schemaBytes);
            SourceVersionKey key = schema.getKey();
            ctx.getBroadcastState(StateDescriptors.SCHEMA_BROADCAST_STATE).put(key, schema);
            LOG.info("Schema broadcast state updated: key={} totalFields={}", key, schema.getTotalFields());
        } catch (Exception e) {
            LOG.error("Failed to parse schema broadcast message, skipping. Error: {}", e.getMessage(), e);
        }
    }
}
