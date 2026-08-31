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

public class DynamicEventBroadcastProcessor extends
        BroadcastProcessFunction<RawKafkaDataEvent, byte[], GenericEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicEventBroadcastProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final OutputTag<String> DLQ_TAG = new OutputTag<String>("event-dlq-side-output") {};

    @Override
    public void processElement(
            RawKafkaDataEvent rawEvent,
            ReadOnlyContext ctx,
            Collector<GenericEvent> collector) throws Exception {

        byte[] eventBytes = rawEvent.getPayload();
        try {
            JsonNode root = MAPPER.readTree(eventBytes);
            JsonNode metadata = root.path("metadata");

            String source = rawEvent.getSource();
            if (source == null || source.isBlank()) {
                source = metadata.path("source").asText(null);
            }
            String version = rawEvent.getVersion();
            if (version == null || version.isBlank()) {
                version = metadata.path("schema_version").asText("v1.0");
            }

            if (source == null || source.isBlank()) {
                ctx.output(DLQ_TAG, "Missing metadata.source: " + new String(eventBytes));
                return;
            }

            SourceVersionKey key = SourceVersionKey.of(source, version);
            TableSchema schema = ctx.getBroadcastState(StateDescriptors.SCHEMA_BROADCAST_STATE).get(key);

            if (schema == null) {
                ctx.output(DLQ_TAG, "Schema missing for key: " + key);
                return;
            }

            GenericEvent event = DynamicEventValidator.validateAndParse(root, key, schema);
            collector.collect(event);
        } catch (Exception e) {
            LOG.error("Failed to process element", e);
            ctx.output(DLQ_TAG, "Corrupted event payload: " + new String(eventBytes));
        }

    }

    @Override
    public void processBroadcastElement(
            byte[] schemaBytes,
            Context ctx,
            Collector<GenericEvent> collector) throws Exception {

        try {
            TableSchema schema = DynamicSchemaJsonParser.parse(schemaBytes);
            ctx.getBroadcastState(StateDescriptors.SCHEMA_BROADCAST_STATE).put(schema.getKey(), schema);
            LOG.info("Broadcast Schema updated successfully for key: {}", schema.getKey());
        } catch (Exception e) {
            LOG.error("Failed to parse and broadcast schema", e);
        }

    }
}
