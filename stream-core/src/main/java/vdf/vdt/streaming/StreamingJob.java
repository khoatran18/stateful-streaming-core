package vdf.vdt.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vdf.vdt.streaming.config.ConfigLoader;
import vdf.vdt.streaming.deserializer.KafkaDataDeserializationSchema;
import vdf.vdt.streaming.model.event.GenericEvent;
import vdf.vdt.streaming.model.event.RawKafkaDataEvent;
import vdf.vdt.streaming.processor.DynamicEventBroadcastProcessor;
import vdf.vdt.streaming.state.StateDescriptors;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Entry point for the Stateful Streaming Core Flink job.
 * Builds and executes the full dataflow pipeline:
 *   Schema stream (broadcast) → connect → Data stream → validate/parse → keyBy(customerId)
 */
public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        // 1. Init Flink execution environment with 200ms watermark interval
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(200);
        LOG.info("Flink execution environment initialized");

        // 2. Load application config and extract Kafka/app settings
        Map<String, Object> config = ConfigLoader.load();
        Map<String, Object> kafkaConfig = (Map<String, Object>) config.get("kafka");
        Map<String, Object> appConfig  = (Map<String, Object>) config.get("app");

        String appName         = (String) appConfig.getOrDefault("name", "Stateful Processing Core");
        String bootstrapServers = (String) kafkaConfig.get("bootstrap_servers");
        String groupId         = (String) kafkaConfig.get("group_id");
        List<String> eventTopics  = (List<String>) kafkaConfig.get("event_topics");
        List<String> schemaTopics = (List<String>) kafkaConfig.get("schema_topics");

        LOG.info("Starting Flink Job [{}] | bootstrap={} | eventTopics={} | schemaTopics={}",
                appName, bootstrapServers, eventTopics, schemaTopics);

        // 3. Schema Kafka source: reads from earliest to load all schema versions on startup
        KafkaSource<byte[]> schemaKafkaSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(schemaTopics)
                .setGroupId(groupId + "-schema")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<byte[]>() {
                    @Override
                    public byte[] deserialize(byte[] message) {
                        return message;
                    }
                })
                .build();
        LOG.info("Schema Kafka source configured: topics={} groupId={}-schema offset=earliest",
                schemaTopics, groupId);

        // 4. Broadcast schema stream to all parallel subtasks via SCHEMA_BROADCAST_STATE
        BroadcastStream<byte[]> schemaBroadcastStream = (BroadcastStream<byte[]>) env
                .fromSource(schemaKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Schema-Source")
                .name("Kafka-Schema-Source")
                .uid("kafka-schema-source")
                .broadcast(StateDescriptors.SCHEMA_BROADCAST_STATE);
        LOG.info("Schema broadcast stream created");

        // 5. Data (event) Kafka source: reads from latest offset for live event processing
        KafkaSource<RawKafkaDataEvent> dataKafkaSource = KafkaSource.<RawKafkaDataEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(eventTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaDataDeserializationSchema())
                .build();
        LOG.info("Data Kafka source configured: topics={} groupId={} offset=latest", eventTopics, groupId);

        // 6. Raw data stream (RawKafkaDataEvent, no watermarks yet — assigned after parsing)
        DataStream<RawKafkaDataEvent> rawDataStream = env
                .fromSource(dataKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Data-Source")
                .name("Kafka-Data-Source")
                .uid("kafka-data-source");

        // 7. Connect data stream with broadcast schema stream; validate and parse each event
        SingleOutputStreamOperator<GenericEvent> parseEventStream = rawDataStream
                .connect(schemaBroadcastStream)
                .process(new DynamicEventBroadcastProcessor())
                .name("Dynamic-Event-Validator-And-Parser")
                .uid("dynamic-event-validator-and-parser");
        LOG.info("BroadcastProcessFunction connected: DynamicEventBroadcastProcessor");

        // 8. Tap DLQ side-output: events that failed validation or schema lookup
        DataStream<String> dlqStream = parseEventStream
                .getSideOutput(DynamicEventBroadcastProcessor.DLQ_TAG);
        dlqStream.printToErr().name("DLQ-Console-Sink");
        LOG.info("DLQ side-output sink attached");

        // 9. Assign event-time watermarks (bounded out-of-orderness: 10s, idleness: 1min)
        DataStream<GenericEvent> keyedStream = parseEventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GenericEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
                                .withIdleness(Duration.ofMinutes(1))
                );
        LOG.info("Watermark strategy assigned: boundedOutOfOrderness=10s idleness=1min");

        // 10. Debug sink: key by customerId and print parsed events to stdout
        keyedStream.keyBy(GenericEvent::getCustomerId)
                .print()
                .name("Validated-Event-Sink");

        // 11. Submit and execute the Flink job
        LOG.info("Submitting Flink job: {}", appName);
        env.execute(appName);
    }
}
