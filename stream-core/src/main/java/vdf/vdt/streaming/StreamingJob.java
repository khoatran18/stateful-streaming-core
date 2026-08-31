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

public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        // 1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(200);

        // 2.
        Map<String, Object> config = ConfigLoader.load();
        Map<String, Object> kafkaConfig = (Map<String, Object>) config.get("kafka");
        Map<String, Object> appConfig = (Map<String, Object>) config.get("app");

        String appName = (String) appConfig.getOrDefault("name", "Stateful Processing Core");
        String bootstrapServers = (String) kafkaConfig.get("bootstrap_servers");
        String groupId = (String) kafkaConfig.get("group_id");

        List<String> eventTopics = (List<String>) kafkaConfig.get("event_topics");
        List<String> schemaTopics = (List<String>) kafkaConfig.get("schema_topics");

        LOG.info("Starting Flink Job [{}] with Kafka Bootstrap: {}", appName, bootstrapServers);

        // 3. Init Kafka source for Schema Stream
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

        // 4. Create Broadcast Stream for Schema
        BroadcastStream<byte[]> schemaBroadcastStream = (BroadcastStream<byte[]>) env
                .fromSource(schemaKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Schema-Source")
                .name("Kafka-Schema-Source")
                .uid("kafka-schema-source")
                .broadcast(StateDescriptors.SCHEMA_BROADCAST_STATE);

        // 5. Init Kafka Source for Data Stream
        KafkaSource<RawKafkaDataEvent> dataKafkaSource = KafkaSource.<RawKafkaDataEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(eventTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaDataDeserializationSchema())
                .build();

        // 6. Create Data Stream for Data
        DataStream<RawKafkaDataEvent> rawDataStream = env
                .fromSource(dataKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Data-Source")
                .name("Kafka-Data-Source")
                .uid("kafka-data-source");

        // 7. Connect Data Stream with Schema Broadcast Stream
        SingleOutputStreamOperator<GenericEvent> parseEventStream = rawDataStream
                .connect(schemaBroadcastStream)
                .process(new DynamicEventBroadcastProcessor())
                .name("Dynamic-Event-Validator-And-Parser")
                .uid("dynamic-event-validator-and-parser");

        // 8.
        DataStream<String> dqlStream = parseEventStream
                .getSideOutput(DynamicEventBroadcastProcessor.DLQ_TAG);

        dqlStream.printToErr().name("DQL-Console-Sink");

        // 9.
        DataStream<GenericEvent> keyedStream = parseEventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GenericEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // 10. Debug
        keyedStream.keyBy(GenericEvent::getCustomerId)
                .print()
                .name("Validated-Event-Sink");

        // 11.
        env.execute(appName);

    }

}
