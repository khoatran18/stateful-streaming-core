package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.KafkaProducerClient;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) props.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String bootstrapServers = props.getProperty("kafka.bootstrap-servers", "localhost:9092");
        String kafkaTopic = props.getProperty("kafka.topic", "stream-input-events");
        int reqPerSecond = Integer.parseInt(props.getProperty("generator.default.req-per-second", "50"));
        int idRange = Integer.parseInt(props.getProperty("generator.default.id-range", "1000"));

        KafkaProducerClient kafkaClient = new KafkaProducerClient(bootstrapServers);
        DataGenerator dataService = new DataGenerator(kafkaClient);

        System.out.println(">>> Starting Data Generator pushing to topic: " + kafkaTopic);
        dataService.startGenerating(reqPerSecond, idRange, kafkaTopic);
    }
}