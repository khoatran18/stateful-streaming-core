package vdf.vdt.streaming.generator.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerClient {
    private final KafkaProducer<String, String> producer;

    public KafkaProducerClient(String bootstrapServers) {
        Properties props = new Properties();
        // Cấu hình địa chỉ Kafka Broker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Cấu hình Serializer cho Key và Value (dùng String cho cả 2)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Cấu hình độ bền tin nhắn (acks = 1 nghĩa là chờ leader nhận được message là OK)
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Tối ưu hóa throughput cho việc gửi dữ liệu liên tục
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // Gom batch nhỏ để gửi nhanh hơn

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Gửi message dạng JSON string lên Kafka Topic với Key cụ thể
     * @param topic Tên topic Kafka
     * @param key Khóa của message (thường là entity ID để đảm bảo cùng Key vào chung một Partition)
     * @param jsonValue Nội dung message dạng chuỗi JSON
     */
    public void send(String topic, String key, String jsonValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message to Kafka topic " + topic + ": " + exception.getMessage());
            }
        });
    }

    /**
     * Đóng kết nối Kafka Producer khi dừng chương trình
     */
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}