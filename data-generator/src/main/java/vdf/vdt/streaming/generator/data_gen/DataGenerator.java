package vdf.vdt.streaming.generator.data_gen;

import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.common.KafkaProducerClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class DataGenerator {
    private final KafkaProducerClient kafkaClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Integer> schemaSizes = List.of(20, 50, 100, 150, 200);

    public DataGenerator(KafkaProducerClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    public void startGenerating(int reqPerSecond, int idRange, String kafkaTopic) {
        System.out.println("Starting Data Generator with " + reqPerSecond + " req/s, ID Range: " + idRange);
        Random random = new Random();
        long intervalMillis = 1000L / Math.max(1, reqPerSecond);

        int schemaIndex = 0;
        while (true) {
            long startTime = System.currentTimeMillis();

            // Luân phiên chọn 1 trong 5 schema: 20, 50, 100, 150, 200 trường
            int totalFields = schemaSizes.get(schemaIndex);
            schemaIndex = (schemaIndex + 1) % schemaSizes.size();

            try {
                Map<String, Object> event = generateEvent(totalFields, idRange, random);
                String jsonStr = objectMapper.writeValueAsString(event);

                // Gửi vào kafka với key là id để đảm bảo gom cụm theo Keyed State
                kafkaClient.send(kafkaTopic, event.get("id").toString(), jsonStr);

            } catch (Exception e) {
                e.printStackTrace();
            }

            // Kiểm soát tốc độ req/s
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed < intervalMillis) {
                try {
                    Thread.sleep(intervalMillis - elapsed);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private Map<String, Object> generateEvent(int totalFields, int idRange, Random random) {
        Map<String, Object> event = new LinkedHashMap<>();

        // 1. Cột ID có thể lặp lại trong khoảng idRange
        long entityId = random.nextInt(idRange) + 1;
        event.put("id", "ID_" + entityId);
        event.put("timestamp", System.currentTimeMillis());

        // 2. Tỷ lệ: 1/3 cột định danh, 2/3 cột số
        List<String> catCols = Constants.CATEGORICAL_COLUMNS.get(totalFields);
        List<String> numCols = Constants.NUMERIC_COLUMNS.get(totalFields);

        // Sinh dữ liệu định danh
        for (String col : catCols) {
            event.put(col, "val_" + random.nextInt(10));
        }

        // Sinh dữ liệu số với khoảng giá trị đa dạng
        for (String col : numCols) {
            if (col.contains("age")) {
                event.put(col, random.nextInt(80) + 10); // Khoảng 10 - 90
            } else if (col.contains("amount") || col.contains("balance")) {
                event.put(col, 2000000 + (10000000 - 2000000) * random.nextDouble()); // Khoảng 2tr - 10tr
            } else {
                event.put(col, random.nextDouble() * 100); // Khoảng 0 - 100
            }
        }

        return event;
    }
}