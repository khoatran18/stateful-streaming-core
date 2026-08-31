# Stateful Streaming Core - Development Progress: Dynamic Data Parser

## 1. Tổng quan kiến trúc luồng dữ liệu (Dataflow Architecture)

### 1.1. Dataflow Pipeline Overview
```text
[config.schema-topic] ──► Flink Kafka Source ──► DynamicSchemaJsonParser ──► BroadcastStream (SCHEMA_BROADCAST_STATE)
                                                                                  │
                                                                                  ▼ (Broadcast Connect)
[source.event-topics] ──► Flink Kafka Source ──► RawKafkaDataEvent ────────► DynamicEventBroadcastProcessor
                                                                                  │
                                                      ┌───────────────────────────┴───────────────────────────┐
                                                      ▼ (Valid Schema Match)                                  ▼ (Schema Missing / Parse Error)
                                                [GenericEvent]                                              [DLQ SideOutput]
                                                      │                                                       │
                                                      ▼                                                       ▼
                                       Assign Watermarks & Timestamps                                   Error Sink / PrintToErr
                                                      │
                                                      ▼
                                              KeyBy(customer_id)
                                                      │
                                                      ▼
                                       [Downstream Stateful Processing Core]
```

### 1.2. Dynamic Schema Validation & Parsing Sequence
```text
[1. Schema Config JSON]
         │
         ▼
[DynamicSchemaJsonParser] ──creates──► [TableSchema] (Chứa Map<String, ColumnDefinition>)
                                             │
                                             │ (Lưu vào Schema Registry / Cache / Flink State)
                                             ▼
[2. Raw Event JSON] ───────► [DynamicEventValidator]
                                    │
                                    ├── Đọc TableSchema để lấy kiểu dữ liệu
                                    ├── Bóc tách, đệ quy flatten các trường lồng nhau
                                    └── Ép kiểu theo DataType
                                    │
                                    ▼
                             [GenericEvent] (Đưa vào Engine xử lý luồng)
```

---

## 2. Cấu trúc thư mục mã nguồn đã hoàn thiện

```text
stream-core/
├── pom.xml                                    # Quản lý dependencies (Flink 1.20, Kafka Connector, Jackson BOM 2.18.2, RoaringBitmap)
├── src/main/resources/
│   ├── config.dev.yml                         # Cấu hình Kafka brokers, topics, app configs
│   └── logback.xml                            # Cấu hình logging FileAppender & ConsoleAppender
└── src/main/java/vdf/vdt/streaming/
    ├── config/
    │   └── ConfigLoader.java                  # Load cấu hình ứng dụng từ file YAML
    │
    ├── deserializer/
    │   ├── KafkaDataDeserializationSchema.java# Trích xuất Header Kafka (source, version) + Payload thành RawKafkaDataEvent
    │   └── KafkaSchemaDeserializationSchema.java # (Tùy chọn) Đọc Schema payload/header từ topic schema
    │
    ├── model/
    │   ├── event/
    │   │   ├── RawKafkaDataEvent.java         # Flink POJO chứa metadata header và raw byte payload
    │   │   └── GenericEvent.java              # Flink POJO chuẩn hóa: customerId, eventTime, sourceVersionKey, HashMap fields
    │   └── schema/
    │       ├── DataType.java                  # Enum: INT, LONG, FLOAT, DOUBLE, STRING, TIMESTAMP, BOOLEAN, OBJECT
    │       ├── FieldCategory.java             # Enum: dynamic_numeric, static_categorical, dynamic_categorical, static_numeric
    │       ├── SourceVersionKey.java          # Record: (source, version) làm composite key định danh Schema
    │       ├── ColumnDefinition.java          # Meta định nghĩa chi tiết cột (fieldPath, dataType, category)
    │       └── TableSchema.java               # Lưu trữ Metadata Schema hoàn chỉnh và Map<String, ColumnDefinition>
    │
    ├── parser/
    │   ├── event/
    │   │   └── DynamicEventValidator.java     # DFS đệ quy flatten JSON, bóc tách routing key, parse ISO-8601 và ép kiểu động
    │   └── schema/
    │       └── DynamicSchemaJsonParser.java   # DFS parse JSON Schema từ topic cấu hình thành TableSchema
    │
    ├── processor/
    │   └── DynamicEventBroadcastProcessor.java# BroadcastProcessFunction: Nạp Schema vào Broadcast State, validate & parse Event, tách DLQ
    │
    ├── state/
    │   └── StateDescriptors.java              # Định nghĩa MapStateDescriptor<SourceVersionKey, TableSchema>
    │
    └── StreamingJob.java                      # Khởi tạo Pipeline, gán Watermark, kích hoạt MiniCluster và thực thi Dataflow
```

---

## 3. Các hạng mục đã giải quyết và tối ưu

* **Dynamic Schema & Event Ingestion Pipeline**:
  * Xây dựng thành công cơ chế cập nhật Schema thời gian thực không cần restart job qua Flink Broadcast State Pattern.
  * Hỗ trợ giải mã JSON đa tầng bằng thuật toán DFS Flatten: ánh xạ các trường lồng nhau thành chuỗi dot-delimited (ví dụ: `risk_signals.fraud_probability_score`) và ép kiểu native theo `DataType`.
  * Hỗ trợ trích xuất dynamic `key_field` theo đường dẫn dot-notation bất kỳ và loại bỏ trùng lặp khóa chính trong map `fields`.
  * Phân luồng dữ liệu lỗi/thiếu schema sang **SideOutput DLQ (Dead Letter Queue)** mà không làm crash stream chính.

* **Khắc phục xung đột JVM & Build System**:
  * **Java 21 Module Encapsulation**: Cấu hình `--add-opens` cho toàn bộ các module `java.base/java.util`, `java.lang`, `java.time`, `java.nio` phục vụ tuần tự hóa Kryo/Chill.
  * **Jackson Dependency Hell**: Chuẩn hóa toàn bộ hệ sinh thái Jackson (`core`, `databind`, `jsr310`, `annotations`) về bản đồng nhất `2.18.2` qua `jackson-bom` trong `dependencyManagement`.
  * **Flink POJO Optimization**: Bổ sung default constructor và full getter/setter cho các model `RawKafkaDataEvent`, `GenericEvent` giúp tối ưu Serialization.
  * Sửa lỗi cấu hình `FileAppender` trên Logback và bổ sung `flink-connector-base`.

---

## 4. Trạng thái kiểm thử Runtime (Verification)

* Khởi chạy MiniCluster local thành công trên JDK 21.
* Schema Consumer gán 3 partitions và đọc cấu hình từ `earliest` offset.
* Data Consumer gán 3 partitions, nhận bản tin `latest` offset và parse ra `GenericEvent` chính xác.
* Stream đã sẵn sàng tại chốt `keyedStream = parseEventStream.assignTimestampsAndWatermarks(...).keyBy(GenericEvent::getCustomerId)`.
