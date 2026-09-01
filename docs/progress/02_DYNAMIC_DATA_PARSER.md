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

## 3. Core Technical Design (Thiết kế kỹ thuật)

### 3.1. Dynamic Schema Broadcast Architecture
Hệ thống áp dụng **Flink Broadcast State Pattern** để quản lý metadata schema linh hoạt trong thời gian thực:
* **Schema Stream**: Tiêu thụ cấu hình từ topic `config.schema` với `earliest` offset để bảo đảm toàn bộ schema được nạp đầy đủ khi job khởi động. Dữ liệu được parse thành `TableSchema` và broadcast tới tất cả các parallel subtasks.
* **Schema State**: Quản lý bằng `MapStateDescriptor<SourceVersionKey, TableSchema>`. Khóa `SourceVersionKey(source, version)` đóng vai trò composite identifier, cho phép chạy song song nhiều phiên bản schema của nhiều nguồn dữ liệu khác nhau mà không bị xung đột.

### 3.2. Recursive JSON Flattening & Dynamic Type-Casting (DFS)
Thay vì sử dụng schema tĩnh (compile-time POJO/Avro), parser giải mã động cây JSON dựa trên `TableSchema`:
* **DFS Hierarchy Traversal**: Sử dụng thuật toán duyệt theo chiều sâu để flatten toàn bộ cấu trúc JSON lồng nhau nhiều tầng thành chuỗi định danh phẳng phân cách bằng dấu chấm (ví dụ: `risk_signals.fraud_probability_score`).
* **Strong Native Type-Casting**: Mỗi trường dữ liệu sau khi duyệt lá được đối chiếu với `ColumnDefinition` trong `TableSchema` để ép kiểu trực tiếp về các kiểu dữ liệu nguyên thủy tương ứng (`INT`, `LONG`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, `STRING`), giúp tối ưu hóa bộ nhớ và tốc độ tính toán downstream.

### 3.3. Flexible Routing Key Extraction
* Cơ chế bóc tách routing key hỗ trợ đường dẫn phân cấp bất kỳ thông qua hàm `extractFieldByPath` (ví dụ: `metadata.customer_id` hoặc `user_info.account_id`).
* Thuật toán tự động bỏ qua (skip) trường khóa chính trong quá trình flatten để tránh dư thừa và trùng lặp dữ liệu trong `GenericEvent.fields`.

### 3.4. Fault Isolation & SideOutput Dead Letter Queue (DLQ)
* **Non-blocking Stream**: Các bản tin bị sai định dạng JSON, không khớp `SourceVersionKey` hoặc thiếu schema hợp lệ sẽ được chuyển hướng sang luồng phụ `DLQ SideOutput` để ghi log cảnh báo hoặc lưu trữ điều tra.
* Luồng xử lý chính (`GenericEvent Stream`) luôn đảm bảo tính liên tục, không bị gián đoạn hay crash JVM bởi dữ liệu lỗi cục bộ.

---

## 4. Implementation Checklist & Tasks

* ✅ **Dependencies Management**: Thiết lập `pom.xml`, cấu hình `jackson-bom:2.18.2` trong `dependencyManagement` để triệt tiêu `NoSuchMethodError`.
* ✅ **Config Loader**: Tạo cấu trúc config YAML và `ConfigLoader` đọc tham số động.
* ✅ **Data Models**: Thiết kế Data Models chuẩn Flink POJO (`RawKafkaDataEvent`, `GenericEvent`, `TableSchema`, `ColumnDefinition`).
* ✅ **DynamicSchemaJsonParser**: Viết `DynamicSchemaJsonParser` hỗ trợ duyệt DFS để flatten các trường dữ liệu lồng nhau.
* ✅ **DynamicEventValidator**: Viết `DynamicEventValidator` hỗ trợ trích xuất dynamic `key_field` dạng dot-notation (và fallback `metadata.customer_id`), ép kiểu `DataType` tương ứng.
* ✅ **DynamicEventBroadcastProcessor**: Xây dựng processor tích hợp Flink Broadcast State Pattern.
* ✅ **DLQ SideOutput**: Cấu hình SideOutput DLQ để cô lập các bản tin rác/lỗi mà không làm crash pipeline.
* ✅ **Java 21 `--add-opens`**: Cấu hình JVM `--add-opens` cho Java 21 tương thích cơ chế tuần tự hóa Kryo/Chill.
* ✅ **Local Verification**: Kiểm thử luồng dữ liệu trên MiniCluster local: Broadcast schema nạp thành công, Event stream được parse và `keyBy(customer_id)` hoạt động chính xác.

---

## 5. Key Technical Decisions & Issues Solved

* **Java 21 Module Encapsulation Fix**:
  * **Vấn đề**: Thư viện Chill/Kryo gặp `InaccessibleObjectException` khi truy cập private fields của JDK collection (`Arrays.asList`).
  * **Giải pháp**: Thêm các cờ `--add-opens=java.base/java.util=ALL-UNNAMED`, `java.lang`, `java.time`, `java.nio` vào VM Options của IDE và template.

* **Jackson Dependency Hell**:
  * **Vấn đề**: Flink Kafka connector kéo gián tiếp Jackson 2.15.2 gây xung đột binary với 2.18.2.
  * **Giải pháp**: Chuyển `jackson-bom` vào khối `<dependencyManagement>` trong `pom.xml` để đồng bộ toàn bộ module Jackson về 2.18.2.

* **Dynamic Key Routing**:
  * **Giải pháp**: Hàm `extractFieldByPath` cho phép lấy routing key ở bất kỳ tầng lồng nhau nào của JSON và tự động bỏ qua không duplicate key vào map `fields`.
