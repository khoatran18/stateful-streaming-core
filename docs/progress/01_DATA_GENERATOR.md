# Stateful Streaming Core - Development Progress: Data & Rule Generator (`data-generator`)

## 1. Tổng quan Module & Kiến trúc phát sinh dữ liệu (Architecture Overview)

Module `data-generator` có nhiệm vụ sinh dữ liệu sự kiện thời gian thực (CDP Events) theo Dual Schema và sinh bộ quy tắc điều kiện (AST Condition Tree Rules) phục vụ benchmark hệ thống Stateful Streaming Engine dựa trên Apache Flink & Apache Kafka.

```text
                               ┌─────────────────────────────────────────┐
                               │           SchemaPublisher               │
                               │  - Generates Schema A & B (36 fields)   │
                               │  - Writes to Kafka (source.schema)      │
                               │  - Saves JSON to data/schema/36/...     │
                               └────────────────────┬────────────────────┘
                                                    │
                                                    ▼
┌───────────────────────────────┐      ┌─────────────────────────┐      ┌───────────────────────────────┐
│        RuleGenerator          │      │     DataGenerator       │      │     KafkaProducerClient       │
│  - Generates AST Rules        │ ──►  │  - Continuous Event Loop│ ──►  │  - Sends events & headers     │
│  - Writes JSON to data/rules/ │      │  - Skew & Seeded Params │      │  - Topic: source.event        │
└───────────────────────────────┘      └─────────────────────────┘      └───────────────────────────────┘
```

---

## 2. Cấu trúc thư mục mã nguồn (Source Code Structure)

```text
data-generator/
├── pom.xml                                    # Quản lý dependencies (Kafka Client, Jackson, Flink, v.v.)
├── src/main/resources/
│   └── application.properties                 # Cấu hình Kafka Bootstrap Servers, topics, schema version, throughput
└── src/main/java/vdf/vdt/streaming/generator/
    ├── common/
    │   ├── Constants.java                     # Định nghĩa bộ trường Schema A/B (36 fields/schema), data types & categories
    │   ├── KafkaProducerClient.java           # Client gửi bản tin kèm Kafka Headers (source, version)
    │   └── PathUtils.java                     # Utility resolve đường dẫn đầu ra cho Schema và Rules
    ├── data_gen/
    │   ├── DataGenerator.java                 # Vòng lặp sinh event thời gian thực với cơ chế Data Skew & Partition Salting
    │   ├── SchemaPublisher.java               # Đẩy cấu hình Schema A & B lên Kafka topic & lưu file local khi startup
    │   ├── SchemaConsumerExample.java         # Module ví dụ đọc Schema từ Kafka topic và validate event
    │   └── Main.java                          # Entry point khởi chạy SchemaPublisher & DataGenerator continuous loop
    ├── model/
    │   ├── FieldDefinition.java               # POJO định nghĩa thuộc tính trường (name, type, category, constraints)
    │   ├── RuleModel.java                     # POJO cấu trúc quy tắc AST (rule_id, trigger_criteria, condition_tree)
    │   └── SchemaDefinition.java              # Wrapper nhẹ cho Schema Metadata & danh sách cột
    └── rule_gen/
        ├── RuleGenerator.java                 # Bộ sinh quy tắc AST đa tầng (Depth 1-2, multi-source trigger criteria)
        └── Main.java                          # Entry point khởi chạy quá trình sinh file quy tắc JSON vào data/rules/
```

---

## 3. Các tính năng chính và thiết kế kỹ thuật (Key Features & Engineering)

* **Dual Schema Strategy (Schema A & B - 36 Leaf Fields Each)**:
  * Mỗi Schema gồm 36 trường lá phủ rộng 7 kiểu dữ liệu: `STRING`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `TIMESTAMP`, `BOOLEAN`.
  * Phân chia theo 4 danh mục: `static_categorical`, `static_numeric`, `dynamic_categorical`, `dynamic_numeric`.
  * Hỗ trợ cấu trúc lồng nhau (Nested Dynamic Group): `debt` ở Schema A và `risk_signals` ở Schema B.
  * Tích hợp Kafka Message Headers: `source` (`A`/`B`) và `version` (`v2`).

* **Cơ chế sinh dữ liệu & Xử lý Data Skew**:
  * Các trường tĩnh (Static Fields): Sinh dữ liệu nhất quán bằng Seeded Random dựa theo `customer_id` (`Random(entityId * 31L)`).
  * Các trường động (Dynamic Fields): Sinh ngẫu nhiên theo thời gian thực (Global Unseeded Random).
  * **Partition Salting Key**: Gửi Key Kafka dưới dạng `ID_{entityId}_{salt}` (`salt` ngẫu nhiên `0..1000`) nhằm phân bổ đều lượng traffic tránh hotspot partition trên Kafka Broker, trong khi payload vẫn giữ nguyên `metadata.customer_id = ID_{entityId}`.

* **AST Rule Generation Engine (`rule_gen`)**:
  * Sinh quy tắc phân tầng AST với độ sâu 1-2 (`maxTreeDepth`), hỗ trợ pre-filter `trigger_criteria` từ nhiều nguồn (multi-source).
  * **`trigger_criteria` đa nguồn**: Random 1 hoặc 2 source (50/50); cả 2 source dùng chung `schema_version` đọc từ `application.properties` (không hardcode).
  * **`conditions` 2D (DNF)**: Mỗi trigger entry có `conditions` là list lồng list — outer list là OR, inner list là AND (1–2 outer group, mỗi group 1–3 điều kiện).
  * **`filter` trong Window là list**: Trước đây là 1 object đơn; giờ là `List<Map>` 1–3 điều kiện AND để lọc sự kiện lịch sử trước khi tổng hợp.
  * **`is_window` trên CONDITION node**: Mọi node `CONDITION` trong `condition_tree` giờ có trường `is_window: true/false` để phân biệt rõ biểu thức có window aggregation với so sánh scalar thông thường.
  * Phủ đầy đủ các toán tử theo `DATA_TYPE.md`: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN`.
  * Hỗ trợ 7 dạng biểu thức trong AST: Categorical, Numeric, Window Aggregation (`SUM`, `AVG`, `MAX`, `MIN`, `COUNT`), Boolean, Linear Combination, Timestamp, và Expression LHS.

---

## 4. Implementation Checklist & Tasks

* ✅ **Dual Schema A & B (36 fields)**: Định nghĩa 36 trường lá mỗi schema với 7 kiểu dữ liệu và 4 danh mục.
* ✅ **Nested Dynamic Groups**: Hỗ trợ nhóm lồng nhau `debt` (Schema A) và `risk_signals` (Schema B).
* ✅ **Kafka Producer & Salting Key**: Gửi event kèm Kafka Headers (`source`, `version`) và partition salting key `ID_{entityId}_{salt}`.
* ✅ **Rule Generation Engine**: Sinh quy tắc AST condition tree (độ sâu 1-2, window aggregations, full toán tử).
* ✅ **SchemaPublisher**: Publish schema lên Kafka topic `source.schema` và ghi file JSON local khi startup.
* ✅ **`trigger_criteria` đa nguồn đúng spec**: 50/50 random 1 hoặc 2 source; `schema_version` đọc từ `application.properties`.
* ✅ **`conditions` 2D (DNF)**: Outer list OR — inner list AND; generator tạo 1–2 outer group, mỗi group 1–3 điều kiện.
* ✅ **`filter` window là list**: `buildWindowFilterList` trả `List<Map>` với 1–3 điều kiện AND thay vì 1 object đơn lẻ.
* ✅ **`is_window` flag trên CONDITION node**: Phân biệt rõ window aggregation vs. scalar comparison trong `condition_tree`.

---

## 5. Cấu hình & Kết quả đầu ra (Outputs & Configuration)

* **Đường dẫn file lưu trữ**:
  * Schemas: `data/schema/36/<version>/<timestamp>/schema_a.json` & `schema_b.json`
  * Rules: `data/rules/<totalRules>/<timestamp>.json`
* **Tham số cấu hình chính (`application.properties`)**:
  * `kafka.bootstrap-servers`: `localhost:9092`
  * `kafka.topic.raw-event`: `source.event`
  * `kafka.topic.schema`: `source.schema`
  * `schema.version`: `v2`
  * `reqPerSecond`: `10` (Tốc độ sinh event/giây)
  * `idRange`: `100` (Quy mô tập customer ID)
  * `totalRules`: `1000` (Số lượng quy tắc cần sinh)
