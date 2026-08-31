# Development & Local Setup Guide

## 1. Requirements
* **JDK**: OpenJDK 21 (Temurin / Corretto / Oracle)
* **Build Tool**: Apache Maven 3.9+
* **Stream Platform**: Apache Flink 1.20 / Flink 2.0 & Apache Kafka 3.4+

---

## 2. IDE Configuration (IntelliJ IDEA)
Do Apache Flink sử dụng cơ chế tuần tự hóa Kryo/Chill với các cấu trúc dữ liệu nội bộ của JDK, khi chạy trực tiếp từ IDE trên nền **Java 17+ (Java 21)**, bắt buộc phải cấp quyền truy cập module thông qua **VM Options**:

### Thiết lập Run Configuration cho `StreamingJob`:
1. Mở **Run** → **Edit Configurations...**
2. Chọn configuration của **`StreamingJob`** (dưới mục *Application*).
3. Nhấp vào **Modify options** → Tích chọn **Add VM options** (`Alt + V`).
4. Dán chính xác chuỗi tham số sau vào ô **VM options**:

```text
--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

Bấm **Apply** → **OK**.

> **Mẹo (Global Template):** Để tự động áp dụng cho tất cả các class `main()` tạo mới, vào **Run** → **Edit Configurations...** → **Edit configuration templates...** (ở góc trái dưới) → **Application** → Thêm chuỗi cờ VM Options ở trên vào template.

---

## 3. Build & Package (Production Deployment)
Khi đóng gói để deploy lên cụm Flink Cluster phân tán (K8s / Standalone), thực thi lệnh:

```bash
mvn clean package -DskipTests
```

> **Lưu ý:** Khi submit JAR lên cụm phân tán thông qua Flink CLI hoặc Web UI, các cờ `--add-opens` đã được Flink runtime cấu hình sẵn trong `flink-conf.yaml`, không cần truyền thủ công.

Tài liệu tham khảo chính thức: [Apache Flink IDE Setup](https://nightlies.apache.org/flink/flink-docs-stable/docs/flinkDev/ide_setup/).

---

## 4. Local Infrastructure & Kafka Topics Setup
Để khởi chạy môi trường hạ tầng local (Kafka, Flink, Postgres, MongoDB, Debezium, Kafka UI):

### Khởi chạy Docker Infrastructure:
```bash
docker compose up -d
```

### Các Topic Kafka mặc định:
Hạ tầng sẽ tự động khởi tạo các Kafka topics sau (thông qua `docker/kafka/init_topic.sh`):
* `source.event`: Topic nhận dữ liệu stream event đầu vào.
* `source.rule`: Topic nhận dynamic rules (Broadcast State).
* `source.schema`: Topic định nghĩa schema (compacted topic).
* `connect-configs`, `connect-offsets`, `connect-statuses`: Topics dành cho Debezium Kafka Connect.

### Các Cổng Dịch Vụ Local:
* **Kafka Broker (Host)**: `localhost:9092`
* **Kafka UI**: `http://localhost:8085`
* **Flink Web UI**: `http://localhost:8084`
* **Mongo Express**: `http://localhost:8081`
* **PostgreSQL**: `localhost:5432` (User: `admin`, DB: `main_db`)
