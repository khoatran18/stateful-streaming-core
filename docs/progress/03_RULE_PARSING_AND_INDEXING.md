# Stateful Streaming Core - Progress 03: Rule Parsing & Inverted Indexing

## 1. Scope & Objectives

Xây dựng lớp cấu trúc dữ liệu và logic đánh chỉ mục nghịch đảo (Inverted Index) cho toàn bộ tập luật (Rules) của hệ thống Streaming CDP. Mục tiêu là chuyển đổi JSON cấu hình Rules thành bộ lọc siêu tốc ($O(1)$ filtering) dựa trên cấu trúc `RoaringBitmap`.

**Kết quả đầu ra kỳ vọng:**
* Phân tách thành công JSON Rule thành 3 tầng Models: `RuleDefinition`, `ConditionTree`, `TriggerCriteria`.
* Khởi tạo `RuleRegistry` để tạo ánh xạ 1-1 giữa chuỗi `rule_id` và giá trị nguyên `bit_index` (int: 0, 1, 2...).
* Thiết kế và xây dựng thành phần `BitsetInvertedIndex` sử dụng thư viện `RoaringBitmap`.
* Triển khai giải thuật "Rare Field Selector" để tối ưu hóa quá trình lookup rules cho mỗi bản tin đầu vào.

### 1.1. Luồng Parse Rule & Lập Chỉ Mục RoaringBitmap Inverted Index
```text
[Rule Config JSON]
        │
        ▼
[RuleJsonParser]
        │
        ├── 1. Parse Rule Metadata & Time Windows
        ├── 2. Parse ConditionTree (Composite Pattern: AND / OR / LeafNode)
        └── 3. Bóc tách TriggerCriteria (Static Criteria)
        │
        ▼
[RuleDefinition]
        │
        ├── 1. Đăng ký vào [RuleRegistry] ────────► Cấp phát bit_index (0, 1, 2...)
        │                                                  │
        │                                                  ▼
        └── 2. Đẩy vào [BitsetInvertedIndex] ◄──── [BitIndex: int]
                     │
                     ├── Đọc các điều kiện tĩnh (Field = Value)
                     ├── Tính toán Tần suất xuất hiện (Document Frequency - DF)
                     └── Cập nhật Map<Field, Map<Value, RoaringBitmap>>
```

### 1.2. Luồng Flink Streaming Dataflow kết hợp Inverted Index Filtering
```text
[config-rules-topic] ──► Flink Kafka Source ──► (RuleJsonParser + BitsetIndexer) ──► BroadcastStream (RULE_STATE + INDEX_STATE)
                                                                                                  │
                                                                                                  │ (Broadcast)
[Validated GenericEvent Stream] ─────────────► keyBy(customer_id) ────────────────────────► KeyedBroadcastProcessFunction
                                                                                                  │
                                                                                                  ├── 1. RareFieldSelector: Chọn 2 fields hiếm nhất
                                                                                                  ├── 2. RoaringBitmap.and(): Lọc nhanh candidate rules (O(1))
                                                                                                  ├── 3. Truy xuất Candidate Rules từ State
                                                                                                  ├── 4. Evaluate chi tiết Condition Tree
                                                                                                  └── 5. Emit Matched Alert / Trigger sang Downstream
```

---

## 2. Component & File Structure (Dự kiến)

```text
stream-core/src/main/java/vdf/vdt/streaming/
├── model/
│   └── rule/
│       ├── RuleDefinition.java          # POJO gốc chứa RuleMetadata, TriggerCriteria, WindowExpression
│       ├── TriggerCriteria.java         # Tiêu chí kích hoạt (Trigger Type, Conditions)
│       ├── ConditionNode.java           # Interface gốc cho cây điều kiện (LeafNode, LogicalNode)
│       └── Operator.java                # Enum cho phép so sánh (EQ, GT, LT, IN, CONTAINS...)
│
├── registry/
│   └── RuleRegistry.java                # Quản lý ánh xạ: Map<String (RuleId), Integer (BitIndex)>
│
└── index/
    ├── BitsetInvertedIndex.java         # Lưu trữ Map<Field, Map<Value, RoaringBitmap>>
    └── RareFieldSelector.java           # Logic tính DF (Document Frequency) để lấy 2 trường lọc tĩnh
```

---

## 3. Core Technical Design (Thiết kế kỹ thuật)

### 3.1. Rule Data Models & Parsing
JSON của mỗi Rule chứa 2 phần phức tạp nhất cần parse:
* **Trigger Criteria (Cây điều kiện tĩnh)**: Phục vụ Inverted Index. Cần thiết kế `ConditionNode` theo pattern Composite (`LeafNode` biểu diễn biểu thức đơn; `LogicalNode` biểu diễn AND/OR).
* **Window & Aggregation Expression**: Các định nghĩa về time window (tumbling, sliding), hàm tổng hợp (`COUNT`, `SUM`) để xử lý trên State.

### 3.2. Rule Registry (Ánh xạ Bit Index)
`RoaringBitmap` chỉ làm việc với số nguyên dương (unsigned 32-bit int). Do `rule_id` của hệ thống là chuỗi UUID hoặc String, ta bắt buộc phải có một `RuleRegistry` để:
* `allocate(rule_id) -> int`: Cấp phát một bit index duy nhất cho mỗi rule mới.
* `getRuleId(bit_index) -> String`: Ánh xạ ngược lại kết quả lọc bitset ra mã rule ban đầu để chạy Condition Tree chi tiết.

### 3.3. Bitset Inverted Index Structure
Cấu trúc lưu trữ Map nhiều tầng trên Flink Broadcast State:

```java
Map<String, Map<Object, RoaringBitmap>> invertedIndex;
// Field Path -> (Value -> Set of Rule Bit Indexes)
// Ví dụ:
// "customer_segment" -> { "ENTERPRISE": [Rule 0, Rule 5], "STANDARD": [Rule 1, Rule 2] }
```

### 3.4. Thuật toán Rare Field Selector (Lọc nhanh O(1))
Thay vì lookup mọi điều kiện tĩnh của rule, hệ thống chọn ra tối đa 2 trường (Fields) tĩnh có tần suất xuất hiện trên các rule (Document Frequency - DF) thấp nhất làm "Index Fields".

* **Lý do**: Lấy trường hiếm nhất để phép giao `RoaringBitmap.and()` trả về số lượng candidate rule nhỏ nhất ngay lập tức (thu nhỏ scope phải chạy cây điều kiện nặng).

---

## 4. Current Progress & Tasks

* ⬜ Thiết kế cấu trúc các class trong `vdf.vdt.streaming.model.rule.*`.
* ⬜ Xây dựng bộ Parser để đọc JSON sinh ra `RuleDefinition` & `ConditionTree`.
* ⬜ Thiết kế `RuleRegistry`.
* ⬜ Xây dựng class `BitsetInvertedIndex` hỗ trợ thêm/xóa/cập nhật bit map.
* ⬜ Tích hợp tính toán Tần suất (DF) cho thuật toán Rare Field Selector.

---

## 5. Technical Notes & Issues Solved

*(Ghi chú các lỗi serialization của Kryo, Jackson JsonNode hoặc RoaringBitmap phát sinh trong quá trình code Phase 02)*

* **(Pending)** Đảm bảo `RoaringBitmap` serializable bằng Kryo khi chạy trên Broadcast State.
