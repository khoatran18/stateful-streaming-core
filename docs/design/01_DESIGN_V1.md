# Design Stateful Streaming Core — V1

---

## 1. Tổng Quan Thiết Kế

Hệ thống **Stateful Streaming Core** được thiết kế dựa trên Apache Flink và Apache Kafka nhằm thực hiện tính toán luồng có trạng thái thời gian thực và phân khúc khách hàng (CDP - Customer Data Platform) ở quy mô cực lớn. 

Kiến trúc hệ thống cho phép xử lý song song các luồng sự kiện dữ liệu (Data Events), luồng cấu hình dữ liệu (Schema Definitions) và luồng định nghĩa quy tắc nghiệp vụ (Rules). Toàn bộ logic kiểm tra và cập nhật trạng thái tích lũy của khách hàng (`Customer State`) diễn ra cục bộ per `customer_id` trên Flink State Backend (RocksDB).

### Sơ Đồ Kiến Trúc Hệ Thống

![Overview Pipeline](../../assets/overview_pipeline.png)

### Luồng Dữ Liệu Tổng Thể

1. **Schema Topic**: Nạp các bản tin định nghĩa cấu trúc dữ liệu theo từng cặp `(source, schema_version)`. Parser đọc và broadcast cấu trúc schema tới toàn bộ các Flink Task Instances.
2. **Data Topic**: Nạp các bản tin sự kiện CDP thời gian thực. Parser đọc header của event để lấy `source` và `schema_version`, khớp với schema tương ứng trong Broadcast State để parse payload, kiểm tra tính hợp lệ và trích xuất `customer_id`.
3. **Rule Topic**: Nạp các quy tắc nghiệp vụ (AST Rules) phát sinh từ Database qua cơ chế CDC (Debezium). Parser đọc rule, phân tích và đưa vào bộ chỉ mục **Indexed Rules**.
4. **Processing, Rule Evaluation & State Update**:
   - **Tra cứu chỉ mục & Đánh giá Rule**: Tra cứu nhanh bộ chỉ mục **Indexed Rules** dựa trên `trigger_criteria` để lấy ra danh sách các Rule ứng viên, tiến hành kiểm tra `condition_tree` của các Rule ứng viên đối với `customer_id` tương ứng.
   - **Đánh giá & Kết xuất đầu ra**:
     - Nếu thỏa mãn $\rightarrow$ Phát kết quả thành công (**Rules pass**) ra đầu ra (**Database / Kafka Topic**).
     - Nếu không thỏa mãn $\rightarrow$ Ghi nhận kết quả không đạt (**Rules failed**) phục vụ audit/monitoring.
   - **Cập nhật Customer State**: Cuối cùng, dữ liệu của Event được cập nhật vào **Customer State** bao gồm:
     - **Profile State**: Lưu giá trị mới nhất của các thuộc tính (Ví dụ: `user_status = 'ACTIVE'`, `age = 25`).
     - **Ring Buffer**: Lưu mảng tích lũy các chỉ số tổng hợp theo cửa sổ thời gian (Ví dụ: `SUM(spend_amount)` trong 1h gần nhất).

---

## 2. Thiết Kế Chi Tiết

---

### 2.1. Parse Data & Quản Lý Schema Broadcast

Thiết kế phân tách việc xử lý Schema và Data Event nhằm hỗ trợ thay đổi cấu hình dữ liệu linh hoạt mà không làm dừng job streaming.

```text
Schema Topic ──────► BroadcastProcessFunction ──(BroadcastState: "source:schema_version")──┐
                                                                                           │ Tra cứu Schema
Data Topic ────────► Data Ingestion Parser ───(Đọc Header: source, schema_version)─────────┴──► keyBy(customer_id)
```

#### 1. Luồng Cấu Hình Schema (Schema Broadcast Stream)
- Bản tin Schema từ **Schema Topic** chứa danh sách các trường, kiểu dữ liệu (`INT`, `LONG`, `FLOAT`, `DOUBLE`, `STRING`, `BOOLEAN`, `TIMESTAMP`) và danh mục trường (`static_categorical`, `static_numeric`, `dynamic_categorical`, `dynamic_numeric`) tương ứng với cặp `(source, schema_version)`.
- Flink sử dụng `BroadcastProcessFunction` để tiếp nhận bản tin Schema, phát rộng (broadcast) tới tất cả các parallel subtask instances và lưu vào `BroadcastState` với khóa định danh dạng chuỗi: `"source:schema_version"`.

#### 2. Luồng Parse Sự Kiện (Data Event Parsing)
- Khi một bản tin sự kiện từ **Data Topic** đi vào hệ thống:
  1. Parser trích xuất thông tin `source` và `schema_version` trực tiếp từ Kafka Message Headers (hoặc metadata của bản tin).
  2. Tra cứu định nghĩa Schema tương ứng trong `BroadcastState` bằng key `"source:schema_version"`.
  3. Thực hiện deserialize và validate các thuộc tính trong payload theo kiểu dữ liệu đã khai báo.
- **Xử lý ngoại lệ**:
  - Bản tin thiếu `customer_id`, `event_time`, hoặc sai lệch định dạng schema $\rightarrow$ Đẩy trực tiếp ra **Error Sink / Dead Letter Queue (DLQ)**.
  - Bản tin có `event_time` đến trễ quá ngưỡng `allowedLateness` cho phép của Watermark $\rightarrow$ Đẩy ra **Late Event Sink**.
  - Bản tin hợp lệ $\rightarrow$ Gắn thông tin Schema Metadata và đẩy sang bước phân nhóm theo khách hàng `keyBy(customer_id)`.

---

### 2.2. Thiết Kế MapState Lưu Trọng Cục Cho Mỗi Customer ID (`Customer State`)

Để đảm bảo hiệu năng tính toán cửa sổ tổng hợp (`SUM`, `COUNT`, `MIN`, `MAX`, `AVG`) theo thời gian thực cho hàng triệu khách hàng, dữ liệu tích lũy của mỗi khách hàng được lưu trữ dưới dạng Keyed State (`MapState<String, double[]>`) nằm trực tiếp trên RocksDB State Backend của từng Flink Subtask.

```text
MapState per customer_id:
┌─────────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────┐
│ Key (String)                                                │ Value (double[480] = 96 buckets × 5 metrics)           │
├─────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ "spend_amount|store_id==A"                                  │ [sum_0, count_0, min_0, max_0, avg_0, sum_1, ...]      │
│ "spend_amount|store_id==A&channel==MOBILE"                  │ [sum_0, count_0, min_0, max_0, avg_0, sum_1, ...]      │
└─────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────┘
```

#### 1. Cấu Trúc Key (Combined Field & Filter Condition Key)
Key trong `MapState` đại diện cho một chuỗi chỉ số tổng hợp cửa sổ ứng với một trường tính toán và bộ điều kiện lọc tương ứng.

- **Cơ chế**:
  - Mỗi biểu thức cửa sổ thực hiện tính toán tổng hợp trên một trường số liệu (`field_name`), kết hợp với bộ điều kiện lọc (`filter`) để xác định các bản tin sự kiện hợp lệ đưa vào cửa sổ.
  - Điều kiện `filter` có thể bao gồm một hoặc nhiều điều kiện kết hợp từ nhiều trường dữ liệu động khác nhau. Danh sách điều kiện lọc này được chuẩn hóa thành chuỗi đại diện `filter_conditions_string`.
- **Định dạng Key**: `field_name|filter_conditions_string`
  - `field_name`: Tên trường dữ liệu số trực tiếp tham gia tính toán tổng hợp.
  - `filter_conditions_string`: Chuỗi chuẩn hóa danh sách các điều kiện lọc (được sắp xếp theo quy chuẩn cố định).
- **Ví dụ**:
  - Tính tổng chi tiêu tại cửa hàng `STORE_A`:  
    `Key = "spend_amount|store_id==STORE_A"`
  - Tính tổng chi tiêu kênh `MOBILE_APP` tại cửa hàng `STORE_A` (loại trừ giao dịch `REFUND`):  
    `Key = "spend_amount|login_channel==MOBILE_APP&store_id==STORE_A&transaction_type!=REFUND"`

#### 2. Cấu Trúc Value (Primitive Array `double[96 × 5]`)
Value ứng với mỗi Key là một mảng số thực nguyên thể 1 chiều `double[]` gồm **480 phần tử**, đại diện cho $96 \text{ buckets} \times 5 \text{ phép toán tổng hợp}$.

- **96 Buckets**: Tương ứng với khoảng thời gian lưu vết tối đa **24 giờ** (24h $\times$ 4 buckets/giờ = 96 buckets, mỗi bucket có độ dài 15 phút).
- **5 Phép tổng hợp**: Mỗi bucket dành riêng 5 ô liên tiếp để lưu trữ 5 chỉ số: `[SUM, COUNT, MIN, MAX, AVG]`.
  - Slot $i$ có chỉ số bắt đầu `base = i * 5`.
  - `base + 0`: `SUM`
  - `base + 1`: `COUNT`
  - `base + 2`: `MIN`
  - `base + 3`: `MAX`
  - `base + 4`: `AVG` (được cập nhật $AVG = \frac{SUM}{COUNT}$ ngay khi ghi bucket).

#### 3. Lý Do Sử Dụng Primitive Array `double[]` Thay Vì POJO (Object Wrapper)

1. **Triệt tiêu gánh nặng Garbage Collection (GC)**:
   - Trong môi trường xử lý hàng triệu sự kiện/giây, việc tạo các POJO Java Object trung gian (như `BucketValue`, `AggMetric`) cho từng bucket sẽ tạo ra hàng tỷ Object ngắn hạn trên Java Heap. Điều này dẫn đến hiện tượng treo hệ thống do Garbage Collector phải hoạt động liên tục (Stop-The-World pauses).
   - Việc dùng mảng nguyên thể `double[]` cố định kích thước giúp loại bỏ hoàn toàn việc tạo Object mới trong vòng lặp runtime.

2. **Tối ưu hóa Serialization/Deserialization (SerDe) & RocksDB I/O**:
   - RocksDB State Backend lưu trữ dữ liệu dưới dạng các mảng byte (`byte[]`).
   - POJO yêu cầu các serializer phức tạp (Jackson, Kryo, hoặc TypeSerializer của Flink) sử dụng Java Reflection, tiêu tốn rất nhiều CPU cycle.
   - Mảng `double[]` có cấu trúc bộ nhớ cố định (480 $\times$ 8 bytes = 3,840 bytes), được Flink Memory Segment serialize/deserialize trực tiếp thành khối byte liên tục mà không cần biến đổi đối tượng.

3. **Tối ưu L1/L2 CPU Cache Line Alignment & SIMD Vectorization**:
   - 480 phần tử `double` nằm xếp liền kề nhau trên vùng nhớ liên tục.
   - Khi CPU thực hiện đọc/ghi hoặc tính toán cửa sổ sliding qua $N$ buckets, toàn bộ dữ liệu của mảng được nạp trực tiếp vào CPU L1/L2 Cache Line, tránh hiện tượng trượt cache (Cache Misses) và truy xuất con trỏ (Pointer Chasing).
   - Cho phép trình biên dịch JVM và CPU tự động áp dụng các chỉ thị SIMD (Single Instruction Multiple Data) để thực hiện tính toán song song trên mảng.

---

### 2.3. Thiết Kế Rule Index (Chỉ Mục Ngược - Inverted Index)

Khi số lượng Rule nghiệp vụ lên tới hàng chục nghìn hoặc hàng triệu, việc duyệt qua từng Rule đối với mỗi event đầu vào có độ phức tạp $O(M)$ (với $M$ là tổng số Rule), gây nghẽn nghiêm trọng CPU. 

Hệ thống sử dụng cấu trúc **Chỉ Mục Ngược (Inverted Index)** giúp lọc nhanh tập Rule ứng viên với thời gian tra cứu $O(1)$, thay vì phải duyệt kiểm tra toàn bộ $M$ Rule.

```text
Rule Topic ──► Rule Parser ──► Trích xuất trigger_criteria per (source, schema_version)
                                                        │
                                                        ▼
                                       ┌──────────────────────────────────┐
                                       │     Inverted Index Registry      │
                                       │    Key: (source, version)        │
                                       ├──────────────────────────────────┤
                                       │  field_A ──► [Rule_1, Rule_5]    │
                                       │  field_B ──► [Rule_2, Rule_5]    │
                                       └──────────────────────────────────┘
```

#### 1. Nguyên Lý Lập Chỉ Mục Dựa Trên `trigger_criteria`
- Chỉ mục ngược được xây dựng hoàn toàn dựa trên trường bộ lọc kích hoạt `trigger_criteria` trong cấu trúc JSON của Rule.
- **Phân tách theo bộ nguồn (`source, schema_version`)**:
  - Mỗi bộ `(source, schema_version)` sở hữu một **bộ chỉ mục riêng biệt (Dedicated Index Registry)**.
  - Điều này giúp cô lập hoàn toàn các Rule thuộc về các nguồn sự kiện hoặc phiên bản schema khác nhau, tránh tra cứu nhầm lẫn giữa các luồng dữ liệu.

#### 2. Cấu Trúc Bộ Chỉ Mục
Khi một Rule mới được nạp từ **Rule Topic**:
1. Parser đọc và phân tích mảng `trigger_criteria`.
2. Trích xuất tất cả các cặp `(source, schema_version)` mà Rule đăng ký lắng nghe.
3. Trong mỗi `(source, schema_version)`, duyệt qua danh sách các trường tham gia vào mảng 2 chiều điều kiện `conditions` (DNF).
4. Đăng ký `rule_id` vào danh sách chỉ mục ứng với các trường đó:
   - Bảng ánh xạ: `Map<String, Set<String>>` (Key là `field_name` hoặc `field_name=value`, Value là tập hợp các `rule_id`).

#### 3. Quy Trình Tra Cứu (Lookup & Candidate Selection Flow)
Khi một Data Event mang thông tin `(source_X, version_Y)` đi qua bước Parser:

1. **Định tuyến Chỉ mục**: Engine truy cập trực tiếp vào bộ chỉ mục của `(source_X, version_Y)`.
2. **Trích xuất Trường**: Liệt kê danh sách các trường dữ liệu xuất hiện trong bản tin event.
3. **Lọc Ứng Viên Phân Cấp (Candidate Filtering)**:
   - Tra cứu tập các Rule chứa điều kiện đối với các trường có mặt trong event.
   - Để tối ưu hiệu năng hơn nữa, hệ thống tính toán tần suất xuất hiện (Document Frequency - DF) của các trường trong tập Rule và chọn **2 trường hiếm nhất (DF thấp nhất)** làm trục chỉ mục chính.
   - Thực hiện phép giao tập hợp (Set Intersection) giữa 2 tập `rule_id` của 2 trường hiếm nhất để thu được tập **Candidate Rules**.
4. **Đánh Giá Chi Tiết**:
   - Sau khi hoàn tất bước lọc candidate từ chỉ mục với thời gian $O(1)$, Event Processor tiến hành kiểm tra `condition_tree` chỉ trên tập **Candidate Rules** đã được thu hẹp (thay vì phải đánh giá toàn bộ $M$ Rule trong hệ thống).
