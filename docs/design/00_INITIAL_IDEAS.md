# Phác Thảo Kiến Trúc Hệ Thống Stateful Streaming Core

Document này phác thảo mô hình thiết kế tổng quan và các phương án tổ chức dữ liệu/trạng thái (State Management) cho hệ thống **Stateful Streaming Core** phát triển trên **Apache Flink**.

---

## 1. Tổng Quan Kiến Trúc & Định Tuyến (KeyBy Partitioning)

Hệ thống được thiết kế để xử lý luồng dữ liệu sự kiện thời gian thực (Real-time Streaming Event) cho hàng triệu khách hàng độc lập.

### 1.1 Khóa Phân Vùng (`keyBy`)
- **Khóa chính**: `customer_id` (trích xuất từ `metadata.customer_id` trong bản tin event).
- **Mục đích**:
  - Đảm bảo toàn bộ trạng thái tích lũy (Window Aggregation State, Profile State) của cùng một khách hàng được định tuyến về duy nhất một **Operator Instance / TaskManager Slot** xử lý.
  - Loại bỏ hoàn toàn nhu cầu đồng bộ giữa các node, đảm bảo khả năng mở rộng ngang (Horizontal Scalability) tuyệt đối theo số lượng khách hàng.

---

## 2. Thiết Kế Quản Lý Trạng Thái (State Management Design)

Trong mỗi phân vùng `customer_id`, hệ thống duy trì các `MapState` của Flink để lưu trữ dữ liệu cửa sổ tính toán (Ring Buffer) và thông tin thuộc tính hiện tại.

### 2.1 Các Phương Án Quản Lý Cửa Sổ Tổng Hợp (Window Ring Buffer State)

Để hỗ trợ tính toán các cửa sổ trượt (Sliding Window) và cửa sổ nhảy (Tumbling Window) với 5 hàm tổng hợp (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`), thời gian được chia thành các lát cắt cố định (**Buckets**). 

#### **Cơ Chế Ring Buffer Vòng Tròn (Fixed 96 Buckets & Circular Overwrite)**
- **Giới hạn cửa sổ tối đa (Max Duration)**: 24 giờ ($24 \times 60 = 1440$ phút).
- **Kích thước một Bucket**: 15 phút.
- **Số lượng Bucket cố định**: $N = \frac{1440}{15} = 96$ buckets (chỉ số slot từ `0` đến `95`).
- **Công thức tính Bucket Slot Index**:
  $$\text{slot_index} = \left(\left\lfloor \frac{\text{event_timestamp_ms}}{15 \times 60 \times 1000} \right\rfloor\right) \pmod{96}$$
- **Cơ chế Ghi Đè Liên Tục (Circular Overwrite)**:
  - Mỗi bucket slot lưu kèm mốc thời gian gốc (`bucket_epoch_id` = $\lfloor \frac{\text{event_timestamp_ms}}{15 \times 60 \times 1000} \rfloor$).
  - Khi event mới đến rơi vào `slot_index`, hệ thống kiểm tra nếu `bucket_epoch_id` của slot hiện tại trong State nhỏ hơn `current_epoch_id` (đã cũ hơn 24h), slot đó sẽ tự động được **xóa sạch/reset** và ghi đè dữ liệu mới.
  - **Lợi ích**: Dung lượng bộ nhớ State cho mỗi khách hàng luôn cố định tuyệt đối ở mức tối đa 96 buckets, loại bỏ việc bộ nhớ phình to và triệt tiêu chi phí scan/delete rác nền.

Hệ thống đề xuất **2 phương án tổ chức `MapState`**:

#### **Phương Án 1: MapState theo `slot_index` (Key = 0..95)**
- **Cấu trúc MapState**: `MapState<Integer, BucketAggregations>`
  - **Key**: `slot_index` ($0 \rightarrow 95$).
  - **Value**: Một đối tượng/Map chứa `bucket_epoch_id`, thông tin tất cả các `field` xuất hiện trong bucket đó kèm theo giá trị tổng hợp của 5 hàm (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`).
- **Chi tiết Value**:
  ```java
  class BucketAggregations {
      long bucketEpochId; // Dùng để xác định bucket cũ hơn 24h cần ghi đè
      Map<String, FieldAggregates> fieldAggs;
  }

  class FieldAggregates {
      double sum;
      long count;
      double avg; // hoặc tính avg = sum / count
      double min;
      double max;
  }
  ```
- **Đánh giá & Đặc điểm**:
  - **Ưu điểm**: Key trong MapState là số nguyên từ 0 đến 95 cố định. Quản lý ghi đè trực tiếp tại key `slot_index`, cực kỳ gọn gàng.
  - **Nhược điểm**: Serialization / Deserialization cost khi đọc/ghi đối tượng `BucketAggregations` chứa tất cả field trong bucket.

---

#### **Phương Án 2: MapState với Composite Key (`slot_index + field`)**
- **Cấu trúc MapState**: `MapState<String, AggregationPojo>`
  - **Key**: Chuỗi kết hợp `slot_index + ":" + field_name` (Ví dụ: `"42:daily_spend_total_vnd"`).
  - **Value**: Một POJO Class tinh gọn chứa `bucket_epoch_id` và 5 biến tương ứng cho 5 phép toán tổng hợp.
- **Chi tiết POJO**:
  ```java
  public class AggregationPojo {
      private long bucketEpochId; // Xác định để reset nếu là chu kỳ 24h mới
      private double sum;
      private long count;
      private double avg;
      private double min;
      private double max;

      public void updateOrReset(long currentEpochId, double value) {
          if (this.bucketEpochId < currentEpochId) {
              // Bucket cũ đã quá 24h -> Ghi đè mới
              this.bucketEpochId = currentEpochId;
              this.sum = value;
              this.count = 1;
              this.avg = value;
              this.min = value;
              this.max = value;
          } else {
              // Cùng bucket 15p -> Tích lũy tăng dần
              this.sum += value;
              this.count += 1;
              this.avg = this.sum / this.count;
              this.min = Math.min(this.min, value);
              this.max = Math.max(this.max, value);
          }
      }
  }
  ```
- **Đánh giá & Đặc điểm**:
  - **Ưu điểm**: Cập nhật dữ liệu tức thì cho từng field với IO nhỏ nhất. Khi ghi đè, việc kiểm tra `bucketEpochId < currentEpochId` tự động reset giá trị mà không cần lệnh delete entry.
  - **Nhược điểm**: Số lượng key trong MapState = $96 \times \text{số lượng field}$.

---

### 2.2 MapState Lưu Trạng Thái Hiện Tại (Customer Profile State)

Bên cạnh dữ liệu cửa sổ Ring Buffer, hệ thống duy trì **1 MapState riêng biệt** phục vụ lưu trữ trạng thái nền/thuộc tính hiện tại của khách hàng:

- **Cấu trúc MapState**: `MapState<String, Object>` (hoặc `ValueState<CustomerProfilePojo>`).
- **Dữ liệu lưu trữ**:
  - **Static Categorical**: Các thuộc tính danh mục cố định (ví dụ: `gender`, `home_province`, `device_type`, `user_status`).
  - **Static Numeric**: Các chỉ số cơ bản/nền tảng của khách hàng (ví dụ: `age`, `tenure_months`, `credit_limit_vnd`, `nps_score_baseline`).
  - **Latest Dynamic Attributes**: Giá trị mới nhất của các thuộc tính động phát sinh từ event gần nhất.
- **Vai trò trong đánh giá Rule**:
  - Cung cấp dữ liệu instant cho các điều kiện không nằm trong cửa sổ Ring Buffer (ví dụ: kiểm tra `age >= 18 AND gender == 'FEMALE'` khi đánh giá một Rule).

---

### 2.3 Phân Tích Sizing Bộ Nhớ Cho MapState (Phương Án 2: Key = bucket_id + field_id)

Dưới đây là bài toán tính toán chi tiết dung lượng lưu trữ (Disk/SSD) và bộ nhớ RAM cần thiết cho **Phương án 2** khi triển khai trên **`EmbeddedRocksDBStateBackend`**.

#### **1. Thiết Lập Bài Toán**
- **Số lượng thực thể ($N$)**: $1.000.000$ `customer_id`.
- **Số trường/metric ($F$)**: $300$ fields.
- **Số bucket ($B$)**: $24 \times 4 = 96$ buckets (cửa sổ 24 giờ, bước trượt 15 phút).
- **State Backend**: `EmbeddedRocksDBStateBackend`.

---

#### **2. Cấu Trúc Và Kích Thước Từng Cell Dữ Liệu Trong RocksDB**
Trong Flink RocksDB, mỗi phần tử của `MapState` được lưu trữ dưới dạng một cặp Key-Value nhị phân độc lập:

- **RocksDB Key Layout**:
  KeyGroup Prefix (2 bytes) + `customer_id` (~20 bytes) + StateName Overhead (~8 bytes) + User Key (`slot_index` + `field_id`) (~12 bytes).
  $$\text{Kích thước Key} \approx 42\text{ bytes}$$
- **RocksDB Value Layout (`AggregationPojo`)**:
  `bucketEpochId` (Long: 8 bytes) + `sum` (Double: 8 bytes) + `count` (Long: 8 bytes) + `min` (Double: 8 bytes) + `max` (Double: 8 bytes) + flags/header (~4 bytes).
  $$\text{Kích thước Value} \approx 44\text{ bytes}$$
- **RocksDB Internal Overhead**:
  Mỗi entry trong SST file của RocksDB tốn thêm metadata (sequence number, type, CRC, key length, block index): $\approx 16\text{ bytes}$.
  $$\text{Dung lượng 1 cell thô} \approx 42 + 44 + 16 = 102\text{ bytes} \approx 100\text{ bytes}$$

---

#### **3. Sizing Dung Lượng Đĩa (Disk Storage) Theo Kịch Bản**

##### **Trường hợp 1: Worst-Case (Dense State - Mọi khách hàng có đủ 300 trường trong 96 buckets)**
- **Tổng số bản ghi**:
  $$\text{Total entries} = 1.000.000 \times 300 \times 96 = 28.800.000.000\text{ entries (28.8 tỷ records)}$$
- **Dung lượng thô**:
  $$28.8 \times 10^9 \times 100\text{ bytes} \approx 2.880\text{ GB} = 2.88\text{ TB}$$
- **Sau nén RocksDB (Snappy / LZ4, tỷ lệ nén $\approx 2.5:1$)**:
  $$\text{Dung lượng đĩa thực tế} \approx \mathbf{1.15\text{ TB} - 1.4\text{ TB}}$$

##### **Trường hợp 2: Kịch bản Thực tế (Sparse State - Dữ liệu phân bố thưa 10%)**
Trong thực tế CDP, một khách hàng chỉ phát sinh tương tác trên 10–20% số trường và chỉ ở một vài bucket trong ngày:
- **Hệ số thưa (Sparsity Factor)**: $10\%$.
- **Tổng số entries hoạt động**: $\approx 2.88\text{ tỷ entries}$.
- **Dung lượng đĩa sau nén**: $\mathbf{120\text{ GB} - 150\text{ GB}}$.

---

#### **4. Bản Chất Cơ Chế Lưu Trữ Và Tính Toán RAM Cho RocksDB State Backend**

Khác với `HashMapStateBackend` (lưu 100% dữ liệu trên Heap RAM), `EmbeddedRocksDBStateBackend` lưu trữ dữ liệu chính trên đĩa cứng (Out-of-Core / Disk-based):

```
┌────────────────────────────────────────────────────────┐
│              RAM (Heap & Off-Heap / Block Cache)       │
│   - Lưu trữ Working Set (Dữ liệu đang truy xuất nóng)  │
└───────────────────────────┬────────────────────────────┘
                            │ (Flush / Compaction / Evict)
                            ▼
┌────────────────────────────────────────────────────────┐
│              NVMe SSD Disk (SST Files)                 │
│   - Lưu trữ toàn bộ 100% State (1.15 TB - 1.4 TB)      │
└────────────────────────────────────────────────────────┘
```

##### **Công thức phân bổ RAM RocksDB**:
$$\text{RAM}_{\text{RocksDB}} = \text{Block Cache} + \text{Write Buffers (MemTable)} + \text{Index \& Bloom Filters}$$

1. **Block Cache (Bộ đệm đọc) — Chiếm ~60% - 70% RAM RocksDB**:
   - Chỉ lưu các bucket thời gian hiện tại/nóng của người dùng hoạt động.
   - Để đạt tỷ lệ trúng cache (Cache Hit Rate) $\ge 90\%$, kích thước Block Cache cần khoảng 3% – 5% dung lượng đĩa:
     $$1.150\text{ GB} \times 3\% - 5\% \approx \mathbf{35\text{ GB} - 55\text{ GB}}$$
2. **Write Buffers / MemTables (Bộ đệm ghi) — Chiếm ~20% - 25% RAM RocksDB**:
   - Đệm các lệnh ghi trước khi flush xuống đĩa. Cụm 32-64 slots tốn khoảng: $\mathbf{4\text{ GB} - 8\text{ GB}}$.
3. **Index & Bloom Filter Overhead — Chiếm ~10% RAM RocksDB**:
   - Tối ưu định vị point-lookup cho MapState mà không cần quét SST file: $\mathbf{3\text{ GB} - 5\text{ GB}}$.

---

#### **5. Tổng Hợp Bảng Phân Bổ Bộ Nhớ (Memory Budget) & Phần Cứng Khuyên Dùng**

| Thành phần RAM | Vai trò | Dung lượng khuyến nghị (Toàn cụm) |
| :--- | :--- | :--- |
| **RocksDB Block Cache** | Cache các bucket nóng của user đang hoạt động | 35 GB – 45 GB |
| **RocksDB Write Buffers** | Đệm ghi tức thời (MemTables) trước khi flush xuống disk | 6 GB – 10 GB |
| **RocksDB Index & Filters** | Tối ưu tìm kiếm point-lookup cho MapState | 4 GB – 6 GB |
| **Flink Task Heap RAM** | Chạy mã Java, Broadcast State, Data parsing | 16 GB – 24 GB |
| **Tổng RAM cụm TaskManager** | Đảm bảo throughput cao & latency < 10ms | **$\approx$ 64 GB – 85 GB** |

- **Dung lượng Disk SSD**: Dự phòng tối thiểu **2.5 TB – 3 TB NVMe SSD** (tính cả headroom cho RocksDB Compaction $\times 2$).
- **Ví dụ cấu hình cụm thực tế**:
  - Cụm gồm **4 TaskManagers**, mỗi TaskManager cấu hình:
    - **16 GB – 20 GB RAM**
    - **4 vCPU**
    - **500 GB – 1 TB NVMe SSD / node**

---

### 2.4 Phân Tích Các Giải Pháp Data Skew, Nút Thắt Kỹ Thuật & Hướng Tối Ưu Khả Thi

Bản chất luồng xử lý của hệ thống là một **chuỗi phụ thuộc tuần tự nghiêm ngặt (Strict Sequential Dependency)** trên từng bản tin event:

$$\text{Event đến} \longrightarrow \text{Trigger Filter} \longrightarrow \text{Inverted Index} \longrightarrow \text{Đánh giá Condition Tree (đọc Bucket cũ)} \longrightarrow \text{Cập nhật Event vào Bucket}$$

Do phụ thuộc thứ tự khắt khe này, **tất cả 10 cơ chế tối ưu/xử lý Data Skew dưới đây KHÔNG THỂ ÁP DỤNG** vì vi phạm trực tiếp logic nghiệp vụ và ràng buộc hệ thống.

#### **1. Phân Tích Chi Tiết 10 Giải Pháp Skew & Lý Do Không Khả Thi**

##### **1. Local Pre-Aggregation (Micro-flush 20–50ms)**
- **Nguyên lý & Thiết kế**: Sử dụng một operator gom cụm không phân vùng (`Non-Keyed Operator` hoặc `Tumbling Processing-Time Window` 20–50ms) đứng trước toán tử `keyBy(customer_id)`. Operator này duy trì một HashMap tạm thời trên Heap memory để gộp các bản tin event cùng `customer_id` phát sinh trong khoảng 20–50ms thành một bản tin tổng hợp Delta (accumulated metrics), giảm lưu lượng event đi vào Keyed State Operator.
- **Kịch bản kỳ vọng**: Giảm tần suất bản tin đi qua mạng và đi vào Keyed State Operator của Flink từ $N$ event/giây xuống còn tối đa $\frac{1000}{20} = 50$ bản tin Delta/giây per customer, giúp giảm tải CPU cho Keyed Subtask.
- **Phân tích lý do không khả thi**:
  1. **Mất ma trận ngữ cảnh event nguyên thủy**: Mỗi bản tin event mang mốc thời gian sự kiện (`metadata.event_time`), ID duy nhất (`metadata.event_id`) và tập thuộc tính thô/động (`payload`). Khi gộp 10 bản tin giao dịch vào 1 Delta record, các thuộc tính danh mục (`categorical`) riêng lẻ của từng giao dịch (như `device_type`, `login_channel`, `location`) bị mất đi hoặc bị ghi đè.
  2. **Sai lệch đánh giá Trigger Filter (`trigger_criteria`)**: Bộ lọc kích hoạt `trigger_criteria` yêu cầu kiểm tra điều kiện ngay trên từng event đơn lẻ (ví dụ: `giao dịch thực hiện qua TABLET VÀ thuộc kênh MOBILE_APP`). Khi micro-flush gộp dữ liệu, hệ thống không thể biết event nào trong 10 event mang thuộc tính gì để kích hoạt trigger chính xác.
  3. **Mất khả năng phát hiện vượt ngưỡng thời gian thực (Threshold Breach Failure)**: Nghiệp vụ yêu cầu nếu giao dịch thứ 3 trong chuỗi 10 giao dịch khiến tổng chi tiêu tích lũy vượt 10 triệu VND, tín hiệu cảnh báo phải bắn ra ngay lập tức ở giao dịch thứ 3. Nếu gộp micro-flush 50ms, tín hiệu bị trễ tới cuối cửa sổ flush, làm sai lệch logic Event-Driven tức thì.

##### **2. Hot-Key Router (Băm Salt ra RAM / Multi-Worker Salting)**
- **Nguyên lý & Thiết kế**: Khi phát hiện một `customer_id` có lưu lượng sự kiện vượt ngưỡng (Hot Key), luồng dữ liệu được bổ sung thêm một giá trị băm ngẫu nhiên `salt` từ $0 \rightarrow K-1$ (tạo thành composite key `customer_id#salt_k`). Flink sẽ `keyBy(customer_id#salt_k)` để phân tán dữ liệu và Ring Buffer State của khách hàng đó ra $K$ TaskManager Slots khác nhau trên Heap RAM.
- **Kịch bản kỳ vọng**: Phân chia khối lượng ghi dữ liệu và tính toán của 1 khách hàng bị Skew ra $K$ CPU Cores chạy song song, nâng throughput lên $K$ lần.
- **Phân tích lý do không khả thi**:
  1. **Phân mảnh trạng thái cửa sổ (Window State Fragmentation)**: Ring Buffer 96 buckets của một khách hàng bị xé lẻ thành $K$ bản sao Ring Buffer không hoàn chỉnh nằm rải rác trên $K$ node mạng khác nhau (mỗi node chỉ nắm giữ một phần dữ liệu của các event mang salt tương ứng).
  2. **Tắc nghẽn giao tiếp mạng cross-node khi đánh giá cây điều kiện**: Để đánh giá một luật cửa sổ 24h (`condition_tree`), hệ thống bắt buộc phải thu gom và tổng hợp dữ liệu từ cả $K$ Ring Buffer của $K$ worker node. Việc này phát sinh thao tác `RPC / Cross-Node State Read` qua mạng, gây ra hiện tượng nghẽn băng thông mạng (Network I/O Saturation), tăng giật cục latency (Latency Spikes) và làm sụp đổ throughput toàn cụm.
  3. **Lỗi xung đột thứ tự & vi phạm chuỗi tuần tự (Serializability Breakdown & Race Conditions)**: Do các event của cùng 1 customer bị đẩy sang $K$ worker node xử lý bất đồng bộ, thứ tự xử lý của Event $n$ và Event $n-1$ bị đảo lộn trên mạng. Trạng thái $S_n$ không còn đảm bảo được tính toán dựa trên đúng trạng thái $S_{n-1}$, làm hỏng tính nhất quán tuyệt đối của dữ liệu.

##### **3. Tách Rời State Ghi Và Rule Engine Đọc Bất Đồng Bộ**
- **Nguyên lý & Thiết kế**: Tách luồng xử lý thành 2 toán tử song song (Pipeline Decoupling):
  - **Subtask Ghi (State Writer)**: Đọc event $E_n$ từ Kafka và lập tức ghi dồn dữ liệu/update Ring Buffer vào State (hoặc Cache ngoài như Redis/In-Memory Store).
  - **Subtask Đọc (Rule Evaluator)**: Bắn một tín hiệu event bất đồng bộ sang luồng đánh giá Rule để đọc State từ Cache ngoài và kiểm tra `condition_tree`.
- **Kịch bản kỳ vọng**: Giải phóng thời gian chờ duyệt cây điều kiện khỏi Subtask Ghi, giúp Subtask Ghi đạt tốc độ tiếp nhận event cực cao.
- **Phân tích lý do không khả thi**:
  1. **Đảo ngược trật tự ràng buộc nghiệp vụ (Strict Business Order Inversion)**: Quy trình nghiệp vụ cốt lõi bắt buộc: Event $E_n$ xuất hiện $\rightarrow$ Kiểm tra cây điều kiện Rule dựa trên trạng thái cửa sổ quá khứ $S_{n-1}$ (tại mốc thời gian ngay trước khi $E_n$ được nạp) $\rightarrow$ Đánh giá kết quả PASS/FAIL $\rightarrow$ Mới được phép ghi $E_n$ vào Ring Buffer để tạo thành trạng thái $S_n$.
  2. **Ô nhiễm State khi đánh giá (State Contamination & Self-Inclusion Bug)**: Nếu Subtask Ghi nạp $E_n$ vào State trước, khi Subtask Đọc truy xuất State lên để đánh giá Rule cho $E_n$, giá trị của $E_n$ đã nằm sẵn trong Ring Buffer. Phép tính cửa sổ (ví dụ: `SUM(amount) > 10M`) sẽ tính gộp cả $E_n$ vào cửa sổ quá khứ, làm sai lệch bản chất của luật ("Tổng chi tiêu *trước giao dịch này* là bao nhiêu").
  3. **Lỗi Race Condition giữa Đọc và Ghi**: Nếu có 2 event $E_n$ và $E_{n+1}$ đến liên tiếp trong vài millisecond, Subtask Ghi có thể nạp cả $E_n$ và $E_{n+1}$ vào State trước khi Subtask Đọc kịp đánh giá cho $E_n$, làm hỏng toàn bộ kết quả audit và phân khúc.

##### **4. Zero-State Early Exit (Lọc trước keyBy)**
- **Nguyên lý & Thiết kế**: Đặt một toán tử Stateless Filter đứng trước `keyBy(customer_id)`. Operator này soi chiếu bản tin event với bộ điều kiện kích hoạt `trigger_criteria` của tất cả các Rule đang active. Nếu event không thỏa mãn trigger của bất kỳ Rule nào, nó sẽ bị loại bỏ (drop) ngay tại tầng Ingestion mà không đẩy qua `keyBy`.
- **Kịch bản kỳ vọng**: Loại bỏ tới 80–90% luồng event không quan trọng trước khi đi vào Keyed State Operator, giảm thiểu đáng kể số lượng bản tin phải `keyBy(customer_id)`.
- **Phân tích lý do không khả thi**:
  1. **Phá vỡ nguyên tắc tích lũy dữ liệu lịch sử (History Buffer Degradation)**: Mọi event phát sinh của khách hàng (dù là giao dịch nạp tiền nhỏ, chuyển khoản, hay đăng nhập) bắt buộc phải được lưu vết vào Ring Buffer 96 buckets để làm nguyên liệu tính toán tích lũy cho các Rule trong tương lai.
  2. **Bản tin bị drop làm đứt gãy cửa sổ tổng hợp (Data Hole Bug)**: Ví dụ một event $E_1$ không khớp trigger của Rule A nên bị drop. 10 phút sau, Rule B được hot-reload vào hệ thống hoặc Event $E_2$ xuất hiện đòi hỏi tính `SUM(amount)` trong 1 giờ qua. Vì $E_1$ đã bị drop từ trước, Ring Buffer thiếu mất dữ liệu của $E_1$, dẫn đến kết quả tổng hợp cửa sổ của Rule B bị sai hoàn toàn.

##### **5. Running Window Total (Duy trì biến tổng lũy kế trên RAM)**
- **Nguyên lý & Thiết kế**: Thay vì lưu trữ 96 buckets riêng lẻ và phải lặp qua các bucket để tính tổng/trung bình mỗi khi có event đến, hệ thống duy trì sẵn các biến tổng cộng dồn (Running Sum, Running Count) trực tiếp trên Heap RAM của từng `customer_id`. Khi event $E_n$ đến, chỉ cần lấy `running_sum += value` với chi phí $O(1)$.
- **Kịch bản kỳ vọng**: Triệt tiêu hoàn toàn vòng lặp quét 96 buckets, đưa độ phức tạp tính toán cửa sổ từ $O(B)$ về $O(1)$.
- **Phân tích lý do không khả thi**:
  1. **Bùng nổ trạng thái trước cấu hình cửa sổ động (Dynamic Window Combinatorial Explosion)**: Các Rule do người dùng nghiệp vụ định nghĩa thông qua Broadcast State chứa vô số biến thể cửa sổ thời gian khác nhau: Tumbling 15 phút, 1 giờ, 2 giờ; Sliding 30 phút (slide 5p), Sliding 24 giờ (slide 15p)... Ngoài ra, mỗi cửa sổ còn đi kèm bộ lọc điều kiện `filter` động (ví dụ: `filter: transaction_type == 'PAYMENT'`).
  2. **Không khả thi để khai báo biến tích lũy trước**: Hệ thống không thể biết trước người dùng sẽ tạo những cửa sổ nào với bộ lọc `filter` nào để duy trì sẵn biến Running Total. Nếu duy trì sẵn biến tổng cho mọi trường và mọi tổ hợp điều kiện khả dĩ, số lượng biến trên RAM của 1 khách hàng sẽ bùng nổ lên hàng chục nghìn biến, gây bùng nổ dung lượng bộ nhớ (Out Of Memory).
  3. **Khó khăn khi trừ dữ liệu hết hạn (Eviction Calculation Cost)**: Đối với cửa sổ trượt, khi mốc thời gian trượt qua, hệ thống vẫn phải biết chính xác giá trị của các bucket cũ hết hạn để trừ đi khỏi `running_sum`, điều này vẫn đòi hỏi phải truy vấn dữ liệu chi tiết của từng bucket.

##### **6. Metric-Group Sharding (`keyBy(customer_id, metric_group)`)**
- **Nguyên lý & Thiết kế**: Phân chia 300 trường dữ liệu của hệ thống thành các nhóm chỉ số độc lập theo miền nghiệp vụ (ví dụ: `GROUP_FINANCE` gồm các chỉ số giao dịch/tiền tệ, `GROUP_ENGAGEMENT` gồm các chỉ số đăng nhập/click/view). Flink thực hiện `keyBy(customer_id, metric_group)` để phân tán luồng xử lý của cùng 1 customer sang các Subtask khác nhau theo nhóm chỉ số.
- **Kịch bản kỳ vọng**: Chia tải tính toán và dung lượng State của 1 khách hàng bị Skew ra $M$ nhóm chỉ số, giúp $M$ Subtask xử lý song song.
- **Phân tích lý do không khả thi**:
  1. **Bất lực trước cây điều kiện kết hợp chéo (Cross-Metric Rule Incompatibility)**: Trong thực tế phân khúc CDP, phần lớn các Rule có giá trị nghiệp vụ cao đều là **Rule chéo (Cross-Metric Rules)**, kết hợp điều kiện từ nhiều nhóm chỉ số khác nhau trong cùng một cây logic `condition_tree`.
     *Ví dụ Rule chéo*: `(SUM(FINANCE.amount_24h) > 10M) AND (COUNT(ENGAGEMENT.app_login_7d) >= 5) AND (PROFILE.age >= 18)`.
  2. **Phát sinh tầng Stream-Stream Join 2 pha phức tạp & gây trễ**: Khi trạng thái bị chia rẽ theo `metric_group`, để đánh giá được Rule chéo trên, hệ thống bắt buộc phải đẩy kết quả đánh giá từng nhánh về một toán tử Join/Barrier Synchronization 2 pha ở tầng sau. Việc này phá vỡ tính chất thời gian thực, gây ra lỗi Race Condition (nhánh này đến trước nhánh kia đến sau) và phức tạp hóa kiến trúc gấp nhiều lần.

##### **7. Rate Limiting / Evaluation Cooldown (Giãn cách chu kỳ đánh giá)**
- **Nguyên lý & Thiết kế**: Vẫn tiếp nhận và cộng dồn dữ liệu event vào Ring Buffer 96 buckets cho mọi bản tin đến, nhưng áp dụng cơ chế hãm tần suất đánh giá (Evaluation Cooldown). Đối với một `customer_id` bị Hot-Key, hệ thống chỉ cho phép thực thi cây logic `condition_tree` tối đa 1 lần trong mỗi khoảng thời gian Cooldown (ví dụ: 50ms hoặc 100ms). Các event đến trong thời gian Cooldown chỉ được ghi vào State mà không chạy Rule.
- **Kịch bản kỳ vọng**: Cắt giảm 90% số lần phải duyệt cây AST và tra cứu Inverted Index đắt đỏ đối với các Hot Key có lưu lượng hàng nghìn event/giây.
- **Phân tích lý do không khả thi**:
  1. **Vi phạm ràng buộc Event-Driven thuần túy (Violation of Strict Event-Driven SLA)**: Hệ thống Stateful Streaming Core được định vị là engine xử lý sự kiện tức thời. Mỗi một sự kiện phát sinh bắt buộc phải được đánh giá ngay lập tức để quyết định xem sự kiện đó có kích hoạt segment/tín hiệu hay không.
  2. **Bỏ lọt mốc thời gian chạm ngưỡng kích hoạt (Threshold Breach Miss Bug)**: Giả sử trong khoảng Cooldown 100ms có 50 event đến. Event thứ 5 chạm đúng ngưỡng `window_sum >= 10M` (kích hoạt Rule). Nếu bỏ qua đánh giá và chờ đến hết 100ms mới chạy, mốc kích hoạt của Event thứ 5 bị trễ 80ms, hoặc nếu đến Event thứ 10 tài khoản bị trừ tiền làm `window_sum < 10M`, lần đánh giá ở cuối chu kỳ Cooldown sẽ thấy `window_sum < 10M` và **bỏ lọt hoàn toàn tín hiệu kích hoạt của Event thứ 5**.

##### **8. Stateful Sequencer + Parallel Evaluator Pool (Offloading bất đồng bộ sang Worker Pool)**
- **Nguyên lý & Thiết kế**: Tách hệ thống làm 2 tầng mạng độc lập theo mô hình Actor / Pipeline Separation:
  - **Tầng 1 (Keyed State Sequencer)**: Chạy `keyBy(customer_id)`. Tầng này hoàn toàn không chứa Rule Engine, chỉ làm nhiệm vụ nhận Event $E_n$, trích xuất nhanh bản chụp trạng thái $S_{n-1}$ từ RAM, cập nhật $E_n$ vào mảng để tạo $S_n$, rồi đóng gói `Context_n = {Event: E_n, StateSnapshot: S_{n-1}}`.
  - **Tầng 2 (Stateless Rule Evaluator Pool)**: Một cụm gồn $N$ Subtask tự do (không `keyBy`). Tầng 1 dùng lệnh `rebalance()` rải đều các gói `Context_n` sang Tầng 2 để các worker rảnh rỗi đánh giá Inverted Index và `condition_tree` song song.
- **Kịch bản kỳ vọng**: Chuyển toàn bộ 99% tải tính toán CPU nặng (tra cứu index, duyệt cây logic) từ 1 Subtask bị nghẽn sang toàn bộ tài nguyên CPU của cả cụm Flink.
- **Phân tích lý do không khả thi**:
  1. **Gây nghẽn băng thông mạng nội bộ nghiêm trọng (Network Bandwidth Saturation)**: Một bản chụp trạng thái $S_{n-1}$ của 1 khách hàng bao gồm dữ liệu của 96 buckets nhân với 300 chỉ số metric (cùng các thuộc tính Profile tĩnh/động). Dung lượng của một gói `Context_n` khi đóng gói có thể lên tới vài trăm KB đến 1 MB. Nếu Hot Key phát sinh $10.000$ event/giây, lưu lượng mạng phải truyền tải giữa Tầng 1 và Tầng 2 lên tới $10.000 \times 1\text{ MB} = 10\text{ GB/giây}$ (vượt quá năng lực của card mạng 10Gbps/40Gbps nội bộ).
  2. **Chi phí Serialization / Deserialization và áp lực GC khổng lồ**: Việc liên tục chuyển đổi (serialize) hàng nghìn đối tượng State phức tạp thành mảng Byte để gửi qua mạng và giải mã (deserialize) ở đầu nhận tạo ra gánh nặng CPU Serialization cực lớn và làm bùng nổ rác bộ nhớ (Garbage Collection Spikes), dẫn đến các đợt ngắt Stop-The-World kéo dài của JVM, làm sụp đổ hoàn toàn hệ thống.

##### **9. In-Place Monolithic Flat Memory Array (`double[300 * 96 * 4]`)**
- **Nguyên lý & Thiết kế**: Tổ chức toàn bộ dữ liệu lưu trữ 96 buckets của 300 trường dữ liệu khách hàng thành một mảng số thực 1 chiều phẳng liên tục (`double[]`) lưu trực tiếp trên Heap RAM của JVM (~920 KB per customer). Đọc/ghi dữ liệu bằng công thức tính tọa độ chỉ số mảng trực tiếp (`index = (fieldId * 96 + bucketId) * 4`), thực thi in-place ngay tại Subtask duy nhất.
- **Kịch bản kỳ vọng**: Triệt tiêu hoàn toàn chi phí I/O RocksDB, chi phí JNI, chi phí Garbage Collection (tái sử dụng mảng cố định) và tận dụng tối đa tốc độ L1/L2 Cache của CPU phần cứng ($\approx 2-5\text{ ns}$ per access).
- **Phân tích lý do không khả thi**:
  1. **Khái niệm "Trường" trong bài toán là không cố định (Dynamic Combination Fields Paradox)**: Trong hệ thống CDP Streaming Core, con số 300 không phải là 300 thuộc tính thô cố định (như `age`, `gender`, `amount`). Một "trường" tham gia vào cửa sổ Ring Buffer được định nghĩa bởi một **biểu thức chỉ số kết hợp với bộ lọc điều kiện `filter` động**.
     *Ví dụ*: `Trường_1 = SUM(amount, filter: login_channel == 'MOBILE')`, `Trường_2 = SUM(amount, filter: login_channel == 'WEB')`, `Trường_3 = COUNT(event_id, filter: location == 'HANOI')`.
  2. **Bất khả thi để quy hoạch kích thước mảng cố định trước**: Vì các bộ lọc điều kiện `filter` bên trong Rule được người dùng tạo mới, sửa đổi hoặc xóa bỏ liên tục theo thời gian thực thông qua Broadcast Stream, **danh sách các "trường tổng hợp" phát sinh hoàn toàn linh hoạt và mở rộng vô hạn**. Do đó, hệ thống không thể dự đoán trước để cấp phát hay quy hoạch cố định một mảng phẳng `double[300 * 96 * 4]` có kích thước cố định từ trước.

##### **10. Partition by Field / Metric Dimension (`keyBy(customer_id, field_id)`)**
- **Nguyên lý & Thiết kế**: Thay vì `keyBy(customer_id)`, hệ thống thực hiện phân vùng sâu hơn theo cặp `keyBy(customer_id, field_id)`. Mỗi Subtask chỉ chịu trách nhiệm lưu trữ và quản lý 96 buckets cho đúng 1 trường dữ liệu duy nhất của khách hàng (dung lượng State cực nhỏ, chỉ ~3 KB/field). Khi có Data Event đến, bản tin event thô (~100 bytes) được nhân bản và đẩy tới các Subtask tương ứng với các trường xuất hiện trong event đó.
- **Kịch bản kỳ vọng**: Phân tán hoàn toàn tải tính toán và lưu trữ của 1 khách hàng bị Skew sang 300 Subtask khác nhau tùy theo `field_id`, loại bỏ tình trạng 1 CPU Core phải gánh toàn bộ 300 trường.
- **Phân tích lý do không khả thi**:
  1. **Dẫn đến tình trạng bất đồng bộ giữa các luồng chỉ số (Asynchronous Stream Desynchronization)**: Các Subtask quản lý `(Customer A, Field_1)` và `(Customer A, Field_2)` chạy độc lập trên các CPU Cores khác nhau với tốc độ xử lý và độ trễ queue khác nhau. Việc này khiến trạng thái lịch sử của cùng 1 khách hàng bị lệch pha thời gian giữa các chỉ số.
  2. **Nghẽn & phức tạp hóa khi đánh giá Rule chéo nhiều trường (Cross-Field Dependency Barrier)**: Khi đánh giá các Rule phức tạp đòi hỏi kết hợp giữa `Field_1` và `Field_2` (ví dụ: `Field_1 > 10M AND Field_2 < 3`), Subtask phụ trách `Field_1` không có thông tin của `Field_2`. Để đánh giá được, bắt buộc phải phát sinh cơ chế truyền tải thông điệp giữa các Subtask hoặc dùng tầng Barrier Synchronization / Stream Join để chờ đợi lẫn nhau. Việc này gây trễ mạng nghiêm trọng, dễ phát sinh dead-lock, race condition và làm hỏng thứ tự tuần tự tuyệt đối của sự kiện.

---

#### **2. Bản Chất Của Nút Thắt Kỹ Thuật (Core Paradox)**

Rào cản lớn nhất của bài toán nằm ở sự xung đột giữa **3 ràng buộc tuyệt đối**:

1. **Tính tuần tự nghiêm ngặt (Strict Serializability)**: Trạng thái $S_n$ của Event $n$ phụ thuộc chính xác vào kết quả đánh giá đối với trạng thái $S_{n-1}$ của Event $n-1$.
2. **Tính toàn vẹn dữ liệu (No Loss / No Aggregation)**: Mọi event đều phải ghi nhận đầy đủ vào Ring Buffer; không được gộp, không được bỏ qua.
3. **Mô hình luồng đơn của Flink (Single-threaded Key Partition)**: `keyBy(customer_id)` khóa chặt toàn bộ việc đọc, duyệt cây AST logic và ghi của một khách hàng vào đúng **1 CPU Core duy nhất**.

---

#### **3. Hướng Đi Tối Ưu Đơn Core Khả Thi Duy Nhất (Single-Core Execution Optimization)**

Khi không được phép can thiệp vào luồng dữ liệu (Dataflow) và không được phép gộp/bỏ qua bất kỳ event nào, lối thoát duy nhất là **thuần túy tối ưu tốc độ thực thi trên 1 CPU Core** để tăng trần chịu tải (Throughput Ceiling):

- **L1 On-Heap State (Primitive Array)**: Lưu Ring Buffer 96 bucket của 1 customer dưới dạng mảng 1 chiều số thực nguyên thủy (`double[]` / `long[]`) trực tiếp trên Heap RAM của JVM thay vì gọi `MapState` RocksDB cho mỗi lần đọc $\rightarrow$ Triệt tiêu hoàn toàn chi phí I/O và JNI serialization.
- **Biên Dịch Bytecode Cho Rule (`condition_tree`)**: Sử dụng Janino hoặc Byte Buddy để dịch toàn bộ cây logic của rule thành mã máy/bytecode Java trực tiếp khi hot-reload (thay vì duyệt đệ quy cây Object AST) $\rightarrow$ 1 CPU core có thể thực thi hàng triệu phép so sánh/giây.
- **Tận Dụng CPU Xung Nhịp Cao (High Single-Core Frequency $\ge 4.0\text{ GHz}$)**: Đảm bảo 1 Subtask xử lý xong toàn bộ chuỗi `[Inverted Index -> Duyệt Logic -> Update Mảng]` trong vòng $\approx 10 - 20\text{ microseconds}$, giúp 1 CPU core gánh được tối đa **$50.000 - 80.000\text{ events/giây}$** cho 1 hot-key.

---

### 2.5 Cơ Chế Tối Ưu Nâng Cao: Intra-TaskManager Work-Stealing Pool Với Dynamic Quota

Để vượt qua rào cản luồng đơn của Flink (`keyBy` khóa 1 customer vào 1 CPU Core) mà **không vi phạm bất kỳ ràng buộc nào** (Zero Network Transfer, Zero Serialization, Giữ nguyên chuỗi tuần tự $S_{n-1} \rightarrow S_n$), hệ thống đề xuất mô hình **Intra-TaskManager Work-Stealing Pool với Dynamic Quota**.

#### **1. Nguyên Lý Hoạt Động & Kiến Trúc Tổng Thể**

Thay vì bắn State hay Event qua mạng giữa các TaskManager khác nhau, mô hình này tận dụng toàn bộ các CPU Cores vật lý **trong cùng một TaskManager JVM Process** thông qua một ThreadPool nội bộ có kiểm soát Hạn ngạch (Quota) và Tái sử dụng CPU rảnh rỗi (Work-Stealing).

```
┌────────────────────────────────────────────────────────────────────────────────┐
│ TaskManager JVM (Ví dụ: 8 Cores Vật lý - Cấu hình 8 Task Slots)               │
│                                                                                │
│ [State]: Mảng / Map State nằm trọn vẹn trên Heap RAM chung                     │
│                                                                                │
│ [Phân bổ Tài nguyên Động]:                                                    │
│ ├── 1 Subtask Hot Key: Cắt tập Rule thành 5 Chunks ──► Chiếm MAX 5 Cores       │
│ │   (Duyệt song song trên Cores 3, 4, 5, 6, 7)                                │
│ └── 7 Subtasks Thường: Tự động co cụm chia sẻ ──► Bảo lưu MIN 3 Cores         │
│     (Cores 0, 1, 2 không bao giờ bị nghẽn hay đói CPU)                         │
│                                                                                │
│ [Work-Stealing]: Khi 7 subtask thường rảnh, 3 Cores rảnh tự động nhảy sang     │
│ gánh phụ Hot Key -> Tận dụng 100% 8 Cores, không lãng phí tài nguyên.          │
└────────────────────────────────────────────────────────────────────────────────┘
```

#### **2. Cơ Chế Quota Lease Theo Khung Thời Gian & Tự Động Gia Hạn (Time-Window Quota Lease & Auto-Renew)**

Để triệt tiêu hoàn toàn chi phí nghẽn khóa (Lock Contention) và atomic overhead khi phải kiểm tra Skew / xin-trả Semaphore cho **từng bản tin event riêng lẻ** (vốn phát sinh hàng chục nghìn lần/giây khi có Hot Key), hệ thống áp dụng cơ chế **Cấp Hạn Ngạch Theo Khung Thời Gian (Time-Window Quota Lease)**:

```
[ Event Stream ] ──► [ EMA Rate Monitor ]
                           │
             (Rate > 500 msgs/s detected)
                           │
                           ▼
          ┌──────────────────────────────────┐
          │  Acquire 5-sec Quota Lease       │ ──► [ Enable Multi-Thread Pool ]
          └──────────────────────────────────┘       (Dùng liên tục trong 5s,
                           │                          Zero Per-Element Overhead)
                           ▼
           ┌───────────────────────────────┐
           │ Periodic Check (Every 1 sec)  │
           └──────────────┬────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
  (Rate still > 500)            (Rate < 100)
    Gia hạn Lease                 Release Lease
  (Auto-Renew 5 sec)          (Trở về Single-Thread)
```

1. **Giám sát Tốc độ Không Khóa (Lock-free EMA Rate Monitoring)**:
   - Subtask duy trì một bộ đếm trung bình trượt Exponential Moving Average (EMA) về event rate cho từng `customer_id` với chi phí cực thấp ($\approx 1\text{ ns}$).
2. **Kích hoạt Lease Quota Khung Thời Gian (Lease Acquisition)**:
   - Khi event rate của một `customer_id` vượt ngưỡng Hot Key (ví dụ $> 500\text{ msgs/s}$), Subtask **xin cấp phép Quota Lease 1 lần duy nhất** từ `TaskManager Quota Manager` cho một khoảng thời gian $T_{\text{lease}}$ (ví dụ: $5\text{ giây}$).
   - Trong suốt khoảng thời gian $5\text{ giây}$ này, **TẤT CẢ các bản tin của Hot Key đó mặc định đi thẳng vào luồng xử lý song song Multi-Threaded Rule Chunking** mà **KHÔNG CẦN thực hiện bất kỳ thao tác xin/trả Semaphore hay Lock Contention nào ở cấp độ per-element**.
3. **Vừa Kiểm Tra Vừa Gia Hạn Động (Periodic Check & Auto-Renew)**:
   - Định kỳ (ví dụ mỗi $1\text{ giây}$), luồng giám sát chạy ngầm kiểm tra lại tốc độ thông lượng của Hot Key:
     - **Nếu Rate vẫn duy trì cao ($> 500\text{ msgs/s}$)**: Tự động **Gia hạn Lease (Auto-Renew)** thêm $5\text{ giây}$ mà không làm gián đoạn luồng đang xử lý.
     - **Nếu Rate hạ nhiệt ($< 100\text{ msgs/s}$)**: Hết $5\text{ giây}$ Lease, Subtask chủ động **Nhả Lease (Release Quota)**, trả 5 CPU Cores về cho Pool chung và đưa `customer_id` quay lại chế độ xử lý đơn luồng thông thường.

---

#### **3. Quy Trình Thực Thi Chi Tiết Khi Có Hot Key**

1. **Giai đoạn 1: Tiếp Nhận & Cập Nhật State Tuần Tự (Sequential State Gatekeeper)**:
   - Luồng chính của Flink (`ProcessFunction.processElement()`) tiếp nhận Event $E_n$.
   - Trích xuất thông tin trạng thái cũ $S_{n-1}$ từ Heap RAM nội bộ (không tốn I/O hay Serialization).
   - Tiến hành cập nhật $E_n$ vào Ring Buffer để sinh ra $S_n$ cho event tiếp theo.
   - Thao tác này hoàn thành cực nhanh ($\approx 0.1\mu s$), bảo đảm tính **tuần tự nghiêm ngặt 100%**.

2. **Giai đoạn 2: Định Tuyến Theo Trạng Thái Lease (Lease-Based Routing)**:
   - Luồng chính kiểm tra cờ `HOT_KEY_LEASE_ACTIVE` của customer (chỉ kiểm tra 1 biến `boolean` trên RAM):
     - **Nếu Lease đang ACTIVE**: Cắt 200 rules thành 5 Chunks và đẩy thẳng vào `Intra-TaskManager Worker Pool` để 5 Cores thực thi song song mà không cần xin Semaphore.
     - **Nếu Lease INACTIVE**: Duyệt tuần tự 200 rules trên 1 Core hiện tại như bình thường.

3. **Giai đoạn 3: Phân Bổ Hạn Ngạch & Bảo Lưu (Dynamic Quota & Anti-Starvation)**:
   - **Hạn ngạch Hot Key (`MAX_HOTKEY_QUOTA = 5`)**: Subtask chỉ được giữ tối đa Lease cho 5 luồng song song trên 5 Cores vật lý (ví dụ: Cores 3, 4, 5, 6, 7). Thời gian duyệt 200 rules giảm từ $1\text{ ms}$ xuống còn $0.2\text{ ms}$.
   - **Bảo lưu tối thiểu (Anti-Starvation Reservation)**: 7 Subtask chứa các user thường còn lại được bảo lưu tối thiểu 3 CPU Cores (Cores 0, 1, 2) để tiếp tục xử lý các event thường mà không lo bị nghẽn dây chuyền (Zero Thread Starvation).

4. **Giai đoạn 4: Trộm Việc Tự Động (Work-Stealing Dynamic Scaling)**:
   - Vào các khung giờ lưu lượng user thường thấp (ví dụ ban đêm), hàng đợi của 7 Subtask thường rỗng.
   - 3 CPU Cores bảo lưu (Cores 0, 1, 2) tự động kích hoạt cơ chế Work-Stealing, tự động "kéo" các Rule Chunks còn tồn đọng của Hot Key về xử lý.
   - Toàn bộ 8 Cores vật lý được dồn lực 100% để tăng tốc chịu tải cho Hot Key mà không lãng phí CPU.

---

#### **4. Bảng Đánh Giá Chi Tiết Ưu Điểm & Thách Thức Kỹ Thuật**

| Tiêu chí | Ưu điểm cốt lõi (Pros) | Thách thức kỹ thuật & Hạn chế (Cons) |
| :--- | :--- | :--- |
| **Băng thông Mạng & Overheads** | **0 byte truyền qua mạng**: Toàn bộ State nằm chung Heap RAM. **Zero Per-Element Lock Overhead**: Cơ chế Quota Lease loại bỏ hoàn toàn chi phí xin/trả Semaphore cho từng bản tin riêng lẻ. | **Chỉ mở rộng theo chiều dọc (Scale-Up Only)**: Sức mạnh chịu tải của 1 Hot Key bị giới hạn bởi số lượng vCPU tối đa của 1 máy chủ TaskManager (ví dụ: max 16–32 cores / node). Không thể scale-out ra nhiều máy qua mạng. |
| **Khả năng gánh Data Skew** | **Bẻ gãy giới hạn 1 Core/Subtask của Flink**: Huy động 5–8 Cores vật lý cùng tính cho 1 Hot Key. **Đảm bảo thứ tự 100%**: Luồng Flink chính vẫn quản lý State tuần tự theo thời gian. | **Độ phức tạp lập trình cao**: Cần tự quản lý `ThreadPoolExecutor`, bộ đếm EMA, Timer gia hạn Lease và `CountDownLatch` (chờ hoàn thành các chunks) trong `processElement()`. |
| **Hiệu quả Tài nguyên CPU** | **Không lãng phí CPU**: Bình thường xử lý 8 slots đều đặn; khi có biến động Hot Key tự động mượn/nhả Cores linh hoạt theo khung thời gian Lease. **Chống chết đói (Fairness)**: Luôn bảo lưu Cores cho Subtask thường. | **Lệch thứ tự mili-giây tại Downstream (Downstream Out-of-Order)**: Do 5 luồng song song hoàn thành 5 Chunks chênh lệch vài $\mu s$, kết quả bắn ra Sink có thể lệch thứ tự nhẹ $\rightarrow$ Downstream Sink cần sắp xếp lại theo `event_time` / `sequence_id`. |

---

#### **5. Cơ Chế Phân Tải Key Cấp Cụm Nhận Biết Hot Key (Cluster-Wide Skew-Aware Key-Group Balancing)**

Mô hình Work-Stealing nội bộ giúp 1 TaskManager huy động 8 Cores để gánh Hot Key. Tuy nhiên, khi toàn cụm K8s tự động scale-out thêm nhiều TaskManager Pods mới, **hệ thống vẫn đối mặt với rủi ro của thuật toán Băm tiêu chuẩn (Standard Hash Paradox)**:

##### **Rủi Ro Cụm: Hot Key Co-location Vulnerability**
Flink mặc định phân chia Key theo công thức `KeyGroup = Math.abs(customer_id.hashCode()) % maxParallelism`. Thuật toán băm này hoàn toàn **không nhận biết được trọng số (Weight/Hotness)** của Key. Do đó, có xác suất 2 hoặc 3 Hot Keys (ví dụ: `cust_100` và `cust_200` có thông lượng $10.000\text{ msgs/s}$) cùng rơi vào chung 1 KeyGroup hoặc cùng được gán cho 1 TaskManager Subtask. Khi đó, dù K8s có scale-out thêm Pods mới, TaskManager dính 3 Hot Keys vẫn bị sụp đổ CPU trong khi các TaskManager Pods khác lại rơi vào trạng thái rảnh rỗi.

##### **Giải Pháp 2 Tầng: Cluster-Wide Skew-Aware Key-Group Assignment**

Để triệt tiêu hoàn toàn rủi ro này, hệ thống kết hợp cơ chế **Phân Vùng KeyGroup Nhận Biết Trọng Số Hot Key (Skew-Aware KeyGroup Partitioner)** ở cấp độ Cụm (Cluster-Level):

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Cluster-Level (K8s & JobManager)                       │
│                                                                                 │
│  [TaskManager Pod 1] ──(Top Hot Keys & Rate EMA)──► [ Flink JobManager ]       │
│  [TaskManager Pod 2] ──────────────────────────────► [ Custom Partitioner ]     │
│                                                              │                  │
│                                                    (Weighted Bin-Packing)       │
│                                                              │                  │
│  Anti-Colocation Rule:                                       ▼                  │
│  • TaskManager Pod 1 ◄── Assigned KeyGroup A (HotKey 1)                         │
│  • TaskManager Pod 2 ◄── Assigned KeyGroup B (HotKey 2)                         │
│  • TaskManager Pod 3 ◄── Assigned KeyGroup C (HotKey 3)                         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Node-Level (Intra-TaskManager)                             │
│                                                                                 │
│  TaskManager Pod 1: Nhận đúng 1 Hot Key ──► Huy động 8 Cores Work-Stealing Pool │
│  TaskManager Pod 2: Nhận đúng 1 Hot Key ──► Huy động 8 Cores Work-Stealing Pool │
└─────────────────────────────────────────────────────────────────────────────────┘
```

1. **Giám Sát & Báo Cáo Trọng Số Hot Key Cấp Cụm (Hot-Key Telemetry)**:
   - Các TaskManager định kỳ (ví dụ mỗi 10 giây) tổng hợp danh sách Top 10 Hot Keys có thông lượng lớn nhất và báo cáo về cho Flink JobManager qua Flink Metrics Reporter.
2. **Thuật Toán Cân Bằng Trọng Số KeyGroup (Weighted Bin-Packing Key-Group Assignment)**:
   - Khi K8s HPA tự động bổ sung TaskManager Pods mới (Scale-Out) và Flink kích hoạt cơ chế chia lại KeyGroup (Rebalancing):
   - Flink không dùng chia đều ngẫu nhiên, mà áp dụng thuật toán **Weighted Bin-Packing** dựa trên ma trận trọng số Hot Key đã thu thập.
   - **Quy tắc Chống Gom Cụm Hot Key (Strict Anti-Colocation Rule)**: Đảm bảo các KeyGroup chứa Top Hot Keys bắt buộc phải được rải đều sang các TaskManager Pods khác nhau (Mỗi TaskManager Pod chỉ phải chịu trách nhiệm tối đa 1 Hot Key).
3. **Mô Hình Phối Hợp 2 Tầng Cân Bằng Tải Toàn Diện**:
   - **Tầng 1 (Cluster-Level - Scale Out)**: K8s HPA + Skew-Aware Key-Group Partitioner đảm bảo phân tán đều các Hot Keys ra $N$ TaskManager Pods riêng biệt (Không bao giờ dồn 2 Hot Keys vào 1 Subtask).
   - **Tầng 2 (Node-Level - Scale Up)**: Intra-TaskManager Work-Stealing Pool với Time-Window Quota Lease huy động toàn bộ 8 Cores nội bộ của TaskManager Pod đó để xử lý dứt điểm 1 Hot Key duy nhất được phân công.

---

### 2.6 Hai Mô Hình Kiến Trúc Cốt Lõi Cấp Độ Trừu Tượng (Core Abstract Architectural Paradigms)

Xét ở cấp độ thiết kế hệ thống tổng thể (Abstract Pattern / Paradigm Architecture), toàn bộ các hướng tiếp cận xử lý bài toán Stateful Streaming Core được quy hoạch thành **2 Mô hình Kiến trúc Trừu tượng cốt lõi**:

#### **Mô Hình 1: Monolithic Keyed In-Place Architecture (Mô hình Tập trung 1 Tầng tại Keyed Subtask)**

##### **1. Triết lý thiết kế & Luồng dữ liệu**
Toàn bộ quá trình tiếp nhận Event $E_n$, tra cứu Inverted Index, truy xuất State $S_{n-1}$, cập nhật State $S_n$ và đánh giá cây logic Rule (`condition_tree`) được gom trọn vẹn vào **đúng 1 Toán tử Keyed Subtask (`keyBy(customer_id)`) duy nhất**. Dữ liệu Ring Buffer 96 buckets và Profile State nằm trọn vẹn trên RAM Heap local của TaskManager process.

##### **2. Ma trận Ưu điểm & Khả năng Thích ứng Tài nguyên Linh hoạt**
- **Tận dụng 100% CPU ở Tải thường (Normal Traffic Efficiency)**: Trong điều kiện lưu lượng bình thường (không có Hot Key), $N$ Task Slots của TaskManager chia nhau $N$ CPU Cores vật lý để xử lý song song các event từ hàng triệu khách hàng thường. Mỗi event hoàn thành siêu nhanh ($\approx 5-10\mu s$), CPU tổng duy trì ở mức tối ưu mà không lãng phí bất kỳ tài nguyên nào.
- **Tự điều tiết Core Động khi có Hot Key (Elastic Intra-Node Core Borrowing)**: Khi xuất hiện Hot Key, Subtask kích hoạt cơ chế `Time-Window Quota Lease` và `Work-Stealing Pool`, tự động mượn thêm các CPU Cores vật lý rảnh rỗi nội bộ trong cùng TaskManager JVM để duyệt các tập Rule Chunks song song.
- **Bảo đảm chuỗi tuần tự tuyệt đối 100% (Strict Serializability Guarantee)**: Luồng Flink chính đóng vai trò Gatekeeper cập nhật State $S_n$ trước khi đẩy Rule Chunks sang ThreadPool, đảm bảo trật tự $S_{n-1} \rightarrow S_n$ tuyệt đối không bị đảo lộn.
- **Zero Overhead**: $0\text{ byte}$ truyền qua mạng giữa các tầng, $0\text{ ns}$ chi phí Serialization / Deserialization.

##### **3. Cơ chế Phân Tải khi Scale-Out / HA (High Availability Skew-Aware Rebalancing)**
- **Rủi ro Băm mặc định**: Khi K8s HPA tự động bổ sung thêm TaskManager Pods mới (Scale-Out HA), thuật toán băm mặc định (`hashCode() % maxParallelism`) có xác suất dồn 2–3 Hot Keys vào chung 1 Subtask / TaskManager Pod.
- **Giải pháp Chia đều Hot Key**: Hệ thống kết hợp bộ phân vùng `Skew-Aware KeyGroup Partitioner`. Bộ phân vùng này thu thập ma trận thông lượng Hot Key toàn cụm và sử dụng thuật toán **Weighted Bin-Packing** kết hợp **Strict Anti-Colocation Rule** để đảm bảo khi rebalance, **mỗi TaskManager Pod / Subtask chỉ nhận đúng tối đa 1 Hot Key**.

##### **4. Trần khả năng chịu tải & Hạn chế**
Sức mạnh chịu tải tối đa của 1 Hot Key bị chặn bởi tổng số vCPU vật lý của 1 máy chủ TaskManager (ví dụ: max 16–32 cores / node).

---

#### **Mô Hình 2: Decoupled Layered Offloading Architecture (Mô hình Tách Rời 2 Tầng Ingestion/State & Parallel Evaluator)**

##### **1. Triết lý thiết kế & Kiến trúc Tách tầng**
Tách hệ thống làm 2 tầng mạng độc lập theo mô hình Pipeline Separation:
- **Tầng 1 (Ingestion & State Gatekeeper Layer - Keyed Stream)**: Chạy `keyBy(customer_id)`, nhận Event $E_n$, cập nhật State $S_{n-1} \rightarrow S_n$, trích xuất tập Candidate Rules từ Inverted Index, và đóng gói bản chụp Context. Tầng 1 giữ cho công việc của Keyed Subtask cực nhẹ (chỉ tốn $\approx 0.1\mu s$ per event).
- **Tầng 2 (Stateless Parallel Rule Evaluator Layer - Non-Keyed Stream)**: Tầng không có Key, nhận các gói Context từ Tầng 1 qua đường truyền mạng Flink `rebalance()`, tự do phân bổ công việc cho hàng trăm Stateless Subtasks trên toàn cụm để đánh giá cây điều kiện Rule song song.

##### **2. Nút thắt Chi phí gốc & Giải pháp Tối ưu Triệt để bằng Mảng Nguyên Thủy (Flat Primitive Array)**
- **Nút thắt kỹ thuật gốc (The POJO Overhead Paradox)**: Nếu Tầng 1 đóng gói Context dưới dạng đối tượng Java POJO Class (chứa POJO fields, objects nested, array lists...), khi bắn qua mạng cho Tầng 2 qua `rebalance()`, Flink phải dùng Kryo / Pojo Serializer. Chi phí CPU Serialization và dung lượng gói tin lên tới vài KB/event sẽ làm nghẽn hoàn toàn băng thông mạng ($10\text{ GB/s}$) và gây sụp đổ JVM do Garbage Collection (GC Spikes).
- **Giải pháp Tối ưu Triệt để (Primitive Flat Array Buffer Architecture)**: Hệ thống **tuyệt đối không sử dụng Java POJO Class** để truyền qua mạng. Thay vào đó, toàn bộ dữ liệu Context, thông số chỉ số và danh sách Candidate Rules được đóng gói nén dưới dạng **Mảng 1D số thực/nguyên thủy (Primitive Flat Double/Byte Array)**:
  $$\text{Payload}_{\text{network}} = \text{byte}[] \quad (\text{Chỉ lưu mảng số thực thô, } 0 \text{ Object Header, } 0 \text{ Reflection})$$
  - *Chi phí Serialization*: Giảm từ vài microsecond xuống tiệm cận **$0\text{ ns}$** (chỉ gọi `System.arraycopy`).
  - *Dung lượng gói tin*: Giảm 95% (từ vài KB xuống còn vài chục Bytes), đưa lưu lượng mạng về mức cực thấp, giải phóng hoàn toàn nghẽn I/O mạng.

##### **3. Khả năng Scale-Out Vô Hạn & Ma trận Ưu điểm Kỹ thuật**
- **Phân tải hoàn hảo nhờ `rebalance()` tự động (Perfect Automatic Load Balancing)**: Điểm mạnh vượt trội của Mô hình 2 là khả năng cân bằng tải tuyệt đối. Tầng 2 là Non-Keyed Stream sử dụng toán tử `rebalance()`. Cứ mỗi khi có bản tin event đến, Tầng 1 tự động chia nhỏ và gửi sang Tầng 2. Công việc được rải đều tăm tắp cho tất cả các Worker Subtasks/Pods trên toàn cụm K8s, triệt tiêu hoàn toàn rủi ro bị bẫy Hot Key ở Tầng 2.
- **Thực thi Bất Đồng Bộ Thuần Túy (Pure Asynchronous & Lock-Free Execution)**: Các Worker Subtasks ở Tầng 2 hoạt động hoàn toàn độc lập. Không cần cơ chế đồng bộ (No Inter-Worker Synchronization), không cần khóa (No Locks), không chờ rào cản (No Barriers) — cứ có gói Context gửi sang là lập tức tính toán bất đồng bộ và bắn kết quả ra Sink.
- **Tối ưu Tài nguyên Độc lập với Flink Fine-Grained Resource Management**:
  - Mô hình 2 rất phù hợp để áp dụng tính năng [Apache Flink Fine-Grained Resource Management](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/finegrained_resource/) (phân chia `SlotSharingGroup` và `ResourceSpec` riêng cho từng Tầng):
  - **Tầng 1 (State Gatekeeper)**: Cần lưu trữ Ring Buffer 96 buckets và Profile State $\rightarrow$ Cấp phát **High Memory (RAM Heap/Off-heap lớn), Low CPU vCores**.
  - **Tầng 2 (Parallel Rule Evaluators)**: Không giữ State, chỉ làm nhiệm vụ tính toán CPU-heavy duyệt cây logic Rule $\rightarrow$ Cấp phát **High CPU vCores, Low Memory**.
  - **Scale-Out Độc lập Cấp Độ Tầng (Independent Layer Elasticity)**: Cho phép K8s HPA chỉ scale-out thêm Pods/TaskManagers cho riêng Tầng 2 khi lượng Rule hoặc thông lượng tăng cao, mà không cần lãng phí bổ sung bộ nhớ cho Tầng 1.

##### **4. Giải Pháp Triệt Tiêu 100% Phân Mảnh Tài Nguyên (Harmonic Resource Pairing)**
Khi áp dụng Fine-Grained Resource Management, rủi ro lớn nhất là **Hiện tượng Phân Mảnh Tài Nguyên (Resource Fragmentation)** do thuật toán Bin-Packing cắt các Slots lẻ không chia hết cho dung lượng phần cứng của TaskManager Pod (để thừa lẻ vài % CPU hoặc RAM mà không tạo nổi 1 Slot mới).

Để triệt tiêu hoàn toàn phân mảnh ($0\%$ Fragmentation), hệ thống áp dụng Quy tắc **Số học Ghép nối Hoàn hảo (Harmonic Resource Pairing)**:

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1 TASKMANAGER POD (Ví dụ: 8.0 vCPU, 10.0 GB RAM)                       │
│                                                                        │
│ ┌──────────────────────────────────┐ ┌───────────────────────────────┐ │
│ │ SLOT 1: TẦNG 1 (Stateful Stream) │ │ SLOT 2: TẦNG 2 (Compute Pool) │ │
│ │ • CPU: 1.0 Core                  │ │ • CPU: 7.0 Cores              │ │
│ │ • RAM: 8.0 GB (Lưu trữ State)    │ │ • RAM: 2.0 GB (Compute RAM)   │ │
│ └──────────────────────────────────┘ └───────────────────────────────┘ │
│                                                                        │
│ TỔNG CỘNG: 1.0 + 7.0 = 8.0 Cores  |  8.0 + 2.0 = 10.0 GB RAM           │
│ ===> TÀI NGUYÊN DƯ THỪA (RESOURCE FRAGMENTATION) = 0%!                 │
└────────────────────────────────────────────────────────────────────────┘
```

- **Nguyên lý Cặp Slot Bù Trừ (Perfect Complementary Sizing)**:
  Căn chỉnh thông số CPU và RAM của Tầng 1 và Tầng 2 sao cho tổng dung lượng của một Cặp Slot (`Slot 1 + Slot 2`) vừa khít **100% dung lượng phần cứng của 1 TaskManager Pod**.
- **Cấu hình trực tiếp bằng Java Flink DataStream API**:
  ```java
  // Tầng 1 (Stateful Gatekeeper): Ăn nhiều RAM (8GB), dùng ít CPU (1 Core)
  SlotSharingGroup ssgLayer1 = SlotSharingGroup.newBuilder("ssg-layer1")
          .setCpuCores(1.0)
          .setTaskHeapMemory(MemorySize.ofGibiBytes(8))
          .build();

  // Tầng 2 (Stateless Compute): Ăn trọn phần CPU còn lại (7 Cores), dùng ít RAM (2GB)
  SlotSharingGroup ssgLayer2 = SlotSharingGroup.newBuilder("ssg-layer2")
          .setCpuCores(7.0)
          .setTaskHeapMemory(MemorySize.ofGibiBytes(2))
          .build();

  // Gán SlotSharingGroup cho từng Operator trong Pipeline Topology
  streamLayer1.process(stateGatekeeperOperator).slotSharingGroup(ssgLayer1);
  streamLayer2.process(ruleEvaluatorOperator).slotSharingGroup(ssgLayer2);
  ```

##### **5. Khả Năng Scale-Out Vô Hạn & Ma Trận Ưu Điểm Kỹ Thuật**
- **Vượt qua trần phần cứng 1 Node (Horizontal Scale-Out Only)**: Tầng 2 là Stateless, có thể Scale-Out ra hàng chục TaskManager Pods (hàng trăm vCPUs) trên K8s mà không bị giới hạn bởi phần cứng của 1 node duy nhất.
- **Giải phóng hoàn toàn Tầng Keyed**: Tầng 1 chỉ tốn $\approx 0.1\mu s$ để update mảng State, giúp 1 Keyed Subtask chịu tải tới $500.000 - 1.000.000\text{ msgs/s}$ trước khi xuất hiện nghẽn.

##### **6. Thách Thức Kỹ Thuật & Hạn Chế Cốt Lõi**
- **Tải Mạng Cực Kỳ Nặng (Heavy Network Bottleneck)**: Vì **mọi bản tin event** đều phải đóng gói và bắn qua mạng giữa Tầng 1 và Tầng 2, băng thông mạng nội bộ chịu áp lực rất lớn (bắt buộc phải áp dụng giải pháp Mảng số thực nén `byte[]` ở Mục 2.6.2 để hạ tải mạng).
- **Kết quả Downstream Lệch Thứ Tự**: Tầng 2 xử lý song song bất đồng bộ nên kết quả đầu ra bắn về Sink có thể bị lệch thứ tự vài millisecond $\rightarrow$ Bắt buộc Sink phải sắp xếp lại theo `event_time`/`sequence_id`.

---

## 3. Cơ Chế Hot-Reload Rules Động (Broadcast State Architecture)

Để thay đổi logic tính toán, thêm/sửa/xóa luật mà không dừng Flink Job:

1. **Rule Stream & Broadcast**:
   - Các định nghĩa Rule được đẩy vào Kafka topic `rule`.
   - Flink Job đọc topic này và chuyển thành một `BroadcastStream`.
2. **Broadcast State**:
   - `BroadcastStream` kết hợp với Data Stream qua toán tử `connect()`.
   - Tất cả các parallel operator instances đều nhận được bản sao của Rule mới và lưu vào Flink `BroadcastState`.
3. **Cập nhật Atomic**:
   - Khi có Rule mới/cập nhật, operator áp dụng thay đổi ngay tức thì vào bộ nhớ local runtime của instance mà không làm gián đoạn việc tính toán Data Stream.

---

## 4. Module Đánh Index và Lọc Nhanh (Inverted Index Trigger Filter Module)

Khi số lượng Rule lên tới hàng trăm nghìn hoặc hàng triệu, việc lặp qua từng Rule để kiểm tra cho mỗi event là quá tốn kém. Hệ thống tích hợp **Module chỉ mục nghịch đảo (Inverted Index)**:

### 4.1 Cơ Chế Lập Index
- Dựa vào trường điều kiện kích hoạt `trigger_criteria` của các Rule (ví dụ: cặp `source`, `version`, và các thuộc tính điều kiện trigger ban đầu).
- Tạo danh mục tra cứu: `(Trigger Field, Operator, Value) -> List<RuleID>`.
- Tính toán tần suất xuất hiện (**Document Frequency - DF**) của các trường trigger.

### 4.2 Luồng Lọc Nhanh 2 Cấp (Two-Tier Filtering)
1. **Tra cứu Inverted Index ($O(1)$)**: Khi event đến, trích xuất các thông tin `source`, `version` và các trường xuất hiện trong event để tra cứu nhanh tập các candidate rules có khả năng khớp.
2. **Loại bỏ dựa trên 2 trường hiếm nhất**: Chọn 2 trường có DF thấp nhất trong các điều kiện trigger làm trục lọc chính. Thực hiện phép giao (Intersection) của 2 tập candidate để thu hẹp số lượng rule cần đánh giá xuống mức tối thiểu trước khi thực thi `condition_tree` sâu hơn.

---

## 5. Tóm Tắt Luồng Xử Lý Sự Kiện (End-to-End Flow)

```
[ Data Event ] ──► Ingestion & Validation
                         │
                         ▼
             [ Trigger Inverted Index ] ──► (Lọc nhanh Candidate Rules)
                         │
                         ▼
             [ keyBy(customer_id) ]
                         │
                         ├─► Cập nhật Customer Profile State (Static/Dynamic attributes)
                         ├─► Cập nhật Ring Buffer Bucket MapState (Phương án 1 hoặc 2)
                         │
                         ▼
             [ Rule Evaluator Engine ] ◄── [ Broadcast State (Hot-reload Rules) ]
                         │
                         ▼
                  [ Result / Sink ]
```
