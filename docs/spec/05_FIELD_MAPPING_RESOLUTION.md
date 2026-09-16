# 1. Problem Statement
Hệ thống xử lý dòng dữ liệu lớn (Streaming Stateful Engine) tích hợp nhiều nguồn dữ liệu khác nhau (Kafka realtime topics như CPM, GNOTI, EVT và các bảng Hive/Batch CDC như L1_SUB_MNGT_*, trans_daily_his). Khi số lượng bài toán nghiệp vụ (Use Cases / Rules) tăng lên, nếu mỗi bài toán tự định nghĩa một lớp Mapper riêng để chuyển đổi toàn bộ event sang schema nghiệp vụ của bài đó, hệ thống sẽ gặp các bế tắc nghiêm trọng:

## Đối với luồng Stream
Khi một event từ một topic nguồn (ví dụ: topic CPM) đi vào hệ thống, nếu có $N$ bài toán/rule đang quan tâm tới nguồn này và mỗi bài có một Mapper riêng, event gốc sẽ bị ánh xạ và nhân bản thành $N$ event mới. Mỗi bản sao này lại phải chạy độc lập qua khối kiểm tra Trigger nội bộ và duyệt qua Condition Tree. 
Hậu quả: Khối lượng tính toán (CPU), chi phí tuần tự hóa/giải tuần tự hóa (SerDe), và lưu lượng truyền tải trên mạng (Network Shuffle khi keyBy(msisdn)) tăng vọt theo cấp số nhân:
$$\text{Tải xử lý} = O(\text{Số lượng Events} \times N_{\text{Rules}})$$
Điều này trực tiếp gây nghẽn luồng xử lý thời gian thực, tăng Garbage Collection (GC) pauses và đẩy độ trễ hệ thống lên cao.

## Đối với luồng Batch
Dữ liệu từ luồng Batch định kỳ được đẩy qua Kafka để cập nhật trạng thái nền (User Profile, gói cước hoạt động, cờ rủi ro/blacklist) vào State gắn với từng msisdn. Nếu mỗi bài toán có một bộ Mapper riêng và ghi trực tiếp các trường sau mapper vào State: cùng một thông tin thực tế của khách hàng (ví dụ: "số tiền giao dịch" hoặc "trạng thái gói cước") sẽ bị lưu lặp đi lặp lại dưới nhiều tên biến khác nhau tùy theo mapper của từng bài (amount, trans_money, so_tien_gd, val). 
Hậu quả: Dung lượng RocksDB StateBackend hoặc heap RAM phình to gấp $N$ lần. Kích thước Checkpoint khổng lồ dẫn đến nguy cơ sập Job khi Savepoint/Checkpoint vượt quá timeout.

# 2. Giải pháp đề xuất
Thay vì để dữ liệu (Data Stream) bị biến đổi và nhân bản thụ động qua nhiều tầng Mapper, hệ thống áp dụng kỹ thuật Biến đổi cây cú pháp quy tắc (AST Rule Rewriting) kết hợp Chỉ mục đảo (Inverted Index Trigger) trước khi phân phối quy tắc vào dòng xử lý chính.

```text
[PostgreSQL: Rule Config]   ──┐
                              ├──► [Rule Compilation & Rewriter Service]
[MongoDB: Mapper Registry]  ──┘    (Liên kết qua mapper_id)
                                             │
                                             ▼
                                1. Phân giải: Sau Mapper -> Tên gốc (Raw Schema)
                                2. Tạo Inverted Index cho Trigger
                                3. Đóng gói Compiled Rule Envelope
                                             │
                                             ▼
[Raw DataStream (Stream/Batch)] ──► [Process Broadcast]
                                       ├── Đánh giá Trigger trực tiếp trên Payload gốc
                                       └── Đọc/Ghi State theo Entity chuẩn
```

## Cơ chế hoạt động:
**Giữ trải nghiệm khai báo trực quan cho người cấu hình (Developer / Business User):**
Người viết Rule không cần nhớ cấu trúc JSON lồng nhau phức tạp của từng topic nguồn. Rule chỉ cần khai báo dựa trên các định danh nghiệp vụ chuẩn hóa (tên trường sau mapper) và trỏ tới một mapper_id xác định.

**Liên kết Mapper và Rule trên tầng Broadcast/Compilation:**
Mỗi Mapper được định danh bởi một mapper_id chứa bảng tra cứu ánh xạ giữa tên trường sau mapper và tên trường trường gốc (JsonPath/Column name) ở nguồn. Trước khi đưa vào luồng streaming chính, hệ thống đối chiếu Rule với mapper_id để dò ngược và viết lại (rewrite) toàn bộ các trường trong biểu thức trigger và condition_tree về đúng tên trường ở schema gốc.

## Ví dụ minh họa Rewrite Rule:
**Mapper cấu hình (mapper_id = "MAPPER_CPM_V1"):**
```json
{
  "mapper_id": "MAPPER_CPM_V1",
  "source_topic": "CPM",
  "mapping": {
    "trans_amount": "request.content.amount",
    "service_type": "request.content.serviceCode",
    "account_id": "request.content.identifyValue"
  }
}
```

**Rule ban đầu (Định nghĩa theo trường sau mapper):**
```json
{
  "rule_id": "RULE_TOPUP_50K",
  "mapper_id": "MAPPER_CPM_V1",
  "trigger": "service_type == 'TOPUP' && trans_amount >= 50000"
}
```

**Rule sau khi Rewrite (Compiled Rule Payload):**
```json
{
  "rule_id": "RULE_TOPUP_50K",
  "source_topic": "CPM",
  "raw_trigger": "request.content.serviceCode == 'TOPUP' && request.content.amount >= 50000",
}
```

## Hiệu quả đạt được:
* **Tối ưu tính toán Stream:** Event không bị clone ra nhiều bản ghi, không phải chạy qua bất kỳ class mapper trung gian nào. Nếu không thỏa mãn trigger, event bị loại bỏ ngay lập tức.
* **Bảo toàn bộ nhớ State:** State chỉ lưu trữ các trường gốc của batch (được truyền vào stream)

# 3. Đồng bộ nhiều nguồn và Tính toán Window

> **[!NOTE]**
> **Phạm vi dự án (Project Scope):** Nội dung tại mục 3 này hiện tại chỉ là thiết kế mang tính định hướng để mở rộng hệ thống trong tương lai. Các tính năng tính toán Window và cấu hình Metrics đa nguồn chưa nằm trong scope của Phase hiện tại.
> **Ví dụ bên dưới chỉ mô phỏng schema rule và logic thiết kế, có thể chưa chuẩn xác.**

Với các bài toán đặc thù cần kết hợp dữ liệu từ 3–4 nguồn khác nhau (như bài toán A1 gộp TDH, EVT, GNOTI, CPM để dedupe hoặc các bài toán tính tổng trên nhiều kênh thanh toán qua Window), nếu quay về dùng tên trường thô của từng nguồn thì condition_tree sẽ trở nên cồng kềnh vì phải viết phép cộng/tổng hợp liệt kê tên trường của cả 4 nguồn (CPM.amount + GNOTI.trans_amount + EVT.money + ...).

Giải pháp cho trường hợp này là áp dụng Trừu tượng hóa chỉ số (Metric Abstraction) thông qua cấu hình metrics registry đa nguồn.

## Cơ chế hoạt động:
## Cơ chế hoạt động (Định hướng thiết kế):
Thay vì khai báo rời rạc, logic trích xuất Metric đa nguồn và cấu hình Window sẽ được đóng gói trực tiếp vào một Node trạng thái (Stateful Node) bên trong chính `condition_tree`. Điều này giúp bảo toàn sự nhất quán của Rule Schema hiện tại (tương tự như toán tử `IS_FIRST_ARRIVAL`).

**Cấu hình mẫu cho bài toán đồng bộ Window đa nguồn:**
```json
{
  "rule_id": "RULE_CROSS_CHANNEL_ALERT",
  
  "trigger_criteria": [
    { "source": "CPM", "version": "v1", "conditions": [...] },
    { "source": "GNOTI", "version": "v1", "conditions": [...] },
    { "source": "EVT", "version": "v1", "conditions": [...] },
    { "source": "TDH", "version": "v1", "conditions": [...] }
  ],

  "condition_tree": {
    "type": "AND",
    "children": [
      {
        "type": "CONDITION",
        "expression": {
          // Toán tử tính tổng trên Window
          "op": "WINDOW_SUM",
          
          // Trích xuất chung metric (VD: Tổng chi tiêu) từ 4 nguồn (Metric Abstraction)
          "metric_mapping": {
            "CPM": "request.content.amount",
            "GNOTI": "trans_amount",
            "EVT": "event_value.trans_money",
            "TDH": "trans_daily_amount"
          },
          
          // Khai báo cấu hình Window trực tiếp trong Node
          "window_size": "10m",
          "slide_size": "1m",
          
          // Điều kiện đánh giá cuối cùng trên kết quả của Window
          "compare_op": ">=",
          "compare_value": 1000000
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          // Window thứ hai (VD: Tổng discount)
          "op": "WINDOW_SUM",
          "metric_mapping": {
            "CPM": "request.content.discount",
            "GNOTI": "discount_amount",
            "EVT": "event_value.discount_money",
            "TDH": "trans_daily_discount"
          },
          "window_size": "10m",
          "slide_size": "1m",
          "compare_op": "<",
          "compare_value": 50000
        }
      }
    ]
  }
}
```

# 4. Bảng tổng hợp so sánh các phương án

| Tiêu chí                                     | Phướng án 1: Dùng Mapper truyền thống (Map trước, Check sau) | Phương án 2: Dùng Tên thô không Mapper (Raw Everywhere)                   | Phương án 3: Rewrite Rule & Metric Abstraction (Đề xuất)                              |
|:---------------------------------------------|:-------------------------------------------------------------|:--------------------------------------------------------------------------|:--------------------------------------------------------------------------------------|
| **Tính tiện dụng khi viết Rule**             | Cao (Tên trường nghiệp vụ gọn gàng).                         | Thấp (Người viết rule phải tự nhớ JSON path của từng topic nguồn).        | Cao (Như phương án 1).                                                                |
| **Mức tiêu thụ CPU / SerDe luồng Stream**    | Cao (Event nhân bản $N$ lần qua $N$ mapper rồi mới check).   | Thấp (Check trực tiếp trên trường thô).                                   | Thấp (Như phương án 2).                                                               |
| **Dung lượng State msisdn (Batch & RT)**     | Cao (Lưu lặp các biến sau mapper của từng rule).             | Trung bình (Dễ bị phân mảnh cấu trúc).                                    | Tối ưu nhất (Chỉ lưu User Profile chuẩn; Window chỉ lưu giá trị số của metric).       |
| **Xử lý Window đa nguồn (A1, Aggregations)** | Phải map toàn bộ 4 nguồn về 1 schema lớn cồng kềnh.          | Condition tree cực kỳ phức tạp (phải cộng gộp tên trường của cả 4 nguồn). | Tinh gọn & Hiệu năng cao (Condition tree ngắn gọn; chỉ extract đúng metric cần tính). |
