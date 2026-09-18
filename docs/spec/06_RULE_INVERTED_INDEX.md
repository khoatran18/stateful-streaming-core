# Kiến trúc Rule Inverted Index

## 1. Đặt tên 2 tập chỉ mục (Index Schema)

Tầng trigger được tổ chức thành 2 tập chỉ mục riêng biệt:

### Tập 1: ExactMatchIndex (Chỉ mục định danh chính xác)
- **Toán tử xử lý:** `==`, `IN` (đã làm phẳng/flatten), `IN_DATASET`.
- **Cấu trúc:** `Map<Key, RoaringBitmap>` với Key là `field:value` (hoặc `field:dataset_id`).
- **Đặc tính:** Băm chính xác $O(1)$. Chỉ bật bit cho các rule khớp cụ thể giá trị đó.

### Tập 2: ComplexPredicateIndex (Chỉ mục sự hiện diện của toán tử khó)
- **Toán tử xử lý:** `>`, `<`, `!=`, `CONTAINS`, `ENDS_WITH`, `REGEX`...
- **Cấu trúc:** `Map<Field, RoaringBitmap>` (Chỉ lập chỉ mục theo tên Field, hoàn toàn không quan tâm giá trị).
- **Đặc tính:** Đánh dấu sự hiện diện. Rule nào kiểm tra trường `amount` bằng toán tử khó (ví dụ `amount > 5000000`) thì bật bit 1 tại key `"amount"`.

Giả định trong thực tế, tỷ lệ các rule chứa toán tử khó (Complex) là tương đối ít, giúp tối ưu chuỗi bit ở Tập 2.

## 2. Quản lý Đa phiên bản & Đa nguồn (Multi-Source / Versioning)

Mỗi Source và mỗi Version cấu hình sở hữu một gói chỉ mục độc lập, đóng gói vào một đối tượng bất biến (Immutable State):

```text
Class RuleIndexState:
    version: int / string
    active_fields: Set<String>                     // Danh sách các field có dùng trong bộ rule này
    exact_index: Map<String, RoaringBitmap>        // Tập 1: field:value -> Bitmap
    complex_index: Map<String, RoaringBitmap>      // Tập 2: field -> Bitmap
    slot_to_rule: Array<RuleInstance>              // Slot ID -> Rule Definition
```

Khi CDC đẩy bản cập nhật Rule (xóa, thêm, cập nhật) qua Broadcast State, worker Flink sẽ **tạo một bản Clone (Copy-On-Write)** của `RuleIndexState` hiện tại ở background.
Sau khi áp dụng các thay đổi (Thêm/Sửa/Xóa rule) trên bản nháp này xong, worker thực hiện **Atomic Pointer Swap** sang instance mới, bảo đảm zero-downtime.

## 3. Luồng xử lý khi Event đến (Runtime Pipeline)

Giả sử Event gửi lên 100 trường, nhưng bộ rule hiện tại chỉ định nghĩa một số trường nhất định.

```text
Incoming Event (100 trường)
          │
          ▼
[ Lọc trường giao thoa: Event.keys ∩ State.active_fields ]
(Loại bỏ ngay 90+ trường thừa, chỉ giữ lại ~5-10 trường thực tế có trong rule)
          │
          ├───────────────────────────────────────────┐
          ▼                                           ▼
  [ Tra cứu Tập 1: Exact ]                    [ Tra cứu Tập 2: Complex ]
  Với mỗi field:                              Với mỗi field:
  BM_exact = exact_index.get("field:val")     BM_complex = complex_index.get("field")
          │                                           │
          ▼                                           ▼
     Exact_BM_List                               Complex_BM_List
          │                                           │
          └─────────────────────┬─────────────────────┘
                                ▼
         [ SIMD FastOr: Gom toàn bộ chuỗi Bit ]
 Candidates = OR(Exact_BM_List)  |  OR(Complex_BM_List)
                                │
                                ▼
         [ Condition Tree: Thẩm định chi tiết ]
  Chạy short-circuit evaluation trên tập Candidates đã lọc
```

## 4. Ví dụ kịch bản cụ thể

Bộ Rule đang có 3 luật:
- **Rule 1 (Slot 1)**: `country == 'VN'` $\rightarrow$ Đưa vào Tập 1 - Tập Exact (`country:VN`).
- **Rule 2 (Slot 2)**: `amount > 10000000` $\rightarrow$ Đưa vào Tập 2 - Tập Complex (`amount`).
- **Rule 3 (Slot 3)**: `mcc == 5411` AND `user_agent CONTAINS "Chrome"`:
  - Điều kiện `mcc == 5411` $\rightarrow$ Đưa vào Tập 1 (`mcc:5411`).
  - Điều kiện `user_agent CONTAINS ...` $\rightarrow$ Đưa vào Tập 2 (`user_agent`).

Khi Event đến: `{ country: "VN", amount: 5000000, ip: "1.1.1.1", ... (97 trường khác) }`:
1. **Lọc trường:** Hệ thống thấy Event có 2 trường xuất hiện trong rule là `country` và `amount` (bỏ qua `ip` và 97 trường còn lại).
2. **Tra cứu:**
   - **Tập 1:** Tra `country:VN` $\rightarrow$ Trả về `[Slot 1]`.
   - **Tập 2:** Tra `amount` $\rightarrow$ Trả về `[Slot 2]`.
3. **Phép OR:** `Candidates = [Slot 1] OR [Slot 2] = [Slot 1, Slot 2]`.
4. **Condition Tree thẩm định:**
   - **Rule 1:** `country == 'VN'` $\rightarrow$ Khớp (Trigger).
   - **Rule 2:** `amount > 10000000` (giá trị thực tế là 5M) $\rightarrow$ Loại.
   - **Rule 3:** Bị gạt bỏ ngay từ bước Index (không tốn tài nguyên chạy Condition Tree).

Mô hình này giữ cho tốc độ lọc ban đầu luôn đạt $O(1)$, giảm tải $95\%$ khối lượng tính toán nặng cho CPU của Flink worker.

## 5. Cơ chế lưu trữ Value trong Inverted Index (Roaring Bitmap & ID Allocator)

Để giải quyết bài toán biểu diễn tập hợp các Rule ID khớp với một điều kiện, mô hình chuẩn nhất là sử dụng **Roaring Bitmap Inverted Index** kết hợp với **ID Allocator (Dense Slot Manager)**.

### 5.1. Kiến trúc tổng thể của Bitwise Matching

Dựa trên việc đã phân vùng Index theo `source:version` ở vòng ngoài, số lượng rule bên trong đã được thu hẹp đáng kể. Bước matching Inverted Index lúc này dùng phép hội (OR) để gom tất cả các rule có khả năng khớp (Candidates), thay vì phép giao (AND) khắt khe như các Search Engine truyền thống.

```text
        [ Incoming Event ]
                 │
                 ▼ (Hash lookups: O(1))
Key "mcc:5411"    ──► RoaringBitmap A: [1, 5, 20]
Key "country:VN"  ──► RoaringBitmap B: [20, 31]
                 │
                 ▼ (Bitwise OR cực nhanh bằng SIMD)
Matched Rule Slots ──► RoaringBitmap C = A | B: [1, 5, 20, 31]
                 │
                 ▼ (Loại bỏ các rule đã bị xóa bằng FreeSlotsBitmap)
Valid Candidates   ──► C AND NOT(FreeSlotsBitmap)
                 │
                 ▼ (Run Condition Tree)
Condition Tree Evaluation
```

### 5.2. Quản lý Rule ID, Thu hồi Bit Index và Lazy Deletion

Vì các rule trong hệ thống được tạo, sửa, xóa liên tục, việc lục tìm và xóa tận gốc từng bit trong hàng ngàn key của Inverted Index khi xóa 1 rule là cực kỳ đắt đỏ. Giải pháp tối ưu là sử dụng **Lazy Deletion** (Xóa lười) kết hợp với **State Bitmap (Chuỗi bit trạng thái)**.

**Cấu trúc bảng quản lý Slot:**
- `slot_to_rule`: Array / Slice `[]RuleMetadata`. Chỉ số mảng chính là BitIndex ($0, 1, 2, \dots, N-1$).
- `rule_to_slot`: `HashMap<RuleID, uint32>`. Lưu mapping ngược để tra cứu nhanh.
- `free_slots_bitmap`: Một `RoaringBitmap` đóng vai trò lưu trạng thái của Slot.
  - **Bit = 1**: Slot đang trống (rule đã bị xóa hoặc chưa cấp phát).
  - **Bit = 0**: Slot đang được sử dụng bởi 1 rule.
- `max_allocated_index`: `uint32`. Vị trí bit cao nhất đã từng cấp phát.

**Cơ chế Thêm / Xóa / Sửa (CRUD):**

| Thao tác | Cơ chế thực thi | Chi phí |
| :--- | :--- | :--- |
| **Xóa rule (DELETE)** | 1. Tìm `bit_index = rule_to_slot[rule_id]`.<br>2. Bật bit đó thành `1` trong `free_slots_bitmap` (Đánh dấu là đã xóa).<br>3. **KHÔNG CẦN** chui vào Inverted Index để xóa rác. Rác sẽ bị mask out bằng toán tử `AND NOT(free_slots_bitmap)` khi event đến. | $O(1)$ |
| **Thêm rule mới (ADD)** | 1. Tìm 1 bit `1` trong `free_slots_bitmap` để cấp phát. Nếu không có, lấy `++max_allocated_index`.<br>2. Chuyển bit vừa được cấp phát về `0` (Đánh dấu đã sử dụng).<br>3. Rule mới duyệt qua các điều kiện của mình và update (thêm bit) vào Inverted Index. (*Lưu ý: Các bit rác cũ từ rule trước đó nếu còn sót lại ở key khác sẽ chỉ gây ra đánh giá sai ở tập Candidates, nhưng sẽ bị chặn đứng an toàn tại Condition Tree*). | $O(K)$ |
| **Cập nhật rule (UPDATE)**| Thực hiện DELETE theo `bit_index` cũ $\rightarrow$ ADD lại (hoặc tái sử dụng chính slot đó để cập nhật key mới). | $O(K)$ |

### 5.3. Vấn đề "Co giãn mảng bit" (Array Expansion)

Trong Roaring Bitmap, không cần cơ chế nhân đôi dung lượng (x2) thủ công:
- **Cách Roaring Bitmap hoạt động:** Không phân bổ một chuỗi bit liên tục dạng `byte[]` hay `long[]` từ $0$ đến $N$. Roaring chia dải 32-bit thành các chunk cố định $2^{16}$ (65,536 bit).
  - Nếu một chunk có ít hơn 4.096 bit: Lưu dạng Array (mảng các số `uint16`).
  - Nếu nhiều hơn 4.096 bit: Chuyển thành Bitset ($8\text{KB}$ cố định).
  - Nếu chứa các chuỗi bit liên tiếp: Nén dạng Run (RLE - Run-Length Encoding).
- **Mở rộng tự động:** Khi cấp phát một bit index lớn hơn (ví dụ từ bit 10,000 nhảy lên bit 70,000), Roaring Bitmap chỉ tạo thêm một container mới ở tầng trên (chỉ tốn vài byte con trỏ). Không có chi phí copy mảng hay reallocate toàn bộ bitmap như `java.util.BitSet`.

### 5.4. Thiết kế đồng bộ & Đảm bảo hiệu năng cao (Concurrency & Zero Downtime)

Trong các hệ thống throughput lớn, luồng matching event chạy liên tục hàng microsecond, không thể dùng Global Lock (Mutex) chặn quá trình match mỗi khi có rule thêm/sửa/xóa.

**Chiến lược: Copy-On-Write (COW) kết hợp Double Buffering**

```text
[ Write Thread (Worker) ]
   │
   ├─► Tạo bản clone của Inverted Index (hoặc chỉ clone các RoaringBitmap bị thay đổi)
   ├─► Thêm/Xóa bit trên bản nháp
   └─► Atomic Pointer Swap (Atomic.set) trỏ sang Index mới
                                 ▲
[ Read Threads (Workers) ]       │
   ├─► Event 1 ──► Đọc Index v1 ──┘
   └─► Event 2 ──► Đọc Index v2 (sau khi swap)
```

- **Roaring Bitmap hỗ trợ COW:** Thư viện Roaring Bitmap (C++, Java, Go, Rust) đều hỗ trợ `Clone()` cực kỳ nhanh (dùng cơ chế copy chunk con trỏ). Khi cập nhật một rule, chỉ clone các bitmap liên quan đến các key của rule đó, cập nhật xong thì hoán đổi con trỏ atomic.
- **Tối ưu Matching Event:**
  - Lấy danh sách bitmap từ các key: `bm1 = index.get("mcc:5411")`, `bm2 = index.get("country:VN")`.
  - Thực hiện gộp bit: `RoaringBitmap.and(bm1, bm2)`. Quá trình này dùng tập lệnh SIMD (AVX-512 / AVX2) của CPU, xử lý hàng triệu bit chỉ mất vài nano-giây.
  - Duyệt qua kết quả: `iterator = resultBitmap.getIntIterator()`, lấy ra từng `bit_index` để trỏ tới Rule và thẩm định `condition_tree`.
