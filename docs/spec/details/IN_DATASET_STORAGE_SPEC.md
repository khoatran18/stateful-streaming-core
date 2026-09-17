# Chi tiết cơ chế lưu trữ IN_DATASET

## Cơ chế lưu trữ và băm Tuple Dataset (Hashing & Encoding)

### 1. Băm bằng Length-prefixed Binary Encoding (Nguyên lý & Lý do sử dụng)
Khi ghép nhiều trường thành một bộ (tuple), lỗi logic phổ biến nhất là dùng ký tự nối (như dấu `_`). Nếu giá trị của trường chứa chính ký tự đó, các bộ giá trị khác nhau sẽ bị dính thành một chuỗi giống hệt nhau (ví dụ: `("A", "B_C")` và `("A_B", "C")` đều ra `"A_B_C"`).

Để triệt tiêu hoàn toàn rủi ro này mà không tốn CPU xử lý chuỗi:
- **Cấu trúc**: Mỗi trường được biểu diễn bằng `[Độ dài mảng byte (2 bytes)]` + `[Dữ liệu mảng byte thực tế]`.
- **Cơ chế ghép**:
  $$\text{Encoded Bytes} = [\text{len}_1][\text{bytes}_1] + [\text{len}_2][\text{bytes}_2] + \dots$$
- **Ví dụ**: Cặp `("TABLET", 5411)`:
  - **Trường 1** (`"TABLET"`): Ghi 2 byte độ dài mang giá trị `6`, liền sau là 6 byte của chuỗi `"TABLET"`.
  - **Trường 2** (`5411`): Ghi 2 byte độ dài mang giá trị `4`, liền sau là 4 byte nhị phân của số nguyên `5411`.
- **Ưu điểm**: Khi đưa vào hàm băm, engine đọc theo ranh giới byte chính xác tuyệt đối mà không cần quan tâm chuỗi bên trong chứa ký tự gì, không cần escape và không cần cấp phát chuỗi tạm (zero object allocation) trên JVM.

### 2. Tạo Key lưu trữ bằng 64-bit Hash (xxHash64)
Thay vì lưu toàn bộ chuỗi byte dài và biến thiên vào bộ nhớ/RocksDB, ta cho toàn bộ mảng byte length-prefixed chạy qua `xxHash64`:
- **Ánh xạ về số nguyên cố định**: Bất kể tuple có 2 trường hay 5 trường, chuỗi dài 10 byte hay 100 byte, `xxHash64` đều tính toán và quy đổi về duy nhất 1 số nguyên `long` (8 bytes / 64 bits).
- **Cơ chế trộn bit**: `xxHash64` chia dữ liệu thành các khối 32 byte, dùng các phép toán nhân số nguyên tố lớn kết hợp quay bit song song trên các thanh ghi CPU. Chỉ cần 1 bit đầu vào thay đổi, giá trị 64-bit đầu ra sẽ bị xáo trộn ngẫu nhiên (hiệu ứng Avalanche).
- **Thiết kế Key 16 Bytes (Phân mảnh theo Dataset)**:
  - Trong thực tế, hệ thống phải lưu trữ nhiều Dataset khác nhau trên cùng một RocksDB. Để phân tách dữ liệu, tên của tập dữ liệu (`dataset_id`) cũng được đưa qua `xxHash64` để ép về số nguyên 8 bytes.
  - Định dạng khóa cuối cùng lưu xuống DB có độ dài **16 bytes**: `[8 bytes xxHash64(dataset_id)] + [8 bytes xxHash64(tuple)]`.

```text
|<------------------------- 16 BYTES ---------------------------->|
+--------------------------------+--------------------------------+
|     dataset_id (8 bytes)       |       tuple_hash (8 bytes)     |
|   (ID số nguyên tự tăng hoặc   |  (xxHash64 của mảng byte tuple |
|   xxHash64 của tên dataset)    |      length-prefixed)          |
+--------------------------------+--------------------------------+
```

- **Hiệu năng lưu trữ**:
  - Thay vì lưu các chuỗi string cồng kềnh, State chỉ cần lưu các chuỗi byte tĩnh 16-byte phẳng trong RocksDB.
  - Giúp index cực gọn, toàn bộ key của 100–300 bộ tuple chỉ chiếm chưa tới 4.8 KB bộ nhớ (do $300 \text{ tuple} \times 16 \text{ bytes/key} = 4.800 \text{ bytes} \approx 4.8 \text{ KB}$), nằm trọn trong L1/L2 Cache của CPU.

### 3. Bản chất tỷ lệ đụng độ trên không gian 64-bit
Nhiều người e ngại hàm băm nén dữ liệu sẽ gây trùng lặp (đụng độ). Tuy nhiên, với không gian 64-bit và số lượng 100–300 phần tử, xác suất này gần như bằng 0, phù hợp với mục đích thông báo và thống kê.

- **Độ lớn của không gian 64-bit**:
  Một số 64-bit có $2^{64}$ khả năng:
  $$2^{64} = 18.446.744.073.709.551.616 \text{ giá trị (khoảng 18,4 tỷ tỷ)}$$
  Tưởng tượng bạn có hơn 18 tỷ tỷ chiếc hộp rỗng. Băm một tuple là thả một hạt đậu ngẫu nhiên vào một trong số các hộp đó.

- **Công thức xác suất đụng độ (Birthday Paradox)**:
  Khi thả $N$ phần tử vào $2^{64}$ chiếc hộp, xác suất có ít nhất 2 phần tử rơi trùng vào một hộp được tính bằng:
  $$P \approx \frac{N^2}{2 \times 2^{64}} = \frac{N^2}{2^{65}}$$

- **Tính toán thực tế với $N = 300$ cặp**:
  $$P \approx \frac{300^2}{2^{65}} = \frac{90.000}{36.893.488.147.419.103.232} \approx 2,44 \times 10^{-15}$$
  Con số này tương đương $0,000000000000244\%$.

- **Xác suất va chạm với luồng Event đến**:
  Giả sử hệ thống lưu sẵn 300 hash trong State. Một event rác (không thuộc 300 cặp này) bay đến, xác suất để hash của nó vô tình trùng khớp với 1 trong 300 hash có sẵn là:
  $$P_{\text{false\_hit}} = \frac{300}{2^{64}} \approx 1,62 \times 10^{-17}$$
  Kể cả hệ thống xử lý 100 triệu event mỗi ngày, về mặt thống kê, bạn cần chạy hệ thống liên tục hàng triệu năm mới có thể gặp một lần đụng độ ngẫu nhiên.
