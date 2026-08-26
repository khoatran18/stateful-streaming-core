# Phân Tích Chuyên Sâu Hiệu Năng So Sánh Chuỗi: Exact Matching vs. Pattern Matching (Regex/Like)

Tài liệu này giải thích chi tiết các nguyên lý khoa học máy tính và kiến trúc phần cứng đằng sau quyết định thiết kế: **chỉ hỗ trợ So sánh chính xác (`==`, `!=`, `IN`, `NOT IN`)** và **loại bỏ các phép toán Pattern Matching (`LIKE`, `RLIKE`, `REGEX`)** trên kiểu dữ liệu `STRING` trong hệ thống Stateful Streaming Core.

---

## 1. NFA Backtracking & Thảm họa ReDoS (Regular Expression Denial of Service)

### NFA (Nondeterministic Finite Automaton) là gì?
Hầu hết các engine Regex mặc định trong các ngôn ngữ lập trình (như Java `java.util.regex.Pattern`, Python `re`, PCRE) sử dụng mô hình tự động hữu hạn không đơn định (**NFA**) kết hợp với thuật toán quay lùi (**Backtracking**).

### Nguyên lý gây bùng nổ hàm mũ $O(2^N)$:
* Khi tìm kiếm mẫu (pattern), nếu gặp một đường đi không khớp, engine NFA sẽ quay lùi (backtrack) lại trạng thái trước đó và thử một đường đi nhánh khác.
* Với các mẫu phức tạp chứa ký tự đại diện lồng nhau (ví dụ: `(a+)+b`) gặp chuỗi đầu vào không khớp (ví dụ: `aaaa...aaa`), số lượng nhánh thử nghiệm quay lùi sẽ tăng bùng nổ theo hàm mũ:
  
  $$\text{Độ phức tạp Worst-Case} = O(2^N)$$
  *(với $N$ là độ dài chuỗi văn bản đầu vào)*

### Hậu quả:
Chỉ với một chuỗi ngắn khoảng 30–40 ký tự độc hại hoặc không may mắn, NFA engine có thể tốn hàng triệu chu kỳ CPU và chạy liên tục trong nhiều phút/giờ để duyệt hết cây quay lùi. Hiện tượng này gọi là **ReDoS**, có thể làm treo cứng (hang/stall) worker thread và làm liệt toàn bộ hệ thống streaming thời gian thực.
*(Tham khảo bài phân tích kinh điển của Russ Cox: [Regular Expression Matching Can Be Simple And Fast](https://swtch.com/~rsc/regexp/regexp1.html)).*

---

## 2. DFA Automaton, Pointer Chasing và L1/L2 Cache Misses

Một số engine Regex tiên tiến (như Google RE2, Hyperscan) khắc phục rủi ro ReDoS bằng cách chuyển đổi mẫu thành **DFA (Deterministic Finite Automaton)**, đảm bảo thời gian chạy tuyến tính $O(N)$. Tuy nhiên, trong môi trường Streaming hiệu năng cao, DFA vẫn đắt đỏ hơn rất nhiều so với So sánh chính xác do 2 nghẽn cổ chai phần cứng:

### a. Pointer Chasing (Rượt đuổi con trỏ bộ nhớ)
* Cấu trúc dữ liệu của một DFA được biểu diễn dưới dạng Ma trận/Bảng chuyển trạng thái (**State Transition Table**):
  $$\text{next\_state} = \text{transition\_table}[\text{current\_state}][\text{input\_char}]$$
* **Cơ chế nghẽn cổ chai:** Địa chỉ ô nhớ của trạng thái tiếp theo phụ thuộc hoàn toàn vào giá trị của ký tự đầu vào ngẫu nhiên.
* CPU không thể thực hiện cơ chế nạp trước dữ liệu (**Hardware Data Prefetching**) vì không thể đoán trước con trỏ tiếp theo sẽ nhảy về đâu. CPU buộc phải đứng chờ (**CPU Stall**) để giải mã xong ký tự hiện tại mới lấy được địa chỉ con trỏ tiếp theo $\rightarrow$ Đây gọi là hiện tượng **Pointer Chasing**.

### b. Trượt Bộ Nhớ Đệm (L1/L2 Cache Misses)
* Bảng chuyển trạng thái của một DFA thường khá lớn (gồm hàng trăm trạng thái $\times 256$ ký tự ASCII/Unicode) nằm rải rác trên RAM.
* Khi luồng văn bản đầu vào thay đổi liên tục, con trỏ phải thực hiện các cú nhảy (**Memory Jumps**) tới các vùng nhớ không liên tục (**Non-sequential / Random Memory Access**).
* Việc truy xuất bộ nhớ ngẫu nhiên này vi phạm nghiêm trọng **Nguyên lý Cục bộ Không gian (Spatial Locality)** của CPU Cache. CPU liên tục bị trượt bộ nhớ đệm (**L1/L2 Cache Misses**), buộc phải nạp lại dữ liệu trực tiếp từ bộ nhớ RAM với chi phí độ trễ đắt đỏ (hàng trăm chu kỳ xung nhịp / Clock Cycles cho mỗi ký tự).

---

## 3. Tối Ưu Hóa Phần Cứng Cho Exact Matching (`String.equals` / SIMD)

Ngược lại với Regex, phép so sánh chuỗi chính xác (`==` hoặc `String.equals`) tận dụng tối đa các tiến bộ về kiến trúc vi xử lý hiện đại (Modern CPU Architecture).

### a. Cấu trúc bộ nhớ tuần tự (Sequential Memory Layout)
* Trong Java (từ Java 9+), chuỗi được lưu dưới dạng mảng byte nén (`byte[] value` - Compact Strings).
* Chuỗi dữ liệu nằm trên các ô nhớ liên tục (**Contiguous Sequential Memory**). CPU tự động kích hoạt bộ nạp trước phần cứng (**Hardware Prefetcher**) để nạp sẵn khối dữ liệu chuỗi vào L1/L2 Cache với tỷ lệ trúng đệm (**Cache Hit**) xấp xỉ 100%.

### b. Tốc độ dừng sớm $\Omega(1)$ và thuật toán $O(L)$
* Phép so sánh `String.equals` luôn kiểm tra tham chiếu địa chỉ (`this == obj`) và so sánh độ dài chuỗi trước (`length1 == length2`).
* Nếu hai chuỗi khác độ dài, thuật toán dừng ngay lập tức tại độ phức tạp $\Omega(1)$ mà không cần đọc bất kỳ ký tự nào.
* Nếu cùng độ dài, thuật toán gọi gián tiếp tới phương thức nội suy của JVM `Arrays.equals` ([tham khảo JVM Array Optimization](https://stackoverflow.com/questions/41153992/why-is-arrays-equalschar-char-8-times-faster-than-all-the-other-versions)).

### c. Song song hóa cấp phần cứng qua lệnh SIMD (Single Instruction, Multiple Data)
* JVM biên dịch `Arrays.equals` thành các chỉ thị SIMD ở cấp độ máy (như **AVX2**, **AVX-512** trên x86_64, hoặc **NEON** trên ARM).
* Thay vì so sánh từng ký tự một ($1 \text{ byte}/\text{cycle}$), thanh ghi SIMD Vector có thể nạp và so sánh song song **32 bytes (AVX2)** hoặc **64 bytes (AVX-512)** cùng lúc chỉ trong đúng **1 chu kỳ xung nhịp (Clock Cycle)**.
* Kết quả: So sánh chuỗi bằng SIMD nhanh hơn engine DFA Regex từ **10 đến 20 lần**, và nhanh hơn NFA ReDoS hàng **triệu lần**.

---

## 4. Tác Động Nhân Chi Phí Trong Hệ Thống Streaming (Workload Amplification)

Trong một hệ thống xử lý luồng dữ liệu thời gian thực (Stateful Streaming Core), chi phí tính toán không chỉ diễn ra 1 lần mà bị nhân lên theo cấp số nhân:

$$\text{Tổng chi phí xử lý} = \text{Số lượng Event} \times \text{Số lượng Pattern/Điều kiện kiểm tra}$$

1. **Bước 1 - Trigger Checking (`trigger_criteria`):** Mỗi event mới đi vào hệ thống đều phải chạy qua danh sách các điều kiện trigger để kiểm tra xem có cần kích hoạt đánh giá rule hay không.
2. **Bước 2 - State & Window Filtering (`condition_tree`):** Nếu rule có điều kiện cửa sổ thời gian (`window`), mỗi event hợp lệ tiếp tục được duyệt qua bộ lọc (`filter`) để cập nhật trạng thái tích lũy (Stateful Aggregation).

Nếu sử dụng toán tử Regex/Like, chi phí xử lý đắt đỏ của Regex (với NFA/DFA, Pointer Chasing và Cache Misses) sẽ bị **nhân lên trên từng event** của luồng streaming triệu message/giây, gây nghẽn cổ chai toàn bộ đường ống dữ liệu. Việc bắt buộc sử dụng **Exact Matching** đảm bảo throughput của hệ thống luôn duy trì ở mức tối đa với độ trễ tối thiểu (Sub-millisecond Latency).
