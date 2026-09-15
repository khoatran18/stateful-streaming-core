# Cấu trúc Dual Schema (Schema A & Schema B) — 36 Leaf Fields

Tài liệu chi tiết về thông số kỹ thuật, phân loại trường (`category`), kiểu dữ liệu (`type`), cấu trúc lồng (`nested group`) và danh sách các trường (36 leaf fields) cho 2 loại sự kiện CDP (Schema A và Schema B).

---

## 1. Tổng quan thiết kế Dual Schema

Mỗi sự kiện CDP trong hệ thống được định nghĩa theo một trong hai loại schema với cùng tổng số **36 trường (leaf fields)**:
* **Schema A (Transaction Events):** Dữ liệu giao dịch tài chính, chuyển tiền, thanh toán và thông tin tài khoản ngân hàng.
* **Schema B (System Access Logs):** Dữ liệu nhật ký truy cập hệ thống, thiết bị, mạng và tín hiệu rủi ro an ninh.

Ngoại trừ 4 trường `metadata` chung (`customer_id`, `schema_version`, `source`, `event_time`), **32 trường nghiệp vụ còn lại của Schema A và Schema B hoàn toàn độc lập**.

### Bảng tổng hợp số lượng trường theo Category

| STT | Loại trường (`category`) | Số lượng (Schema A) | Số lượng (Schema B) | Ghi chú & Mô tả |
| :---: | :--- | :---: | :---: | :--- |
| 1 | **`static_categorical`** | **8** | **8** | 3 trường metadata + 5 trường thuộc tính tĩnh định danh khách hàng |
| 2 | **`static_numeric`** | **9** | **9** | Chỉ số tĩnh cố định theo khách hàng (tuổi, điểm tín dụng, thu nhập...) |
| 3 | **`dynamic_categorical`** | **9** | **9** | 1 trường metadata (`event_time`) + 8 trạng thái biến đổi theo event |
| 4 | **`dynamic_numeric`** | **14** | **14** | Chỉ số thời gian thực (`INT`, `FLOAT`, `LONG`, `DOUBLE`) |
| **TỔNG** | **Tổng số Leaf Fields** | **36** | **36** | **Tỉ lệ phân bổ: 8 / 9 / 9 / 14** |

---

## 2. Chi tiết Schema A — Transaction Events (36 Leaf Fields)

### 2.1 Metadata Block (4 fields)
* `metadata.customer_id` (`STRING`, `static_categorical`): Định danh khách hàng (Partition key).
* `metadata.schema_version` (`STRING`, `static_categorical`): Phiên bản schema (`v2`).
* `metadata.source` (`STRING`, `static_categorical`): Nguồn sự kiện (`A`).
* `metadata.event_time` (`TIMESTAMP`, `dynamic_categorical`): Thời gian phát sinh sự kiện.

### 2.2 Static Fields (14 fields)
* **Categorical & Profile (5 fields):**
  * `customer_segment` (`STRING`, `static_categorical`): Phân khúc khách hàng (`PREMIUM`, `STANDARD`, `BASIC`, `VIP`, `ENTERPRISE`).
  * `loyalty_tier` (`STRING`, `static_categorical`): Hạng hội viên (`BRONZE`, `SILVER`, `GOLD`, `PLATINUM`, `DIAMOND`).
  * `risk_rating` (`STRING`, `static_categorical`): Mức độ rủi ro (`LOW`, `MEDIUM`, `HIGH`, `VERY_HIGH`).
  * `account_opened_date` (`TIMESTAMP`, `static_categorical`): Ngày mở tài khoản.
  * `is_vip_member` (`BOOLEAN`, `static_categorical`): Cờ xác nhận VIP.
* **Numeric (9 fields):**
  * `age` (`INT`, `static_numeric`): Tuổi (18–100).
  * `base_credit_score` (`INT`, `static_numeric`): Điểm tín dụng gốc (300–850).
  * `tenure_months` (`INT`, `static_numeric`): Thâm niên khách hàng (tháng).
  * `number_of_products_held` (`INT`, `static_numeric`): Số sản phẩm đang sở hữu.
  * `total_loan_count` (`INT`, `static_numeric`): Tổng số khoản vay.
  * `total_card_count` (`INT`, `static_numeric`): Tổng số thẻ sở hữu.
  * `credit_limit_vnd` (`FLOAT`, `static_numeric`): Hạn mức tín dụng (VND).
  * `monthly_income_vnd` (`FLOAT`, `static_numeric`): Thu nhập hàng tháng (VND).
  * `debt_to_income_ratio` (`FLOAT`, `static_numeric`): Tỉ lệ nợ trên thu nhập.

### 2.3 Dynamic Fields (18 fields)
* **Categorical (7 fields ở Root):**
  * `transaction_type` (`STRING`, `dynamic_categorical`): Loại giao dịch (`TRANSFER`, `PAYMENT`, `WITHDRAWAL`, `DEPOSIT`, `TOP_UP`, `REFUND`).
  * `last_transaction_status` (`STRING`, `dynamic_categorical`): Trạng thái giao dịch (`SUCCESS`, `FAILED`, `PENDING`, `REVERSED`, `CANCELLED`).
  * `card_status` (`STRING`, `dynamic_categorical`): Trạng thái thẻ (`ACTIVE`, `BLOCKED`, `EXPIRED`, `LOST`, `STOLEN`).
  * `active_product_category` (`STRING`, `dynamic_categorical`): Phân loại sản phẩm hoạt động.
  * `transaction_currency` (`STRING`, `dynamic_categorical`): Loại tiền tệ (`VND`, `USD`, `EUR`, `KRW`, `JPY`, `SGD`, `AUD`).
  * `last_transaction_time` (`TIMESTAMP`, `dynamic_categorical`): Thời gian giao dịch gần nhất.
  * `is_international_transaction` (`BOOLEAN`, `dynamic_categorical`): Cờ giao dịch quốc tế.
* **Numeric (11 fields ở Root):**
  * `current_balance_vnd` (`FLOAT`, `dynamic_numeric`): Số dư hiện tại (VND).
  * `last_transaction_amount_vnd` (`FLOAT`, `dynamic_numeric`): Số tiền giao dịch gần nhất (VND).
  * `daily_spend_total_vnd` (`FLOAT`, `dynamic_numeric`): Tổng chi tiêu trong ngày (VND).
  * `pending_transaction_amount_vnd` (`FLOAT`, `dynamic_numeric`): Số tiền giao dịch chờ xử lý (VND).
  * `current_credit_card_balance_vnd` (`FLOAT`, `dynamic_numeric`): Dư nợ thẻ tín dụng (VND).
  * `atm_withdrawal_amount_today_vnd` (`FLOAT`, `dynamic_numeric`): Số tiền rút ATM trong ngày (VND).
  * `online_transfers_today` (`INT`, `dynamic_numeric`): Số lượt chuyển khoản online trong ngày.
  * `failed_transactions_count_today` (`INT`, `dynamic_numeric`): Số giao dịch thất bại trong ngày.
  * `successful_transactions_count_today` (`INT`, `dynamic_numeric`): Số giao dịch thành công trong ngày.
  * `total_transaction_count_lifetime` (`LONG`, `dynamic_numeric`): Tổng số giao dịch tích lũy.
  * `average_transaction_amount_vnd` (`DOUBLE`, `dynamic_numeric`): Giá trị giao dịch trung bình (VND).

### 2.4 Nested Group: `debt` (4 fields)
Nhóm cấu trúc lồng `event.debt.{fieldName}`:
* `debt.loan_repayment_status` (`STRING`, `dynamic_categorical`): Trạng thái trả nợ (`ON_TIME`, `OVERDUE_1_30`, `OVERDUE_31_90`, `OVERDUE_90_PLUS`, `NO_LOAN`).
* `debt.transfer_amount_today_vnd` (`FLOAT`, `dynamic_numeric`): Tổng tiền chuyển khoản hôm nay (VND).
* `debt.loan_repayment_amount_this_month_vnd` (`FLOAT`, `dynamic_numeric`): Số tiền trả nợ tháng này (VND).
* `debt.total_outstanding_debt_vnd` (`FLOAT`, `dynamic_numeric`): Tổng dư nợ còn lại (VND).

---

## 3. Chi tiết Schema B — System Access Logs (36 Leaf Fields)

### 3.1 Metadata Block (4 fields)
* `metadata.customer_id` (`STRING`, `static_categorical`): Định danh khách hàng.
* `metadata.schema_version` (`STRING`, `static_categorical`): Phiên bản schema (`v2`).
* `metadata.source` (`STRING`, `static_categorical`): Nguồn sự kiện (`B`).
* `metadata.event_time` (`TIMESTAMP`, `dynamic_categorical`): Thời gian truy cập.

### 3.2 Static Fields (14 fields)
* **Categorical & Profile (5 fields):**
  * `home_province` (`STRING`, `static_categorical`): Tỉnh/Thành phố đăng ký (`HANOI`, `HCM`, `DANANG`...).
  * `preferred_language` (`STRING`, `static_categorical`): Ngôn ngữ ưu tiên (`VI`, `EN`, `ZH`...).
  * `customer_type` (`STRING`, `static_categorical`): Loại khách hàng (`INDIVIDUAL`, `CORPORATE`, `SME`).
  * `account_created_date` (`TIMESTAMP`, `static_categorical`): Ngày tạo tài khoản.
  * `is_2fa_enabled` (`BOOLEAN`, `static_categorical`): Cờ bật xác thực 2 lớp.
* **Numeric (9 fields):**
  * `digital_adoption_score` (`INT`, `static_numeric`): Điểm độ sẵn sàng kỹ thuật số (0–100).
  * `engagement_score_baseline` (`INT`, `static_numeric`): Điểm tương tác nền (0–100).
  * `behavioral_score_baseline` (`INT`, `static_numeric`): Điểm hành vi gốc (0–1000).
  * `avg_monthly_transaction_count` (`INT`, `static_numeric`): Số lượng giao dịch trung bình tháng.
  * `propensity_churn_score` (`INT`, `static_numeric`): Điểm nguy cơ rời bỏ dịch vụ.
  * `nps_score_baseline` (`INT`, `static_numeric`): Điểm hài lòng NPS nền (0–10).
  * `preferred_contact_hour` (`INT`, `static_numeric`): Khung giờ liên hệ ưu tiên (0–23).
  * `social_media_influence_score` (`INT`, `static_numeric`): Điểm ảnh hưởng mạng xã hội.
  * `financial_literacy_score` (`INT`, `static_numeric`): Điểm hiểu biết tài chính.

### 3.3 Dynamic Fields (18 fields)
* **Categorical (5 fields ở Root):**
  * `login_channel` (`STRING`, `dynamic_categorical`): Kênh đăng nhập (`MOBILE_APP`, `WEB`, `ATM`, `BRANCH`, `CALL_CENTER`).
  * `device_type` (`STRING`, `dynamic_categorical`): Loại thiết bị (`IOS`, `ANDROID`, `DESKTOP`, `TABLET`).
  * `network_type` (`STRING`, `dynamic_categorical`): Loại mạng (`WIFI`, `4G`, `5G`, `3G`, `ETHERNET`).
  * `auth_method` (`STRING`, `dynamic_categorical`): Phương thức đăng nhập (`PASSWORD`, `OTP`, `BIOMETRIC`, `PIN`, `TOKEN`).
  * `current_location_type` (`STRING`, `dynamic_categorical`): Vị trí truy cập (`HOME`, `OFFICE`, `TRAVEL`, `ABROAD`, `UNKNOWN`).
  * `last_login_time` (`TIMESTAMP`, `dynamic_categorical`): Thời gian đăng nhập gần nhất.
* **Numeric (12 fields ở Root):**
  * `session_duration_seconds` (`FLOAT`, `dynamic_numeric`): Thời lượng phiên (giây).
  * `response_latency_ms` (`FLOAT`, `dynamic_numeric`): Độ độ trễ phản hồi (ms).
  * `idle_time_seconds` (`FLOAT`, `dynamic_numeric`): Thời gian chờ không hoạt động (giây).
  * `transaction_velocity_score` (`FLOAT`, `dynamic_numeric`): Tốc độ phát sinh giao dịch.
  * `login_attempts_count` (`INT`, `dynamic_numeric`): Số lần thử đăng nhập.
  * `pages_viewed_session` (`INT`, `dynamic_numeric`): Số trang đã xem trong phiên.
  * `products_viewed_session` (`INT`, `dynamic_numeric`): Số sản phẩm đã xem.
  * `app_session_count_today` (`INT`, `dynamic_numeric`): Số phiên sử dụng app trong ngày.
  * `offers_clicked_today` (`INT`, `dynamic_numeric`): Số ưu đãi đã click trong ngày.
  * `notifications_received_today` (`INT`, `dynamic_numeric`): Số thông báo nhận hôm nay.
  * `total_login_count_lifetime` (`LONG`, `dynamic_numeric`): Tổng số lần đăng nhập tích lũy.
  * `average_session_duration_seconds` (`DOUBLE`, `dynamic_numeric`): Thời lượng phiên trung bình.

### 3.4 Nested Group: `risk_signals` (4 fields)
Nhóm cấu trúc lồng `event.risk_signals.{fieldName}`:
* `risk_signals.session_status` (`STRING`, `dynamic_categorical`): Trạng thái phiên (`ACTIVE`, `IDLE`, `EXPIRED`, `LOCKED`).
* `risk_signals.is_suspicious_ip` (`BOOLEAN`, `dynamic_categorical`): Cờ IP nghi vấn.
* `risk_signals.fraud_probability_score` (`FLOAT`, `dynamic_numeric`): Điểm xác suất gian lận.
* `risk_signals.behavioral_anomaly_score` (`FLOAT`, `dynamic_numeric`): Điểm bất thường hành vi.

---

## 4. Tham chiếu Mã nguồn

* Hằng số cấu hình schema: [`Constants.java`](../../data-generator/src/main/java/vdf/vdt/streaming/generator/common/Constants.java#L24-L25)
* Trình phát Schema: [`SchemaPublisher.java`](../../data-generator/src/main/java/vdf/vdt/streaming/generator/data_gen/SchemaPublisher.java#L83-L166)
* Trình sinh Event: [`DataGenerator.java`](../../data-generator/src/main/java/vdf/vdt/streaming/generator/data_gen/DataGenerator.java#L190-L215)
