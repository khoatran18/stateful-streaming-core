# Quy định về kiểu dữ liệu, toán tử, các phép tổng hợp 

## 1. Danh sách kiểu dữ liệu và toán tử hỗ trợ
* **`INT`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN`, `+`, `-`, `*`, `/`, `%`
* **`FLOAT`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `+`, `-`, `*`, `/`
* **`STRING`**: `==`, `!=`, `IN`, `NOT IN`
* **`BOOLEAN`**: `==`, `!=`
* **`TIMESTAMP`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`
* **`OBJECT`**: Cấu trúc lồng nhau (Nested Objects), truy xuất dữ liệu đa cấp qua cú pháp Dot Notation (ví dụ: `risk_signals.fraud_probability_score`).

---

## 2. Phép toán tổng hợp cửa sổ
Hỗ trợ 5 hàm tổng hợp cửa sổ: `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`.

* **Bộ lọc kích hoạt kiểm tra rule (`trigger_criteria`):**
    * Chỉ áp dụng cho event đến, check xem có thỏa mãn điều kiện để kích hoạt đánh giá rule không.
    * Tham gia xây dựng chỉ mục ngược. 
* **Quy định tính toán:**
    * Các hàm `SUM`, `AVG`, `MAX`, `MIN` chỉ áp dụng tính toán cho trường kiểu `INT`, `FLOAT` hoặc biểu thức tuyến tính (`Expr`).
    * Hàm `COUNT` dùng để đếm tần suất xuất hiện bản ghi thỏa mãn điều kiện.
* **Bộ lọc điều kiện (`filter`):** Cho phép gắn trực tiếp điều kiện lọc vào bên trong cửa sổ để tính toán có chọn lọc trên dữ liệu quá khứ (ví dụ: chỉ tính trung bình hoặc đếm khi giao dịch được thực hiện qua kênh cụ thể).

#### Cấu trúc JSON Rule hoàn chỉnh (kèm `filter` trong Window):
```text
{
  "rule_id": "rule_B_54",
  "schema_fields_count": 34,
  "metadata": {
    "event_time": "2026-08-24T16:02:37+07:00",
    "user_id": "user_001"
  },
  "trigger_criteria": {                             // Điều kiện trigger rule
    "source": "B",
    "version": "v2",
    "conditions": [
      {
        "field": "nps_score_baseline",
        "op": "IN",
        "value": [7, 3]
      },
      {
        "field": "device_type",
        "op": "==",
        "value": "TABLET"
      },
      {
        "field": "financial_literacy_score",
        "op": "<",
        "value": 26
      }
    ]
  },
  "condition_tree": {                               // Logic chính của rule 
    "type": "OR",
    "children": [
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.risk_signals.fraud_probability_score",
          "agg": "avg",
          "filter": {
            "field": "device_type",
            "op": "==",
            "value": "TABLET"
          },
          "window": {
            "type": "sliding",
            "time": "20m"
          },
          "op": "<=",
          "threshold": 40.13
        }
      }
    ]
  }
}
```
## 3. Quy tắc toán tử:
* **Vế trái:** Luôn là một trường dữ liệu `Field`, hoặc biểu thức tuyến tính `Expr` (chỉ áp dụng đối với `INT` hoặc `FLOAT`).
* **Vế phải:** Có thể là giá trị cụ thể (Literal), danh sách giá trị (`List`), hoặc trường dữ liệu khác (`Field`), biểu thức tuyến tính `Expr` (với `INT` hoặc `FLOAT`).
* **Quy định bất biến cho `IN`, `NOT IN`, `BETWEEN`:** Vế phải bắt buộc là danh sách/mảng các **giá trị cụ thể (Literal values)**, không chứa `Field` bên trong để tránh phức tạp hóa cây điều kiện.

---

### Bảng chi tiết Toán tử, Đầu vào và Ví dụ mẫu

> **Chú thích bảng:**
> * `Expr`: Biểu thức số học tuyến tính kết hợp giữa các trường và hằng số (ví dụ: `pending * 0.7 + spend * 0.3`).
> * `Field`: Tên một trường trong bản tin dữ liệu.
> * Trong ngoặc đơn `(...)`: Kiểu dữ liệu tương ứng của trường đó.




| Data Type | Operators | Vế trái | Vế phải                                   | Ghi chú ngữ nghĩa | VD: Vế phải Literal Value                                                                                                                                    | VD: Vế phải Cross-Field                                                                                                        | VD: Chứa Expr                                                                                                                          |
| :--- | :--- | :--- |:------------------------------------------| :--- |:-------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| **`INT`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Int)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | So sánh đơn trị | <pre>{<br>  "field": "age",<br>  "op": ">=",<br>  "value": 18<br>}</pre>                                                                                     | <pre>{<br>  "field": "login_attempts_count",<br>  "op": ">",<br>  "right_field": "app_session_count_today"<br>}</pre>          | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": ">=",<br>  "value": 5<br>}</pre>                                      |
| | `BETWEEN` | `Field (Int)` / `Expr` | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`) | <pre>{<br>  "field": "login_attempts_count",<br>  "op": "BETWEEN",<br>  "value": [1.5, 8.5]<br>}</pre>                                                       |                                                                                                                                | <pre>{<br>  "expr": "age + tenure_months / 12",<br>  "op": "BETWEEN",<br>  "value": [20, 60]<br>}</pre>                                |
| | `IN`, `NOT IN` | `Field (Int)` / `Expr` | `List<Int>`                               | Tra cứu tập hợp ($O(1)$ Hash Set) | <pre>{<br>  "field": "mcc_code",<br>  "op": "IN",<br>  "value": [5411, 5812, 5999]<br>}</pre>                                                                |                                                                                                                                | <pre>{<br>  "expr": "number_of_products_held % 10",<br>  "op": "IN",<br>  "value": [1, 3, 5]<br>}</pre>                                |
| | `+`, `-`, `*`, `/`, `%` | `Field (Int)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Int/Float` | <pre>{<br>  "expr": "number_of_products_held % 2",<br>  "op": "==",<br>  "value": 0<br>}</pre>                                                               | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": "<",<br>  "right_field": "app_session_count_today"<br>}</pre> | <pre>{<br>  "expr": "total_loan_count + total_card_count",<br>  "op": ">",<br>  "right_expr": "number_of_products_held * 2"<br>}</pre> |
| **`FLOAT`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Float)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | So sánh ngưỡng số thực | <pre>{<br>  "field": "fraud_probability_score",<br>  "op": ">",<br>  "value": 70.0<br>}</pre>                                                                | <pre>{<br>  "field": "last_transaction_amount_vnd",<br>  "op": ">",<br>  "right_field": "monthly_income_vnd"<br>}</pre>        | <pre>{<br>  "expr": "(pending * 0.7 + spend * 0.3)",<br>  "op": ">=",<br>  "value": 1000000.0<br>}</pre>                               |
| | `BETWEEN` | `Field (Float)` / `Expr` | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`) | <pre>{<br>  "field": "response_latency_ms",<br>  "op": "BETWEEN",<br>  "value": [100.0, 500.0]<br>}</pre>                                                    |                                                                                                                                | <pre>{<br>  "expr": "(latency_ms + idle_time * 1000) / 2",<br>  "op": "BETWEEN",<br>  "value": [500.0, 3000.0]<br>}</pre>              |
| | `+`, `-`, `*`, `/` | `Field (Float)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Float` | <pre>{<br>  "expr": "last_transaction_amount / monthly_income",<br>  "op": ">",<br>  "value": 0.8<br>}</pre>                                                 | <pre>{<br>  "expr": "current_balance - last_txn_amount",<br>  "op": "<",<br>  "right_field": "credit_limit_vnd"<br>}</pre>     | <pre>{<br>  "expr": "pending_amount * 0.7",<br>  "op": ">=",<br>  "right_expr": "daily_spend_total * 0.3"<br>}</pre>                   |
| **`STRING`** | `==`, `!=` | `Field (String)` | `Field (String)` / `String`               | Khớp chuỗi chính xác (hỗ trợ so sánh chéo giữa các trường) | <pre>{<br>  "field": "login_channel",<br>  "op": "==",<br>  "value": "MOBILE_APP"<br>}</pre>                                                                 | <pre>{<br>  "field": "home_province",<br>  "op": "!=",<br>  "right_field": "current_province"<br>}</pre>                       |                                                                                                                                        |
| | `IN`, `NOT IN` | `Field (String)` | `List<String>`                            | Tra cứu tập chuỗi ($O(1)$ Hash Set) | <pre>{<br>  "field": "login_channel",<br>  "op": "IN",<br>  "value": [<br>    "MOBILE_APP",<br>    "WEB"<br>  ]<br>}</pre>                                   |                                                                                                                                |                                                                                                                                        |
| **`BOOLEAN`** | `==`, `!=` | `Field (Boolean)` | `Field (Boolean)` / `Boolean`             | Kiểm tra hoặc đối chiếu cờ trạng thái | <pre>{<br>  "field": "is_suspicious_ip",<br>  "op": "==",<br>  "value": true<br>}</pre>                                                                      | <pre>{<br>  "field": "is_2fa_enabled",<br>  "op": "==",<br>  "right_field": "is_biometric_enabled"<br>}</pre>                  |                                                                                                                                        |
| **`TIMESTAMP`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Timestamp)` | `Field (Timestamp)` / `String (ISO-8601)` | So sánh mốc thời gian (hỗ trợ so sánh giữa 2 trường thời gian) | <pre>{<br>  "field": "account_created_date",<br>  "op": ">",<br>  "value": "2026-01-01T00:00:00Z"<br>}</pre>                                                 | <pre>{<br>  "field": "event_time",<br>  "op": ">",<br>  "right_field": "last_login_time"<br>}</pre>                            |                                                                                                                                        |
| | `BETWEEN` | `Field (Timestamp)` | `[from, to]` (`Timestamp[2]`)             | Khoảng thời gian (`>= from && <= to`) | <pre>{<br>  "field": "last_login_time",<br>  "op": "BETWEEN",<br>  "value": [<br>    "2026-08-01T00:00:00Z",<br>    "2026-08-24T23:59:59Z"<br>  ]<br>}</pre> |                                                                                                                                |                                                                                                                                        |