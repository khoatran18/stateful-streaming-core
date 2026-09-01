# Quy định về kiểu dữ liệu, toán tử, các phép tổng hợp 

## 1. Danh sách kiểu dữ liệu và toán tử hỗ trợ
* **`INT` / `LONG`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN`, `+`, `-`, `*`, `/`, `%`
* **`FLOAT` / `DOUBLE`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `+`, `-`, `*`, `/`
* **`STRING`**: `==`, `!=`, `IN`, `NOT IN`
    * **Lưu ý thiết kế (Không hỗ trợ `LIKE`, `RLIKE`, `REGEX`):**
        * *Tránh ReDoS (NFA):* Các engine Regex mặc định (như Java `Pattern`) dùng NFA Backtracking có độ phức tạp Worst-Case lên tới $O(2^N)$ ([tham khảo phân tích NFA Complexity](https://swtch.com/~rsc/regexp/regexp1.html)), dễ gây treo CPU.
        * *Hạn chế của DFA:* Dù dùng engine DFA (Google RE2, Hyperscan) đạt $O(N)$ với $N$ là độ dài chuỗi, CPU vẫn tốn chi phí do liên tục tra cứu bảng chuyển trạng thái (State Transition Table Lookup), gây Pointer Chasing và trượt L1/L2 Cache Misses liên tục trên từng ký tự.
        * *So sánh `==` (`String.equals`):* Đạt độ phức tạp $O(L)$ với $L = \min(\text{length}_1, \text{length}_2)$ (dừng sớm $\Omega(1)$ nếu khác độ dài/hash). Do cấu trúc chuỗi Java dùng mảng `byte[]`, `String.equals` gọi gián tiếp tới `Arrays.equals` của JVM ([tham khảo JVM Array Optimization](https://stackoverflow.com/questions/41153992/why-is-arrays-equalschar-char-8-times-faster-than-all-the-other-versions)), tận dụng SIMD của CPU để so sánh song song với truy xuất bộ nhớ tuần tự.
        * *Độ phức tạp nhân theo Event và Pattern (Workload Amplification):* Với mỗi event đến, hệ thống phải thực hiện kiểm tra liên tục trên từng pattern/điều kiện (ở bước `trigger_criteria` và lặp lại khi lọc dữ liệu cửa sổ `window` trong `condition_tree`). Tổng chi phí tính toán bị nhân lên theo công thức: $\text{Số lượng Event} \times \text{Số lượng Pattern cần kiểm tra}$. Nếu dùng Regex/Like, việc nhân chi phí đắt đỏ này trên luồng streaming quy mô lớn sẽ gây quá tải hệ thống.
* **`BOOLEAN`**: `==`, `!=`
* **`TIMESTAMP`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`
* **`OBJECT`**: Cấu trúc lồng nhau (Nested Objects), truy xuất dữ liệu đa cấp qua cú pháp Dot Notation (ví dụ: `risk_signals.fraud_probability_score`).

---

## 2. Phép toán tổng hợp cửa sổ
Hỗ trợ 5 hàm tổng hợp cửa sổ: `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`.

* **Các loại cửa sổ hỗ trợ (`window`):**
    * **`sliding window`:** Cấu hình bao gồm khoảng thời gian hiệu lực `duration` và chu kỳ trượt `slide` (ví dụ: `"duration": "20m"`, `"slide": "5m"`).
    * **`tumbling window`:** Cấu hình chỉ bao gồm khoảng thời gian hiệu lực `duration` (ví dụ: `"duration": "1h"`).
    * **Lưu ý:** `duration` và `slide` bắt buộc phải chia hết cho độ dài của một bucket trong kiến trúc Ring Buffer của hệ thống (thời gian bucket cụ thể sẽ được cấu hình sau).
    * **Trường thời gian:** Khi sử dụng `window`, hệ thống mặc định sử dụng trường `event_time` trong phần `metadata` của bản tin event để xác định mốc thời gian tính toán cửa sổ.
* **Bộ lọc kích hoạt (`trigger_criteria`):**
    * Cấu trúc dạng mảng: `[{ "source": "...", "version": "...", "conditions": [ [cond1, cond2], [cond3] ] }]`.
    * **Đa nguồn (Multi-source):** Khai báo `source` và `version` riêng giúp định tuyến và lọc dữ liệu nhanh theo từng loại event.
    * **Cấu trúc `conditions` (Mảng 2 chiều):** Là danh sách các list điều kiện. Chỉ cần thỏa mãn **toàn bộ phần tử trong 1 list điều kiện** là pass trigger.
* **Quy định tính toán:**
    * Các hàm `SUM`, `AVG`, `MAX`, `MIN` chỉ áp dụng tính toán cho trường kiểu `INT`, `LONG`, `FLOAT`, `DOUBLE` hoặc biểu thức tuyến tính (`Expr`).
    * Hàm `COUNT` dùng để đếm tần suất xuất hiện bản ghi thỏa mãn điều kiện.
* **Bộ lọc điều kiện (`filter`):** **Bắt buộc** — mọi biểu thức cửa sổ đều phải kèm điều kiện lọc trực tiếp để tính toán có chọn lọc trên dữ liệu quá khứ (ví dụ: chỉ tính trung bình hoặc đếm khi giao dịch được thực hiện qua kênh cụ thể). `filter` là một object độc lập ngang hàng với `field`/`agg`, **không** nằm bên trong `window`.

#### Cấu trúc JSON Rule hoàn chỉnh (kèm `filter` trong Window):
```text
{
  "rule_id": "rule_B_54",
  "schema_fields_count": 36,
  "metadata": {
    "event_time": "2026-08-24T16:02:37.123+07:00",
    "user_id" : "user_011"
  },
  "trigger_criteria": [
    {
      "source": "B",
      "version": "v2",
      "conditions": [
        [                                                           // Bộ điều kiện 1 (AND)
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
        ],
        [                                                           // Bộ điều kiện 2 (OR với bộ 1)
          {
            "field": "is_vip",
            "op": "==",
            "value": true
          }
        ]
      ]
    },
    {
      "source": "A",
      "version": "v1",
      "conditions": [
        [
          {
            "field": "user_status",
            "op": "==",
            "value": "ACTIVE"
          }
        ]
      ]
    }
  ],
  "condition_tree": {                               // Logic chính của rule 
    "type": "OR",
    "children": [
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.risk_signals.fraud_probability_score",     // Cửa sổ trượt (Sliding Window)
          "agg": "avg",
          "filter": {
            "field": "device_type",
            "op": "==",
            "value": "TABLET"
          },
          "window": {
            "type": "sliding",
            "duration": "20m",
            "slide": "5m"
          },
          "op": "<=",
          "threshold": 40.13
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "field": "A.v2.daily_spend_total_vnd",                    // Cửa sổ nhảy (Tumbling Window)
          "agg": "sum",
          "filter": {
            "field": "transaction_type",
            "op": "NOT IN",
            "value": ["REFUND", "REVERSAL"]
          },
          "window": {
            "type": "tumbling",
            "duration": "1h"
          },
          "op": ">",
          "threshold": 10000000.0
        }
      }
    ]
  }
}
```
## 3. Quy tắc toán tử:
* **Vế trái:** Luôn là một trường dữ liệu `Field`, hoặc biểu thức tuyến tính `Expr` (chỉ áp dụng đối với `INT`, `LONG`, `FLOAT` hoặc `DOUBLE`).
* **Vế phải:** Có thể là giá trị cụ thể (Literal), danh sách giá trị (`List`), hoặc trường dữ liệu khác (`Field`), biểu thức tuyến tính `Expr` (với `INT`, `LONG`, `FLOAT` hoặc `DOUBLE`).
* **Quy định bất biến cho `IN`, `NOT IN`, `BETWEEN`:** Vế phải bắt buộc là danh sách/mảng các **giá trị cụ thể (Literal values)**, không chứa `Field` bên trong để tránh phức tạp hóa cây điều kiện.

---

### Bảng chi tiết toán tử, toán hạng và ví dụ mẫu

> **Chú thích bảng:**
> * `Expr`: Biểu thức số học tuyến tính kết hợp giữa các trường và hằng số (ví dụ: `pending * 0.7 + spend * 0.3`).
> * `Field`: Tên một trường trong bản tin dữ liệu.
> * Trong ngoặc đơn `(...)`: Kiểu dữ liệu tương ứng của trường đó.




| Data Type | Operators | Vế trái | Vế phải                                   | Ghi chú ngữ nghĩa | VD: Vế phải Literal Value                                                                                                                                    | VD: Vế phải Cross-Field                                                                                                        | VD: Chứa Expr                                                                                                                          |
| :--- | :--- | :--- |:------------------------------------------| :--- |:-------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| **`INT / LONG`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Int/Long)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | So sánh đơn trị | <pre>{<br>  "field": "age",<br>  "op": ">=",<br>  "value": 18<br>}</pre>                                                                                     | <pre>{<br>  "field": "login_attempts_count",<br>  "op": ">",<br>  "right_field": "app_session_count_today"<br>}</pre>          | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": ">=",<br>  "value": 5<br>}</pre>                                      |
| | `BETWEEN` | `Field (Int/Long)` / `Expr` | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`) | <pre>{<br>  "field": "login_attempts_count",<br>  "op": "BETWEEN",<br>  "value": [1.5, 8.5]<br>}</pre>                                                       |                                                                                                                                | <pre>{<br>  "expr": "age + tenure_months / 12",<br>  "op": "BETWEEN",<br>  "value": [20, 60]<br>}</pre>                                |
| | `IN`, `NOT IN` | `Field (Int/Long)` / `Expr` | `List<Int/Long>`                               | Tra cứu tập hợp ($O(1)$ Hash Set) | <pre>{<br>  "field": "mcc_code",<br>  "op": "IN",<br>  "value": [5411, 5812, 5999]<br>}</pre>                                                                |                                                                                                                                | <pre>{<br>  "expr": "number_of_products_held % 10",<br>  "op": "IN",<br>  "value": [1, 3, 5]<br>}</pre>                                |
| | `+`, `-`, `*`, `/`, `%` | `Field (Int/Long)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Int/Long/Float/Double` | <pre>{<br>  "expr": "number_of_products_held % 2",<br>  "op": "==",<br>  "value": 0<br>}</pre>                                                               | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": "<",<br>  "right_field": "app_session_count_today"<br>}</pre> | <pre>{<br>  "expr": "total_loan_count + total_card_count",<br>  "op": ">",<br>  "right_expr": "number_of_products_held * 2"<br>}</pre> |
| **`FLOAT / DOUBLE`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Float/Double)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | So sánh ngưỡng số thực | <pre>{<br>  "field": "fraud_probability_score",<br>  "op": ">",<br>  "value": 70.0<br>}</pre>                                                                | <pre>{<br>  "field": "last_transaction_amount_vnd",<br>  "op": ">",<br>  "right_field": "monthly_income_vnd"<br>}</pre>        | <pre>{<br>  "expr": "(pending * 0.7 + spend * 0.3)",<br>  "op": ">=",<br>  "value": 1000000.0<br>}</pre>                               |
| | `BETWEEN` | `Field (Float/Double)` / `Expr` | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`) | <pre>{<br>  "field": "response_latency_ms",<br>  "op": "BETWEEN",<br>  "value": [100.0, 500.0]<br>}</pre>                                                    |                                                                                                                                | <pre>{<br>  "expr": "(latency_ms + idle_time * 1000) / 2",<br>  "op": "BETWEEN",<br>  "value": [500.0, 3000.0]<br>}</pre>              |
| | `+`, `-`, `*`, `/` | `Field (Float/Double)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Float/Double` | <pre>{<br>  "expr": "last_transaction_amount / monthly_income",<br>  "op": ">",<br>  "value": 0.8<br>}</pre>                                                 | <pre>{<br>  "expr": "current_balance - last_txn_amount",<br>  "op": "<",<br>  "right_field": "credit_limit_vnd"<br>}</pre>     | <pre>{<br>  "expr": "pending_amount * 0.7",<br>  "op": ">=",<br>  "right_expr": "daily_spend_total * 0.3"<br>}</pre>                   |
| **`STRING`** | `==`, `!=` | `Field (String)` | `Field (String)` / `String`               | Khớp chuỗi chính xác (hỗ trợ so sánh chéo giữa các trường) | <pre>{<br>  "field": "login_channel",<br>  "op": "==",<br>  "value": "MOBILE_APP"<br>}</pre>                                                                 | <pre>{<br>  "field": "home_province",<br>  "op": "!=",<br>  "right_field": "current_province"<br>}</pre>                       |                                                                                                                                        |
| | `IN`, `NOT IN` | `Field (String)` | `List<String>`                            | Tra cứu tập chuỗi ($O(1)$ Hash Set) | <pre>{<br>  "field": "login_channel",<br>  "op": "IN",<br>  "value": [<br>    "MOBILE_APP",<br>    "WEB"<br>  ]<br>}</pre>                                   |                                                                                                                                |                                                                                                                                        |
| **`BOOLEAN`** | `==`, `!=` | `Field (Boolean)` | `Field (Boolean)` / `Boolean`             | Kiểm tra hoặc đối chiếu cờ trạng thái | <pre>{<br>  "field": "is_suspicious_ip",<br>  "op": "==",<br>  "value": true<br>}</pre>                                                                      | <pre>{<br>  "field": "is_2fa_enabled",<br>  "op": "==",<br>  "right_field": "is_biometric_enabled"<br>}</pre>                  |                                                                                                                                        |
| **`TIMESTAMP`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Timestamp)` | `Field (Timestamp)` / `String (ISO-8601)` | So sánh mốc thời gian (hỗ trợ so sánh giữa 2 trường thời gian) | <pre>{<br>  "field": "account_created_date",<br>  "op": ">",<br>  "value": "2026-01-01T00:00:00Z"<br>}</pre>                                                 | <pre>{<br>  "field": "event_time",<br>  "op": ">",<br>  "right_field": "last_login_time"<br>}</pre>                            |                                                                                                                                        |
| | `BETWEEN` | `Field (Timestamp)` | `[from, to]` (`Timestamp[2]`)             | Khoảng thời gian (`>= from && <= to`) | <pre>{<br>  "field": "last_login_time",<br>  "op": "BETWEEN",<br>  "value": [<br>    "2026-08-01T00:00:00Z",<br>    "2026-08-24T23:59:59Z"<br>  ]<br>}</pre> |                                                                                                                                |                                                                                                                                        |