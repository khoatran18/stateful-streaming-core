# Data Generator & Rule Generator (`data-generator`)

Generates synthetic CDP streaming events and hierarchical condition-tree rules to benchmark stateful streaming on Kafka + Flink.

---

## Package Structure

```text
generator/
├── common/
│   ├── Constants.java             # field lists: 200-field legacy + Schema A/B (36 each)
│   ├── KafkaProducerClient.java   # send() and sendWithHeader()
│   └── PathUtils.java             # output path resolution
├── data_gen/
│   ├── DataGenerator.java         # dual-schema event loop → Kafka
│   ├── SchemaPublisher.java       # publishes Schema A + B to Kafka + local files on startup
│   ├── SchemaConsumerExample.java # reference: reads schema topic, validates events
│   └── Main.java
├── model/
│   ├── FieldDefinition.java       # name, type, category (constraint fields internal-only)
│   ├── RuleModel.java             # rule_id, schema_fields_count, metadata, trigger_criteria[], condition_tree (AST)
│   └── SchemaDefinition.java      # lightweight wrapper: totalFields + column name lists
└── rule_gen/
    ├── RuleGenerator.java         # AST rule generator (depth 1-2, multi-source trigger_criteria, dual 36-field schema)
    └── Main.java
```

Output paths:
- Rules: `data/rules/<totalRules>/<timestamp>.json`
- Schemas: `data/schema/36/<version>/<timestamp>/schema_a.json` & `schema_b.json`

---

## Dual Schema — 36 Leaf Fields Each

Data types: `STRING`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `TIMESTAMP`, `BOOLEAN`.
Categories: `static_categorical`, `static_numeric`, `dynamic_categorical`, `dynamic_numeric`.

### Structure & Layout
- **Metadata**: Nested object containing `customer_id`, `schema_version`, `source`, `event_time`.
- **Flat root fields**: Static profile fields + dynamic metrics/flags.
- **Nested dynamic group**: Mixed-type sub-object:
  - **Schema A (`debt`)**: `loan_repayment_status` (STRING), `transfer_amount_today_vnd` (FLOAT), `loan_repayment_amount_this_month_vnd` (FLOAT), `total_outstanding_debt_vnd` (FLOAT).
  - **Schema B (`risk_signals`)**: `session_status` (STRING), `is_suspicious_ip` (BOOLEAN), `fraud_probability_score` (FLOAT), `behavioral_anomaly_score` (FLOAT).

### Schema Payload (`structure` map)
The `structure` field mirrors event JSON 100%. Every leaf field specifies `type` and `category`:

```json
{
  "metadata": { "schema_version": "v2", "source": "A", "timestamp": "2026-08-24T14:45:00.000+07:00" },
  "key_field": "customer_id",
  "total_fields": 36,
  "structure": {
    "metadata": {
      "customer_id": { "type": "STRING", "category": "static_categorical" },
      "event_time":  { "type": "TIMESTAMP", "category": "dynamic_categorical" }
    },
    "customer_segment": { "type": "STRING", "category": "static_categorical" },
    "age": { "type": "INT", "category": "static_numeric" },
    "total_transaction_count_lifetime": { "type": "LONG", "category": "dynamic_numeric" },
    "average_transaction_amount_vnd": { "type": "DOUBLE", "category": "dynamic_numeric" },
    "debt": {
      "loan_repayment_status": { "type": "STRING", "category": "dynamic_categorical" },
      "transfer_amount_today_vnd": { "type": "FLOAT", "category": "dynamic_numeric" }
    }
  }
}
```

Headers on Kafka messages: `version: v2`, `source: A|B`.

---

## Data Generation

- `ENUM` → random pick from `enumValues`.
- `RANGE` (`INT`/`LONG`/`FLOAT`/`DOUBLE`) → random in `[minValue, maxValue]`.
- `TIMESTAMP` → ISO-8601 string with millisecond precision within range.
- `BOOLEAN` → `true` or `false`.
- Static fields: Seeded `Random(entityId * 31L)` (deterministic per customer ID).
- Dynamic fields: Global unseeded `Random`.
- **Kafka Partition Key**: Salted key `ID_{entityId}_{salt}` (`salt` random 0..1000) to distribute traffic across partitions under data skew, while keeping `metadata.customer_id` as `ID_{entityId}`.

### Event JSON Example (Schema A)
```json
{
  "metadata": {
    "customer_id": "ID_42",
    "schema_version": "v2",
    "source": "A",
    "event_time": "2026-08-24T14:45:00.123+07:00"
  },
  "customer_segment": "PREMIUM",
  "age": 35,
  "is_vip_member": true,
  "total_transaction_count_lifetime": 12450,
  "average_transaction_amount_vnd": 1850000.00,
  "debt": {
    "loan_repayment_status": "ON_TIME",
    "transfer_amount_today_vnd": 1500000.0,
    "loan_repayment_amount_this_month_vnd": 2000000.0,
    "total_outstanding_debt_vnd": 50000000.0
  }
}
```

---

## Rule Generation

Rules contain `metadata`, pre-filter `trigger_criteria` (array), and a `condition_tree` (depth 1-2).

### Operator Coverage (per DATA_TYPE.md)

| Type | Operators | rhs variants |
|---|---|---|
| INT / LONG | `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN` | literal, `right_field` |
| FLOAT / DOUBLE | `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN` | literal, `right_field` |
| STRING | `==`, `!=`, `IN`, `NOT IN` | literal, `right_field` |
| BOOLEAN | `==`, `!=` | literal, `right_field` |
| TIMESTAMP | `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN` | ISO-8601 literal, `right_field` |

For `IN`, `NOT IN`, `BETWEEN`: rhs is always a literal list (no `right_field` inside list ops).

### Window Format

```json
// Tumbling window
{ "type": "tumbling", "duration": "1h" }

// Sliding window
{ "type": "sliding", "duration": "20m", "slide": "5m" }
```

The optional `filter` field sits **at the same level as `field` and `agg`**, not inside `window`:

```json
{
  "field": "A.v2.daily_spend_total_vnd",
  "agg": "sum",
  "filter": { "field": "transaction_type", "op": "IN", "value": ["TRANSFER", "PAYMENT"] },
  "window": { "type": "sliding", "duration": "20m", "slide": "5m" },
  "op": ">=",
  "threshold": 50000000.00
}
```

### Rule Structure Example

```json
{
  "rule_id": "rule_B_54",
  "schema_fields_count": 36,
  "metadata": {
    "event_time": "2026-08-24T16:02:37.123+07:00",
    "user_id": "user_001"
  },
  "trigger_criteria": [
    {
      "source": "B",
      "version": "v2",
      "conditions": [
        { "field": "nps_score_baseline", "op": "BETWEEN", "value": [3, 8] },
        { "field": "device_type", "op": "NOT IN", "value": ["DESKTOP"] },
        { "field": "total_login_count_lifetime", "op": ">=", "value": 500 }
      ]
    },
    {
      "source": "A",
      "version": "v1",
      "conditions": [
        { "field": "customer_segment", "op": "IN", "value": ["PREMIUM", "VIP"] }
      ]
    }
  ],
  "condition_tree": {
    "type": "OR",
    "children": [
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.risk_signals.fraud_probability_score",
          "agg": "avg",
          "filter": { "field": "device_type", "op": "==", "value": "TABLET" },
          "window": { "type": "sliding", "duration": "20m", "slide": "5m" },
          "op": "<=",
          "threshold": 40.13
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.last_login_time",
          "op": "BETWEEN",
          "value": ["2026-08-01T00:00:00.000+07:00", "2026-08-24T23:59:59.000+07:00"]
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.total_login_count_lifetime",
          "op": "IN",
          "value": [100, 500, 1000]
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "expr": "B.v2.pages_viewed_session - B.v2.products_viewed_session",
          "op": ">=",
          "right_field": "B.v2.app_session_count_today"
        }
      }
    ]
  }
}
```

### Expression Types in condition_tree

| # | Builder | Description |
|---|---|---|
| 0 | Categorical | STRING `==`/`!=`/`IN`/`NOT IN`; rhs: literal or `right_field` |
| 1 | Numeric | INT/LONG/FLOAT/DOUBLE; all ops per type; rhs: literal, `right_field`, or structured Map |
| 2 | Window agg | `SUM`/`AVG`/`MAX`/`MIN`/`COUNT`; tumbling or sliding; optional `filter` |
| 3 | Boolean | `==`/`!=`; rhs: literal or `right_field` |
| 4 | Linear combination | Weighted sum formula; raw string or windowed Map with `expr` key |
| 5 | Timestamp | `==`/`!=`/`>`/`<`/`>=`/`<=`/`BETWEEN`; rhs: ISO-8601 literal or `right_field` |
| 6 | Expr-lhs | Arithmetic expr as lhs; rhs: `value`, `right_field`, or `right_expr` |

---

## Configuration

| Property | Default | Description |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | Kafka broker URL |
| `kafka.topic.raw-event` | `source.event` | Topic for event stream |
| `kafka.topic.schema` | `source.schema` | Topic for schema registry |
| `schema.version` | `v2` | Schema version tag |
| `reqPerSecond` | `10` | Event throughput |
| `idRange` | `100` | Customer ID pool size |
| `totalRules` | `1000` | Number of rules to generate |
| `maxUserId` | `20` | Max user_id in rule metadata |
| `maxTreeDepth` | `2` | Max condition_tree AST depth (randomized 1..maxTreeDepth) |