# Data Generator & Rule Generator (`data-generator`)

Produces synthetic CDP streaming events and hierarchical condition-tree rules to benchmark stateful streaming on Kafka + Flink.

---

## Package Structure

```text
generator/
├── common/
│   ├── Constants.java             # field lists: 200-field legacy + Schema A/B (30 each)
│   ├── KafkaProducerClient.java   # send() and sendWithHeader()
│   └── PathUtils.java             # output path resolution
├── data_gen/
│   ├── DataGenerator.java         # dual-schema event loop → Kafka
│   ├── SchemaPublisher.java       # publishes Schema A + B to Kafka + local files on startup
│   ├── SchemaConsumerExample.java # reference: reads schema topic, validates events
│   └── Main.java
├── model/
│   ├── FieldDefinition.java       # name, type, constraint_kind, enum/range values
│   ├── RuleModel.java             # rule_id, schema_fields_count, condition_tree (AST)
│   └── SchemaDefinition.java      # lightweight wrapper: totalFields + column name lists
└── rule_gen/
    ├── RuleGenerator.java         # AST rule generator, 4 expression types (uses 200-field schema)
    └── Main.java
```

Output files:
```text
data/rules/<totalRules>/<timestamp>.json
data/schema/30/<version>/<timestamp>/schema_a.json
data/schema/30/<version>/<timestamp>/schema_b.json
```

---

## Dual Schema — 30 Fields Each (ratio 1:2:3:4 → 3/6/9/12)

### Schema A — Transaction Events

| Category | Count | Sample Fields |
|---|---|---|
| `static_categorical` | 3 | `customer_segment`, `loyalty_tier`, `risk_rating` |
| `dynamic_categorical` | 6 | `transaction_type`, `card_status`, `loan_repayment_status` |
| `static_numeric` | 9 | `age`, `base_credit_score`, `credit_limit_vnd`, `debt_to_income_ratio` |
| `dynamic_numeric` | 12 | `current_balance_vnd`, `daily_spend_total_vnd`, `transfer_amount_today_vnd` |

### Schema B — System Access Logs

| Category | Count | Sample Fields |
|---|---|---|
| `static_categorical` | 3 | `home_province`, `preferred_language`, `customer_type` |
| `dynamic_categorical` | 6 | `session_status`, `login_channel`, `device_type`, `auth_method` |
| `static_numeric` | 9 | `digital_adoption_score`, `behavioral_score_baseline`, `propensity_churn_score` |
| `dynamic_numeric` | 12 | `session_duration_seconds`, `response_latency_ms`, `fraud_probability_score` |

Both schemas share the same customer `id` as Kafka message key.

Schema JSON payload structure (published to Kafka + local files):
```json
{
  "version": "v2",
  "source": "A",
  "total_fields": 30,
  "fields": {
    "static_categorical":  [ { "name": "customer_segment", "type": "STRING", ... } ],
    "dynamic_categorical": [ "..." ],
    "static_numeric":      [ { "name": "age", "type": "INT", ... } ],
    "dynamic_numeric":     [ "..." ]
  }
}
```

Kafka headers on both schema messages and data events: `schema-version: v2`, `source: A|B`.

---

## Data Generation

- `ENUM` → random pick from `enum_values`
- `INT RANGE` → `nextInt(max - min + 1) + min`
- `FLOAT RANGE` → `min + (max - min) * nextDouble()`
- Static fields: seeded `Random(entityId * 31L)` — same ID always produces the same values.
- Dynamic fields: global unseeded `Random` — varies each event.
- Schema selection per tick: 50/50 random between A and B.
- Throughput: `sleep(1000 / reqPerSecond)` per iteration.

### Skew Data

Skew parameters control how traffic is distributed across customer IDs:

- `skewIdCount` — number of IDs (1..k) that receive heavy traffic.
- `skewPctPerSkewId` — individual traffic share (%) for each skew ID.
- Constraint: `skewIdCount * skewPctPerSkewId ≤ 80`. Exceeding this throws `IllegalArgumentException` with a descriptive message.
- Non-skew IDs `[k+1, idRange]` share the remaining traffic uniformly.

ID selection algorithm:
```
Roll r ∈ [0.0, 100.0)
For i in 0..skewIdCount-1:
    if r < (i+1) * skewPctPerSkewId → entityId = i+1 (skew ID)
Otherwise → uniform random from [skewIdCount+1, idRange]
```

---

## Rule Generation

Rules are condition trees (AND/OR gates + CONDITION leaves), depth 2–5, targeting the **200-field legacy schema** in `Constants`. Rule IDs: `rule_200_<i>`.

See [README rule section details](README.md) for expression types and window naming format.

### Window Aggregation Expression Format

```
<field>_<winType>_<agg>_<windowTime>_<subIntervalTime>
```

- **windowTime**: duration in minutes, one of `2m, 5m, 10m, 15m, 20m, 25m, 30m`
- **subIntervalTime**: whole-minute value strictly less than windowTime
  - **tumbling** — bucket; must evenly divide windowTime (`windowMin % bucketMin == 0`)
  - **sliding**  — slide step; any value in `[1, windowTime - 1]`

Examples:
```
daily_spend_total_vnd_tumbling_sum_10m_2m  >= 50000000.00
fraud_probability_score_sliding_count_5m_1m > 3.00
transfer_amount_today_vnd_tumbling_avg_30m_5m < 75000000.00
```

## Config

| Property | Default | Notes |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | |
| `kafka.topic.raw-event` | `source.event` | data events |
| `kafka.topic.schema` | `source.schema` | schema definitions |
| `schema.version` | `v2` | |
| `reqPerSecond` | `10` | code constant |
| `idRange` | `100` | code constant |
| `skewIdCount` | `5` | code constant |
| `skewPctPerSkewId` | `10.0` | code constant — 5 × 10% = 50% total skew |
| `totalRules` | `1000` | code constant (rule_gen only) |