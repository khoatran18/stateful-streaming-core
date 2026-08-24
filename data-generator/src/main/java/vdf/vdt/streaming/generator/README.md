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
│   ├── FieldDefinition.java       # name, type (constraint fields are internal-only, not in JSON)
│   ├── RuleModel.java             # rule_id, schema_fields_count, metadata, condition_tree (AST)
│   └── SchemaDefinition.java      # lightweight wrapper: totalFields + column name lists
└── rule_gen/
    ├── RuleGenerator.java         # AST rule generator, 4 expression types (dual 30-field schema)
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

| Group | Category | Count | Sample Fields |
|---|---|---|---|
| `profile` | `static_categorical` | 3 | `customer_segment`, `loyalty_tier`, `risk_rating` |
| `profile` | `static_numeric` | 9 | `age`, `base_credit_score`, `credit_limit_vnd`, `debt_to_income_ratio` |
| `transaction` | `dynamic_categorical` | 6 | `transaction_type`, `card_status`, `loan_repayment_status` |
| `transaction` | `dynamic_numeric` | 12 | `current_balance_vnd`, `daily_spend_total_vnd`, `transfer_amount_today_vnd` |

### Schema B — System Access Logs

| Group | Category | Count | Sample Fields |
|---|---|---|---|
| `profile` | `static_categorical` | 3 | `home_province`, `preferred_language`, `customer_type` |
| `profile` | `static_numeric` | 9 | `digital_adoption_score`, `behavioral_score_baseline`, `propensity_churn_score` |
| `session` | `dynamic_categorical` | 6 | `session_status`, `login_channel`, `device_type`, `auth_method` |
| `session` | `dynamic_numeric` | 12 | `session_duration_seconds`, `response_latency_ms`, `fraud_probability_score` |

Both schemas share the same customer `customer_id` as Kafka message key.

Schema JSON payload (sent to Kafka + written to file):
```json
{
  "metadata": { "schema_version": "v2", "source": "A" },
  "key_field": "customer_id",
  "total_fields": 30,
  "groups": {
    "profile": {
      "static_categorical": [ { "name": "customer_segment", "type": "STRING" }, "..." ],
      "static_numeric":     [ { "name": "age",              "type": "INT"    }, "..." ]
    },
    "transaction": {
      "dynamic_categorical": [ { "name": "transaction_type",      "type": "STRING" }, "..." ],
      "dynamic_numeric":     [ { "name": "current_balance_vnd",   "type": "FLOAT"  }, "..." ]
    }
  }
}
```

Kafka headers on both schema messages and data events: `version: v2`, `source: A|B`.

---

## Data Generation

- `ENUM` → random pick from `enum_values`
- `INT RANGE` → `nextInt(max - min + 1) + min`
- `FLOAT RANGE` → `min + (max - min) * nextDouble()`
- Static fields (`profile` group): seeded `Random(entityId * 31L)` — same ID always produces the same values.
- Dynamic fields (`transaction`/`session` group): global unseeded `Random` — varies each event.
- Schema selection per tick: 50/50 random between A and B.
- Throughput: `sleep(1000 / reqPerSecond)` per iteration.

Data event JSON structure (Schema A example):
```json
{
  "metadata": {
    "customer_id": "ID_42",
    "schema_version": "v2",
    "source": "A",
    "timestamp": "2026-08-24T12:02:41+07:00"
  },
  "profile": {
    "customer_segment": "PREMIUM",
    "loyalty_tier": "GOLD",
    "risk_rating": "LOW",
    "age": 35,
    "base_credit_score": 750,
    "credit_limit_vnd": 50000000.0
  },
  "transaction": {
    "transaction_type": "TRANSFER",
    "card_status": "ACTIVE",
    "current_balance_vnd": 12500000.5,
    "daily_spend_total_vnd": 5000000.0,
    "transfer_amount_today_vnd": 1500000.0
  }
}
```

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

Rules are condition trees (AND/OR gates + CONDITION leaves), depth 2–5. Each rule targets one source
(A or B, chosen randomly). Rule IDs: `rule_{source}_{i}`.

Each rule carries a `metadata` block with `timestamp` (ISO-8601) and `user_id` (random `user_001`..`user_<maxUserId>`).

### Field Reference Format

All field references use the fully-qualified dot-path:

```
{source}.v2.{group}.{fieldName}
```

| Field location | Group | Example path | Allowed operators |
|---|---|---|---|
| static categorical | `profile` | `A.v2.profile.loyalty_tier` | `==`, `!=` |
| static numeric | `profile` | `A.v2.profile.age` | `==`, `!=`, `<=`, `>=`, `<`, `>` |
| dynamic categorical | `transaction`/`session` | `A.v2.transaction.transaction_type` | `==`, `!=` |
| dynamic numeric | `transaction`/`session` | `B.v2.session.fraud_probability_score` | `<=`, `>=`, `<`, `>` |

### Window Aggregation Expression Format

Window CONDITION nodes use a **structured object**. Sub-interval is omitted.

```json
{
  "type": "CONDITION",
  "expression": {
    "field": "A.v2.transaction.daily_spend_total_vnd",
    "agg": "sum",
    "window": { "type": "tumbling", "time": "10m" },
    "op": ">=",
    "threshold": 50000000.00
  }
}
```

- **window.time**: one of `"2m"`, `"5m"`, `"10m"`, `"15m"`, `"20m"`, `"25m"`, `"30m"`
- **window.type**: `"tumbling"` or `"sliding"`

### Expression Types

| # | Builder | Field pool | Output type |
|---|---|---|---|
| 0 | `buildCategoricalExpr` | `profile` static categorical | String — `path == 'VALUE'` |
| 1 | `buildRawNumericExpr` | `profile` static numeric + dynamic numeric | String |
| 2 | `buildWindowAggExprMap` | dynamic numeric | Object (see above) |
| 3 | `buildLinearCombinationExpr` | **dynamic numeric only** | String (raw) or Object (windowed) — 50/50 |

### Dynamic Categorical AND Pair (~20% of leaf positions)

```json
{
  "type": "AND",
  "children": [
    { "type": "CONDITION", "expression": "A.v2.transaction.transaction_type == 'TRANSFER'" },
    { "type": "CONDITION", "expression": {
        "field": "A.v2.transaction.transfer_amount_today_vnd",
        "agg": "sum",
        "window": { "type": "sliding", "time": "10m" },
        "op": ">=", "threshold": 50000000.00
    }}
  ]
}
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
| `maxUserId` | `20` | code constant (rule_gen only) — upper bound for random user_id |