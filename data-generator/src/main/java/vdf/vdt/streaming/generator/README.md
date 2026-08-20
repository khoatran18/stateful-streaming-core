# Data Generator & Rule Generator (`data-generator`)

Produces synthetic CDP streaming events and hierarchical condition-tree rules to benchmark stateful streaming on Kafka + Flink.

---

## Package Structure

```text
generator/
├── common/
│   ├── Constants.java             # 200-field schema as 4 typed lists
│   ├── KafkaProducerClient.java   # send() and sendWithHeader()
│   └── PathUtils.java             # output path resolution
├── data_gen/
│   ├── DataGenerator.java         # continuous event loop → Kafka
│   ├── SchemaPublisher.java       # publishes schema to Kafka + local files on startup
│   ├── SchemaConsumerExample.java # reference: reads schema topic, validates events
│   └── Main.java
├── model/
│   ├── FieldDefinition.java       # name, type, constraint_kind, enum/range values
│   ├── RuleModel.java             # rule_id, schema_fields_count, condition_tree (AST)
│   └── SchemaDefinition.java      # lightweight wrapper: totalFields + column name lists
└── rule_gen/
    ├── RuleGenerator.java         # AST rule generator, 4 expression types
    └── Main.java
```

Output files:
```text
data/rules/<totalRules>/<timestamp>.json
data/schema/<totalFields>/<version>/<timestamp>.json|yaml
```

---

## Schema — 200 Fields in 4 Categories (ratio 1:2:3:4)

| Category | Count | Description |
|---|---|---|
| `static_categorical` | 20 | ENUM, fixed per customer ID. Referenced with `_current` suffix in rules. |
| `dynamic_categorical` | 40 | ENUM, changes each event. |
| `static_numeric` | 60 | INT/FLOAT RANGE, fixed per customer ID. Referenced with `_current` suffix. |
| `dynamic_numeric` | 80 | INT/FLOAT RANGE, real-time metric per event. |

Each `FieldDefinition` carries: `name`, `type` (STRING/INT/FLOAT), `constraint_kind` (ENUM/RANGE), and either `enum_values` or `min_value`/`max_value`. Category is runtime-only (not in JSON) — the wrapping key in the schema payload already conveys it.

Schema JSON payload structure (published to Kafka + local files):
```json
{
  "version": "v1",
  "total_fields": 200,
  "fields": {
    "static_categorical":  [ { "name": "loyalty_tier", "type": "STRING", "constraint_kind": "ENUM", "enum_values": ["BRONZE",...] } ],
    "dynamic_categorical": [ "..." ],
    "static_numeric":      [ { "name": "age", "type": "INT", "constraint_kind": "RANGE", "min_value": 18, "max_value": 100 } ],
    "dynamic_numeric":     [ "..." ]
  }
}
```

Data events on the Kafka data topic carry header `schema-version: v1` for downstream routing.

---

## Data Generation

- `ENUM` → random pick from `enum_values`
- `INT RANGE` → `nextInt(max - min + 1) + min`
- `FLOAT RANGE` → `min + (max - min) * nextDouble()`
- Static fields: seeded `Random(entityId * 31L)` — same ID always produces the same values.
- Dynamic fields: global unseeded `Random` — varies each event.
- Throughput: `sleep(1000 / reqPerSecond)` per iteration.

---

## Rule Generation

Rules are condition trees (AND/OR gates + CONDITION leaves), depth 2–5, targeting the 200-field schema. Rule IDs: `rule_200_<i>`.

**Type 0 — Categorical** (ENUM fields, `==` / `!=`)
```
loyalty_tier_current == 'GOLD'
churn_risk_flag != 'CHURNED'
```

**Type 1 — Raw Numeric** (RANGE fields)
- Static: `_current` suffix, full operators (`==`, `!=`, `<=`, `>=`, `<`, `>`).
- Dynamic: inequalities only.
```
age_current >= 35
daily_spend_total_vnd > 50000000.00
```

**Type 2 — Window Aggregation** (dynamic numeric only)
- Name format: `<field>_<windowType>_<agg>_<time>`
- Window types: `tumbling` (non-overlapping) or `sliding` (overlapping).
- Windows: `5s`, `10s`, `30s`, `1m`, `5m`, `10m`, `1h`. Aggregations: `sum`, `count`, `avg`, `max`, `min`.
- Thresholds: `expectedEventsPerId = reqPerSecond × windowSeconds / idRange`; count/sum scale by it, min/max/avg are random within field range.
```
fraud_probability_score_tumbling_count_5m > 3.00
daily_spend_total_vnd_sliding_sum_1h >= 50000000.00
```

**Type 3 — Linear Combination** (any 2 numeric fields)
```
(current_balance_vnd * 0.7 + monthly_spend_total_vnd * 0.3) >= 500000000.00
```

Rule JSON structure:
```json
{
  "rule_id": "rule_200_0",
  "schema_fields_count": 200,
  "condition_tree": {
    "type": "AND",
    "children": [
      { "type": "CONDITION", "expression": "loyalty_tier_current == 'GOLD'" },
      { "type": "CONDITION", "expression": "daily_spend_total_vnd_tumbling_sum_1h >= 50000000.00" }
    ]
  }
}
```

---

## Config

| Property | Default | Notes |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | |
| `kafka.topic` | `stream-input-events` | data events |
| `kafka.schema-topic` | `stream-schema-registry` | schema definitions |
| `schema.version` | `v1` | |
| `reqPerSecond` | `10` | code constant |
| `idRange` | `100` | code constant |
| `totalRules` | `1000` | code constant |