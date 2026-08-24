# Data Generator & Rule Generator (`data-generator`)

Generates synthetic CDP streaming events and hierarchical condition-tree rules to benchmark stateful streaming on Kafka + Flink.

---

## Package Structure

```text
generator/
├── common/
│   ├── Constants.java             # field lists: 200-field legacy + Schema A/B (34 each)
│   ├── KafkaProducerClient.java   # send() and sendWithHeader()
│   └── PathUtils.java             # output path resolution
├── data_gen/
│   ├── DataGenerator.java         # dual-schema event loop → Kafka
│   ├── SchemaPublisher.java       # publishes Schema A + B to Kafka + local files on startup
│   ├── SchemaConsumerExample.java # reference: reads schema topic, validates events
│   └── Main.java
├── model/
│   ├── FieldDefinition.java       # name, type, category (constraint fields internal-only)
│   ├── RuleModel.java             # rule_id, schema_fields_count, metadata, condition_tree (AST)
│   └── SchemaDefinition.java      # lightweight wrapper: totalFields + column name lists
└── rule_gen/
    ├── RuleGenerator.java         # AST rule generator, 5 expression types (dual 34-field schema)
    └── Main.java
```

Output paths:
- Rules: `data/rules/<totalRules>/<timestamp>.json`
- Schemas: `data/schema/34/<version>/<timestamp>/schema_a.json` & `schema_b.json`

---

## Dual Schema — 34 Leaf Fields Each

Data types: `STRING`, `INT`, `FLOAT`, `TIMESTAMP`, `BOOLEAN`.  
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
  "metadata": { "schema_version": "v2", "source": "A", "timestamp": "2026-08-24T14:45:00+07:00" },
  "key_field": "customer_id",
  "total_fields": 34,
  "structure": {
    "metadata": {
      "customer_id": { "type": "STRING", "category": "static_categorical" },
      "event_time":  { "type": "TIMESTAMP", "category": "dynamic_categorical" }
    },
    "customer_segment": { "type": "STRING", "category": "static_categorical" },
    "age": { "type": "INT", "category": "static_numeric" },
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
- `RANGE` (`INT`/`FLOAT`) → random in `[minValue, maxValue]`.
- `TIMESTAMP` → ISO-8601 string within range.
- `BOOLEAN` → `true` or `false`.
- Static fields: Seeded `Random(entityId * 31L)` (deterministic per customer ID).
- Dynamic fields: Global unseeded `Random`.

### Event JSON Example (Schema A)
```json
{
  "metadata": {
    "customer_id": "ID_42",
    "schema_version": "v2",
    "source": "A",
    "event_time": "2026-08-24T14:45:00+07:00"
  },
  "customer_segment": "PREMIUM",
  "age": 35,
  "is_vip_member": true,
  "debt": {
    "loan_repayment_status": "ON_TIME",
    "transfer_amount_today_vnd": 1500000.0,
    "loan_repayment_amount_this_month_vnd": 2000000.0,
    "total_outstanding_debt_vnd": 50000000.0
  }
}
```

### Traffic Skew
- `skewIdCount * skewPctPerSkewId ≤ 80%`.
- Skew IDs (1..k) take configured traffic share; remaining IDs split traffic uniformly.

---

## Rule Generation

Condition trees (AND/OR gates + CONDITION leaves, depth 2–5). Rule targets source A or B randomly.

### Rule Metadata & Paths
- Metadata: `timestamp` (ISO-8601), `user_id` (`user_001`..`user_<maxUserId>`).
- Paths: `{src}.v2.{fieldName}` (root) or `{src}.v2.{group}.{fieldName}` (nested).
  - e.g., `A.v2.customer_segment`, `A.v2.debt.loan_repayment_status`, `B.v2.risk_signals.is_suspicious_ip`.

### Expression Types
1. **Categorical**: `A.v2.customer_segment == 'PREMIUM'`
2. **Numeric**: `A.v2.age >= 35`, `B.v2.risk_signals.fraud_probability_score > 75.0`
3. **Boolean**: `A.v2.is_vip_member == true`, `B.v2.risk_signals.is_suspicious_ip == true`
4. **Window Aggregation** (Structured object):
   ```json
   {
     "type": "CONDITION",
     "expression": {
       "field": "A.v2.debt.transfer_amount_today_vnd",
       "agg": "sum",
       "window": { "type": "tumbling", "time": "10m" },
       "op": ">=",
       "threshold": 50000000.00
     }
   }
   ```
5. **Linear Combination**: `(A.v2.daily_spend_total_vnd * 0.7 + A.v2.debt.transfer_amount_today_vnd * 0.3) >= 50000000.00`

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
| `skewIdCount` | `5` | Number of skewed IDs |
| `skewPctPerSkewId` | `10.0` | Traffic share per skew ID (%) |
| `totalRules` | `1000` | Number of rules to generate |
| `maxUserId` | `20` | Max user_id in rule metadata |