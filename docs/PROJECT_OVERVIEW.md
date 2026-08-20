# Stateful Streaming Core — Architecture & Logic

> See also: [Code Style Rules](../.claude/rules/code-style.md)

---

## 1. Project Overview

This project builds the **Streaming Core layer** on top of **Apache Flink**, designed for real-time stateful computation and customer segmentation (CDP — Customer Data Platform). The system supports changing business logic (rules, formulas) while the job is running, without stopping or restarting.

In simple terms: events flow in from Kafka, the system aggregates and evaluates them against a set of rules, and outputs segment membership or trigger signals in real time — even as those rules change live.

---

## 2. Four Core Pillars

### 2.1 Stateful Window Aggregation

Uses a **Ring Buffer** to maintain time-series data per customer.

- Time is divided into fixed-size **buckets** (e.g. 1-second slices).
- The ring buffer holds a sliding window of bucket values, so computing `sum`, `count`, `avg`, `min`, `max` over the last N seconds only requires iterating the relevant buckets — no full replay.
- Supports both **tumbling windows** (fixed non-overlapping intervals) and **sliding windows** (overlapping, step < window size).
- Memory is bounded and predictable regardless of event volume.

### 2.2 Data-Driven Hot-Reload

Uses Flink **Broadcast State** to update business logic at runtime.

- Configuration changes (rules, formulas) are published via CDC (PostgreSQL → Debezium → Kafka).
- The Flink job receives them through a `BroadcastStream` and applies them to the runtime state of each operator.
- Processing is never interrupted — the data stream and the config stream run in parallel.
- Versioning is checked on every config update to prevent out-of-order or stale overrides.

### 2.3 High-Scale Filtering (Inverted Index)

Avoids evaluating every rule against every event.

- An **inverted index** is built from all rules, keyed by field value (similar to document frequency in search engines).
- When an event arrives, the system looks up only the rules that reference fields present in that event.
- Among candidate rules, the two rarest fields (lowest document frequency) are used as the primary filter axes to cut the candidate set before full evaluation.
- This reduces per-event rule evaluation from O(total rules) to O(matching rules).

### 2.4 Fault Tolerance

Guarantees **exactly-once** processing end to end.

- Flink Checkpoints and Savepoints persist operator state to durable storage.
- Late-arriving events are handled via **Watermarks** with a configurable allowed lateness.
- State TTL (time-to-live) is set per field category to avoid unbounded state growth:
  - Static fields: long TTL (rarely expires).
  - Dynamic fields: shorter TTL aligned to the longest window in use.
- Every Flink operator must have an explicit `uid` set so Savepoints can restore state correctly after code deploys.

---

## 3. Processing Pipeline

```
Kafka (data topic)
    │
    ▼
Ingestion Operator
    decode JSON → validate schema version → enrich with static fields
    │
    ▼
Aggregation Layer  (Keyed by customer ID)
    Ring Buffer per field → update bucket for current time slice
    Expose aggregated values: field_tumbling_sum_5m, field_sliding_avg_1h, ...
    │
    ▼
Filtering & Evaluation Layer
    Inverted index lookup → candidate rule set
    Expression engine evaluates AND/OR condition trees and linear combinations
    │
    ▼
Sink Operator
    Write segment membership / trigger signals to output topic or storage
    Cooldown logic prevents duplicate signals for the same customer
```

---

## 4. Data Generator & Rule Generator

See [data-generator README](../data-generator/src/main/java/vdf/vdt/streaming/generator/README.md) for full details.

**Data Generator** produces synthetic CDP events for the 200-field schema:
- 20 static categorical + 40 dynamic categorical + 60 static numeric + 80 dynamic numeric fields.
- Static fields are seeded by customer ID (same ID → same values every run).
- Events are pushed to Kafka with a `schema-version` header.

**Rule Generator** produces AST condition-tree rules targeting the same schema:
- Four expression types: categorical, raw numeric, window aggregation, linear combination.
- Window expressions include the window type in the field name: `field_tumbling_avg_5m`, `field_sliding_sum_1h`.
- Rules are written as JSON files under `data/rules/<totalRules>/<timestamp>.json`.

**Schema Publisher** publishes the full schema (with field categories) to Kafka and to local files:
- Kafka: schema topic, key = version.
- Local: `data/schema/<totalFields>/<version>/<timestamp>.json|yaml`.

---

## 5. Config & Rules Workflow

```
1. Developer or admin changes a rule/formula in PostgreSQL.
2. Debezium captures the change and publishes it to Kafka.
3. Flink BroadcastStream delivers the config update to all parallel operator instances.
4. Each operator validates the version and applies the new config atomically.
5. Subsequent events are evaluated against the updated rules immediately.
```

---

## 6. Coding Constraints

- **Design patterns:** State Pattern and Strategy for computation variants. Factory for rule/expression types.
- **Data-driven:** All business constants (thresholds, window sizes, topic names) must come from Broadcast State or configuration — never hardcoded.
- **Operator UIDs:** Must be set explicitly on every Flink operator. Without this, Savepoint restore fails after any topology change.
- **Observability:** Every operator must expose metrics: throughput (events/s), processing latency (ms), state size (bytes).
- **Comments:** Plain English, no HTML or markdown tags inside code. See [code style rules](../.claude/rules/code-style.md).
