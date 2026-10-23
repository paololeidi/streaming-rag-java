# Enterprise Data Contract: SystemLog

This document outlines the formal schema, constraints, serialization rules, and vector normalization for distributed system logs ingested by **streaming-rag-java**.

---

## 1. Java 21 Record Definition

The central domain model is represented as an immutable Java 21 record:

```java
public record SystemLog(
    UUID eventId,
    Instant timestamp,
    String serviceName,
    LogLevel logLevel,
    String message,
    String stackTrace
)
```

### Field Specifications & Constraints

| Field | Type | Required | Description | Constraints |
|---|---|---|---|---|
| `eventId` | `UUID` | **Yes** | Globally unique identifier of the log event | Non-null, RFC 4122 UUID v4 |
| `timestamp` | `Instant` | **Yes** | UTC creation timestamp of the log event | Non-null, ISO-8601 UTC |
| `serviceName` | `String` | **Yes** | Originating microservice | Non-null, alphanumeric + hyphens |
| `logLevel` | `LogLevel` | **Yes** | Log severity | Enum: `INFO`, `WARN`, `ERROR` |
| `message` | `String` | **Yes** | Core event message | Non-null, max 2048 chars |
| `stackTrace` | `String` | **No** | Exception stack trace | Present for `ERROR` logs, nullable |

---

## 2. Kafka Partitioning & Ordering

- **Kafka Topic:** `system-logs`
- **Partition Key:** `serviceName` (`String`)
- **Guarantee:** Strict partition-level total ordering per microservice. Events from `payment-gateway` are always processed sequentially by partition consumers.

---

## 3. Vector Embedding Normalization

To ensure consistent semantic vector representations across both the Python and Java implementations, `SystemLog` normalizes text before embedding via `toEmbeddingText()`:

```text
[LEVEL] service-name @ timestamp
message
stack_trace: ... (optional)
```

### Example (Without Stack Trace)
```text
[WARN] user-auth @ 2026-10-12T14:15:30Z
Rate limit approaching for IP 192.168.1.50
```

### Example (With Stack Trace)
```text
[ERROR] inventory-service @ 2026-10-12T14:15:35Z
Optimistic lock exception on stock decrement for SKU-WIDGET-01
stack_trace: org.springframework.dao.OptimisticLockingFailureException: Row was updated by another transaction
    at com.streamingrag.inventory.StockRepository.decrement(StockRepository.java:93)
```

---

## 4. Apache Lucene Vector & Document Schema

When indexed into the embedded Apache Lucene vector database, each document contains the following Lucene fields:

| Lucene Field | Field Type | Purpose |
|---|---|---|
| `id` | `StringField` (Stored) | Unique vector document identifier (UUID) |
| `vector` | `KnnFloatVectorField` | 384-dimensional dense float vector indexed with `VectorSimilarityFunction.COSINE` |
| `text` | `StoredField` | Full normalized log text representation |
| `text_content` | `TextField` (Analyzed) | Standard-analyzed full text for lexical search |
| `metadata` | `StoredField` (JSON) | Serialized metadata map for citation reconstruction |
| `timestamp_ms` | `LongPoint` + `StoredField` | Epoch millisecond timestamp indexed in Lucene's BKD-tree for sliding time-window range queries |

### Metadata JSON Schema
```json
{
  "event_id": "9a7278c9-2c9d-4615-982e-0343ceed0b79",
  "service_name": "payment-gateway",
  "log_level": "ERROR",
  "timestamp": "2026-10-12T14:15:35Z",
  "partition": "1",
  "offset": "204"
}
```
