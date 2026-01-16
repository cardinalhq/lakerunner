---
sidebar_position: 1
---

# Internals Overview

This section documents Lakerunner's internal data structures, storage formats, and implementation details.

## Parquet Schemas

Lakerunner stores all processed telemetry as Apache Parquet files with type-specific schemas:

| Data Type | Schema | Key Features |
| --------- | ------ | ------------ |
| [Logs](./logs-schema.md) | Flattened OTEL with fingerprinting | Pattern grouping via `chq_fingerprint` |
| [Metrics](./metrics-schema.md) | DDSketch-encoded time series | Pre-computed rollup statistics |
| [Traces](./traces-schema.md) | Flattened spans with semantic fingerprints | Service and operation grouping |

## Schema Conventions

### Field Prefixes

All Parquet schemas use consistent prefixes:

| Prefix | Description |
| ------ | ----------- |
| `chq_*` | System fields controlled by Lakerunner |
| `resource_*` | OTEL Resource attributes (infrastructure/deployment metadata) |
| `scope_*` | OTEL InstrumentationScope attributes |
| `attr_*` | Data-specific attributes (log/span/datapoint) |

### Name Normalization

OTEL attribute keys are normalized before storage:

- Converted to lowercase
- Dots (`.`) replaced with underscores (`_`)
- Non-alphanumeric characters replaced with underscores

Example: `http.request.method` becomes `attr_http_request_method`

### Type Coercion {#type-coercion}

OTEL attribute values are coerced to Parquet types:

| Source Type | Stored As |
| ----------- | --------- |
| String | string |
| Boolean | `"true"` / `"false"` |
| Integer | int64 or string |
| Float | float64 or string |
| Array | JSON string |
| Object/Map | JSON string |
| Null | Parquet null |

Metrics may preserve numeric types; logs and traces convert most values to strings.

## Schema Evolution

### Transactional Schemas

Schemas are **transactional** and **per-file**:

1. Each input file produces its own schema based on actual content
2. Schema discovery happens during ingestion
3. Compaction merges schemas with type promotion
4. Normalization converts values to match promoted types

### Type Promotion

When merging files with different types for the same field:

- String + Int64 → String
- Float64 + Int64 → Float64
- Any + String → String

### Backward Compatibility

- Required fields are never removed
- New optional fields may be added
- Column ordering is not guaranteed (access by name only)

## Database Schemas

### PostgreSQL Segment Index

The segment index tracks all Parquet files for query planning:

| Table | Purpose |
| ----- | ------- |
| `*_seg` tables | Segment metadata partitioned by org_id and dateint |

Key fields tracked per segment:

- File location (S3 path)
- Time range (min/max timestamp)
- Row count
- Column statistics for pruning

## Next Steps

- [Logs Schema](./logs-schema.md) – Detailed log Parquet schema
- [Metrics Schema](./metrics-schema.md) – Detailed metrics Parquet schema
- [Traces Schema](./traces-schema.md) – Detailed traces Parquet schema
