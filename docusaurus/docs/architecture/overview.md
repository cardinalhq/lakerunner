---
sidebar_position: 1
---

# Architecture Overview

Lakerunner processes three types of telemetry data through a common event-driven pipeline with type-specific optimizations.

## Design Principles

1. **Object Storage First** – All telemetry data lives in S3 as Parquet files
2. **Event-Driven Processing** – Kafka topics coordinate distributed work
3. **Columnar Analytics** – DuckDB executes queries directly against Parquet
4. **Metadata Indexing** – PostgreSQL maintains segment metadata for query planning
5. **Stateless Workers** – All services are horizontally scalable and ephemeral

## Common Pipeline

All data types share the same event notification and ingestion entry point:

```mermaid
flowchart TB
    subgraph External
        collectors[Collectors]
        grafana[Grafana]
    end

    subgraph Storage
        s3_raw[S3: Raw Files]
        s3_cooked[S3: Parquet Segments]
    end

    subgraph Coordination
        kafka[Kafka]
        postgres[PostgreSQL]
    end

    subgraph Workers
        pubsub[PubSub]
        ingest[Ingest Workers]
        compact[Compact Workers]
        rollup[Rollup Workers]
        query[Query Workers]
    end

    collectors -->|write| s3_raw
    s3_raw -->|events| pubsub
    pubsub --> kafka
    kafka --> ingest
    kafka --> compact
    kafka --> rollup
    ingest --> s3_cooked
    compact --> s3_cooked
    rollup --> s3_cooked
    ingest --> postgres
    compact --> postgres
    rollup --> postgres
    query --> s3_cooked
    query --> postgres
    grafana --> query
```

## Event Notification

When collectors write raw telemetry to object storage, the store emits notifications. The PubSub service receives these and publishes to Kafka:

```mermaid
flowchart TB
    collectors["Collectors"]
    raw_s3["S3: Raw Files"]
    sqs["AWS SQS"]
    pubsub_gcp["GCP Pub/Sub"]
    eventgrid["Azure Event Grid"]
    webhook["HTTP Webhook"]
    pubsub["PubSub Service"]
    kafka_ingest["Kafka Topic: ingest"]

    collectors -->|"writes"| raw_s3
    raw_s3 -.->|"S3 events"| sqs
    raw_s3 -.->|"GCS events"| pubsub_gcp
    raw_s3 -.->|"Blob events"| eventgrid
    raw_s3 -.->|"notifications"| webhook
    sqs -->|"file notification"| pubsub
    pubsub_gcp -->|"file notification"| pubsub
    eventgrid -->|"file notification"| pubsub
    webhook -->|"file notification"| pubsub
    pubsub -->|"publishes"| kafka_ingest
```

## Data Type Processing

Each telemetry type has specialized processing:

| Data Type | Ingestion | Compaction | Additional Processing |
| --------- | --------- | ---------- | --------------------- |
| [Logs](./logs.md) | Schema discovery, fingerprinting | Merge and dedupe | Log pattern analysis |
| [Metrics](./metrics.md) | DDSketch encoding, TID generation | Merge by time series | Multi-tier rollups |
| [Traces](./traces.md) | Span fingerprinting | Merge by trace | Service mapping |

## Query Path

```mermaid
flowchart LR
    grafana[Grafana] --> api[Query API]
    api --> postgres[PostgreSQL Index]
    api --> worker[Query Worker]
    worker --> duckdb[DuckDB]
    duckdb --> s3[S3 Parquet]
```

1. Query API parses SQL and extracts time ranges
2. Segment index consulted for partition pruning
3. Query plan distributed to workers
4. DuckDB reads Parquet directly from S3
5. Results streamed back to client

## Storage Layout

```
s3://bucket/
├── logs-raw/           # Raw collector output
├── logs-cooked/        # Processed Parquet segments
│   └── org_id=123/
│       └── dateint=20250114/
│           └── seg_<uuid>.parquet
├── metrics-raw/
├── metrics-cooked/
├── metrics-rollup-60s/
├── metrics-rollup-5m/
├── metrics-rollup-20m/
├── metrics-rollup-1h/
├── traces-raw/
└── traces-cooked/
```

## Service Summary

| Service | Role |
| ------- | ---- |
| **pubsub** | Receives S3 notifications, publishes to Kafka |
| **ingest-logs** | Converts raw logs to Parquet with fingerprinting |
| **ingest-metrics** | Converts raw metrics to Parquet with DDSketch |
| **ingest-traces** | Converts raw spans to Parquet |
| **boxer-compact** | Groups segments for compaction |
| **compact-logs/metrics/traces** | Merges small segments |
| **boxer-rollup-metrics** | Groups metrics for rollup |
| **rollup-metrics** | Pre-aggregates metrics by time window |
| **sweeper** | Cleans up expired segments |
| **query-api** | SQL parsing and query planning |
| **query-worker** | DuckDB execution against Parquet |

## Next Steps

- [Logs Architecture](./logs.md) – Log processing pipeline and fingerprinting
- [Metrics Architecture](./metrics.md) – Metrics pipeline with rollups
- [Traces Architecture](./traces.md) – Trace processing and span fingerprinting
