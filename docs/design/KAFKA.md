# Kafka Topics and Consumer Groups

Lakerunner uses Kafka for distributed work coordination. All topics are prefixed with `lakerunner` by default (configurable via `kafka_topics.topicPrefix`).

## Topic Architecture

The system uses three topic categories:

1. **Objstore Topics** - Raw file notifications from pubsub
2. **Segments Topics** - Grouped work units for processing
3. **Boxer Topics** - Segment notifications for downstream grouping

## Topic Flow

```
PubSub → objstore.ingest.* → Boxer-Ingest → segments.*.ingest → Ingest Workers
                                                                       ↓
                                                            (produce segments)
                                                                       ↓
Boxer-Compact ← boxer.*.compact ←──────────────────────────────────────┘
      ↓
segments.*.compact → Compact Workers → (compacted segments)
      ↓
boxer.*.compact (cycle continues)

Boxer-Rollup ← boxer.metrics.rollup ← Ingest Workers (metrics only)
      ↓
segments.metrics.rollup → Rollup Workers → (rolled up metrics)
      ↓
boxer.metrics.rollup (next tier) + boxer.metrics.compact
```

## Topics and Consumer Groups

### Objstore Ingest Topics

Written by: **pubsub** service
Read by: **boxer-ingest-\*** services

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.objstore.ingest.logs` | `lakerunner.ingest.logs` | boxer-ingest-logs |
| `lakerunner.objstore.ingest.metrics` | `lakerunner.ingest.metrics` | boxer-ingest-metrics |
| `lakerunner.objstore.ingest.traces` | `lakerunner.ingest.traces` | boxer-ingest-traces |

### Segment Ingest Topics

Written by: **boxer-ingest-\*** services
Read by: **ingest-\*** workers

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.segments.logs.ingest` | `lakerunner.segments.logs.ingest` | ingest-logs |
| `lakerunner.segments.metrics.ingest` | `lakerunner.segments.metrics.ingest` | ingest-metrics |
| `lakerunner.segments.traces.ingest` | `lakerunner.segments.traces.ingest` | ingest-traces |

### Segment Compact Topics

Written by: **boxer-compact-\*** services
Read by: **compact-\*** workers

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.segments.logs.compact` | `lakerunner.compact.logs` | compact-logs |
| `lakerunner.segments.metrics.compact` | `lakerunner.compact.metrics` | compact-metrics |
| `lakerunner.segments.traces.compact` | `lakerunner.compact.traces` | compact-traces |

### Segment Rollup Topics (Metrics Only)

Written by: **boxer-rollup-metrics** service
Read by: **rollup-metrics** worker

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.segments.metrics.rollup` | `lakerunner.rollup.metrics` | rollup-metrics |

### Boxer Compact Topics

Written by: **ingest-\*** and **compact-\*** workers
Read by: **boxer-compact-\*** services

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.boxer.logs.compact` | `lakerunner.boxer.logs.compact` | boxer-compact-logs |
| `lakerunner.boxer.metrics.compact` | `lakerunner.boxer.metrics.compact` | boxer-compact-metrics |
| `lakerunner.boxer.traces.compact` | `lakerunner.boxer.traces.compact` | boxer-compact-traces |

### Boxer Rollup Topics (Metrics Only)

Written by: **ingest-metrics** and **rollup-metrics** workers
Read by: **boxer-rollup-metrics** service

| Topic | Consumer Group | Reader Service |
|-------|---------------|----------------|
| `lakerunner.boxer.metrics.rollup` | `lakerunner.boxer.metrics.rollup` | boxer-rollup-metrics |

## Service Roles

### Producers

- **pubsub** - Publishes S3 notifications to `objstore.ingest.*` topics
- **boxer-ingest-\*** - Groups raw notifications into work units on `segments.*.ingest` topics
- **boxer-compact-\*** - Groups segment notifications into compaction work on `segments.*.compact` topics
- **boxer-rollup-metrics** - Groups segments into rollup work on `segments.metrics.rollup` topic
- **ingest-\*** workers - After processing, publish to `boxer.*.compact` (and `boxer.metrics.rollup` for metrics)
- **compact-\*** workers - After compaction, publish to `boxer.*.compact` for follow-up
- **rollup-metrics** worker - After rollup, publish to `boxer.metrics.rollup` (next tier) and `boxer.metrics.compact`

### Consumers

- **boxer-ingest-\*** - Consume `objstore.ingest.*` topics
- **ingest-\*** workers - Consume `segments.*.ingest` topics
- **compact-\*** workers - Consume `segments.*.compact` topics
- **rollup-metrics** worker - Consumes `segments.metrics.rollup` topic
- **boxer-compact-\*** - Consume `boxer.*.compact` topics
- **boxer-rollup-metrics** - Consumes `boxer.metrics.rollup` topic

## Consumer Group Patterns

Each service type has **exactly one consumer group** per topic. This ensures:
- Work is distributed across all replicas of a service
- No duplicate processing
- Kafka handles load balancing via partition assignment

## Configuration

Topic names and consumer groups are managed in `config/kafka_topics.go`. The prefix is configurable:

```yaml
kafka_topics:
  topicPrefix: lakerunner  # default
```

Override via environment:
```bash
LAKERUNNER_KAFKA_TOPIC_PREFIX=my-prefix
```

## Debugging Commands

Lakerunner provides built-in Kafka debugging tools accessible via `lakerunner debug kafka`.

### Check Consumer Lag

View lag for all consumer groups across topics:

```bash
./bin/lakerunner debug kafka consumer-lag
```

This shows how far behind each consumer group is from the latest messages in their topics. Use this to identify:
- Slow consumers that can't keep up with production rate
- Stalled consumers that have stopped processing
- Scaling opportunities (high lag = need more replicas)

Run with `--help` for filtering and formatting options.

### Flush Consumer Groups

Reset a consumer group to the current high water mark (latest offset):

```bash
./bin/lakerunner debug kafka flush-consumer
```

This skips all pending messages and moves the consumer group to the latest position. Useful for:
- Clearing backlogs after incidents
- Skipping corrupted messages
- Testing with fresh data

**Warning:** This discards unprocessed messages. Use with caution in production.

Run with `--help` for topic filtering and confirmation options.

### Additional Commands

Other available commands:
- `tail` - Stream messages from a topic in real-time

Run `./bin/lakerunner debug kafka --help` for the complete list.
