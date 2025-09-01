# Telemetry Documentation

This document describes all telemetry metrics emitted by LakeRunner services.

## Common Attributes

Many metrics include these standardized attributes:

- `signal` - The type of telemetry data being processed: `metrics`, `logs`, or `traces`
- `action` - The operation being performed: `ingest`, `compact`, or `rollup`
- `organizationID` - UUID of the organization the data belongs to
- `instanceNum` - Instance number for the organization
- `component` - Component name for scoped metrics

## Counters

| Metric Name | Description | Attributes |
|-------------|-------------|------------|
| `lakerunner.reader.rows.in` | Number of rows read by readers from their input source | `reader` |
| `lakerunner.reader.rows.out` | Number of rows output by readers to downstream processing | `reader` |
| `lakerunner.reader.rows.dropped` | Number of rows dropped by readers due to invalid data | `reader`, `reason`, `metric_type` |
| `lakerunner.s3.download_errors` | Number of S3 download errors | - |
| `lakerunner.s3.download_not_found` | Number of missing S3 objects during download | - |
| `lakerunner.s3.download_count` | Number of S3 downloads | - |
| `lakerunner.s3.download_bytes` | Bytes downloaded from S3 | - |
| `lakerunner.s3.upload_count` | Number of S3 uploads | - |
| `lakerunner.s3.upload_bytes` | Bytes uploaded to S3 | - |
| `lakerunner.compaction.segments.filtered` | Number of segments filtered out during compaction processing | `organizationID`, `instanceNum`, `signal`, `action`, `reason` |
| `lakerunner.compaction.segments.processed` | Number of segments successfully processed during compaction | `organizationID`, `instanceNum`, `signal`, `action` |
| `lakerunner.file.sorted` | Number of files processed, tracking whether they were sorted or not | `input_sorted` |
| `lakerunner.metric.rollup.file.sorted` | Number of files processed during metric rollup | `component` |
| `lakerunner.metric.compact.file.sorted` | Number of files processed during metric compaction | - |
| `lakerunner.metric.ingest.file.sorted` | Number of files processed during metric ingestion | - |
| `lakerunner.pipeline.bufferpool.gets` | Total number of gets from the buffer pool | - |
| `lakerunner.pipeline.bufferpool.puts` | Total number of puts back to the buffer pool | - |
| `lakerunner.sweeper.object_cleanup_total` | Count of objects processed during cleanup | `result` |
| `lakerunner.sweeper.legacy_table_sync_total` | Count of legacy table synchronization runs | - |
| `lakerunner.sweeper.workqueue_expiry_total` | Count of work queue items expired due to staleness | `signal`, `action` |
| `lakerunner.sweeper.inqueue_expiry_total` | Count of inqueue items expired due to staleness | `signal` |
| `lakerunner.sweeper.mcq_expiry_total` | Count of MCQ items expired due to stale heartbeats | - |
| `lakerunner.sweeper.signal_lock_cleanup_total` | Count of orphaned signal locks cleaned up | - |
| `lakerunner.sweeper.metric_estimate_update_total` | Count of metric estimate updates processed | `estimate_source` |

## Histograms

| Metric Name | Description | Unit | Attributes |
|-------------|-------------|------|------------|
| `lakerunner.workqueue.request.delay` | The delay in ms for a request for new work to be returned | ms | - |
| `lakerunner.workqueue.duration` | The duration in seconds for a work item to be processed | s | - |
| `lakerunner.workqueue.lag` | The lag in seconds for a work item to be processed in the work queue | s | - |
| `lakerunner.inqueue.request.delay` | The delay in seconds for a request for new inqueue work to be returned | s | `hasError`, `errorIsNoRows` |
| `lakerunner.inqueue.duration` | The duration in seconds for an inqueue item to be processed | s | `organizationID`, `signal`, `instanceNum` |
| `lakerunner.inqueue.lag` | Time in seconds from when an item was queued until it was claimed for processing | s | `organizationID`, `signal`, `instanceNum` |
| `lakerunner.manual_gc.duration` | Duration of manual garbage collection in seconds | s | - |
| `lakerunner.sweeper.legacy_table_sync_duration_seconds` | Duration of legacy table synchronization runs in seconds | s | - |

## Gauges

| Metric Name | Description | Unit | Attributes |
|-------------|-------------|------|------------|
| `lakerunner.exists` | Indicates if the service is running (1) or not (0) | - | - |
| `lakerunner.duckdb.memory.database_size` | DuckDB database size | By | - |
| `lakerunner.duckdb.memory.block_size` | DuckDB block size | By | - |
| `lakerunner.duckdb.memory.total_blocks` | DuckDB total blocks | 1 | - |
| `lakerunner.duckdb.memory.used_blocks` | DuckDB used blocks | 1 | - |
| `lakerunner.duckdb.memory.free_blocks` | DuckDB free blocks | 1 | - |
| `lakerunner.duckdb.memory.wal_size` | DuckDB WAL size | By | - |
| `lakerunner.duckdb.memory.memory_usage` | DuckDB memory usage | By | - |
| `lakerunner.duckdb.memory.memory_limit` | DuckDB memory limit | By | - |

## Metric Groupings by Component

### File Reader Components

- `lakerunner.reader.rows.*` - Track row processing through the reader stack
- `lakerunner.file.sorted` - Track whether input files are pre-sorted

### S3 Operations

- `lakerunner.s3.*` - All S3 upload/download operations and error tracking

### Work Queue Management

- `lakerunner.workqueue.*` - Work queue request handling and processing duration
- `lakerunner.inqueue.*` - Inqueue (ingestion queue) processing metrics

### Processing Pipelines

- `lakerunner.compaction.segments.*` - Segment compaction processing
- `lakerunner.metric.*` - Metric-specific processing (ingest, compact, rollup)
- `lakerunner.pipeline.bufferpool.*` - Memory pool usage

### Background Maintenance

- `lakerunner.sweeper.*` - Cleanup and maintenance operations
- `lakerunner.manual_gc.duration` - Manual garbage collection

### Database Operations

- `lakerunner.duckdb.memory.*` - DuckDB memory usage and configuration

### Service Health

- `lakerunner.exists` - Service availability indicator

## Service-Level Attributes

Each service sets global attributes during startup:

| Service | signal | action |
|---------|--------|---------|
| ingest-metrics | metrics | ingest |
| ingest-logs | logs | ingest |
| ingest-traces | traces | ingest |
| compact-metrics | metrics | compact |
| compact-logs | logs | compact |
| compact-traces | traces | compact |
| rollup-metrics | metrics | rollup |

These attributes are automatically added to relevant metrics via `commonAttributes` sets.
