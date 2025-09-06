# Telemetry Documentation

This document describes all telemetry metrics emitted by Lakerunner services.

## Common Attributes

Many metrics include these standardized attributes:

- `action` - The operation being performed: `ingest`, `compact`, or `rollup`
- `component` - Component name for scoped metrics
- `signal` - The type of telemetry data being processed: `metrics`, `logs`, or `traces`

## Counters

| Metric Name | Description | Attributes |
|-------------|-------------|------------|
| `lakerunner.pipeline.bufferpool.gets` | Total number of gets from the buffer pool | - |
| `lakerunner.pipeline.bufferpool.puts` | Total number of puts back to the buffer pool | - |
| `lakerunner.processing.bytes.in` | Number of bytes input to processing pipeline | `signal`, `action` |
| `lakerunner.processing.bytes.out` | Number of bytes output from processing pipeline | `signal`, `action` |
| `lakerunner.processing.input.filetype` | Number of input files processed by processing pipeline | `filetype`, `input_sorted`, `action` (optional) |
| `lakerunner.processing.records.in` | Number of records input to processing pipeline | `signal`, `action` |
| `lakerunner.processing.records.out` | Number of records output from processing pipeline | `signal`, `action` |
| `lakerunner.processing.segments.download_errors` | Number of segment download errors during processing | `signal`, `action`, `reason` |
| `lakerunner.processing.segments.filtered` | Number of segments filtered out during processing pipeline | `signal`, `action`, `reason` |
| `lakerunner.processing.segments.in` | Number of segments input to processing pipeline | `signal`, `action` |
| `lakerunner.processing.segments.out` | Number of segments output from processing pipeline | `signal`, `action` |
| `lakerunner.reader.rows.dropped` | Number of rows dropped by readers due to invalid data | `reader`, `reason`, `metric_type` |
| `lakerunner.reader.rows.in` | Number of rows read by readers from their input source | `reader` |
| `lakerunner.reader.rows.out` | Number of rows output by readers to downstream processing | `reader` |
| `lakerunner.s3.download.bytes` | Bytes downloaded from S3 | - |
| `lakerunner.s3.download.count` | Number of S3 downloads | - |
| `lakerunner.s3.download.errors` | Number of S3 download errors | `reason` |
| `lakerunner.s3.upload.bytes` | Bytes uploaded to S3 | - |
| `lakerunner.s3.upload.count` | Number of S3 uploads | - |
| `lakerunner.sweeper.inqueue_expiry_total` | Count of inqueue items expired due to staleness | `signal` |
| `lakerunner.sweeper.legacy_table_sync_total` | Count of legacy table synchronization runs | - |
| `lakerunner.sweeper.mcq_expiry_total` | Count of MCQ items expired due to stale heartbeats | - |
| `lakerunner.sweeper.mrq_expiry_total` | Count of MRQ items expired due to stale heartbeats | - |
| `lakerunner.sweeper.metric_estimate_update_total` | Count of metric estimate updates processed | `estimate_source` |
| `lakerunner.sweeper.object_cleanup_total` | Count of objects processed during cleanup | `result` |
| `lakerunner.sweeper.signal_lock_cleanup_total` | Count of orphaned signal locks cleaned up | - |
| `lakerunner.sweeper.workqueue_expiry_total` | Count of work queue items expired due to staleness | `signal`, `action` |

## Histograms

| Metric Name | Description | Unit | Attributes |
|-------------|-------------|------|------------|
| `lakerunner.inqueue.duration` | The duration in seconds for an inqueue item to be processed | s | `bucket`, `batchSize` |
| `lakerunner.inqueue.lag` | Time in seconds from when an item was queued until it was claimed for processing | s | `signal` |
| `lakerunner.inqueue.request.delay` | The delay in seconds for a request for new inqueue work to be returned | s | `hasError`, `errorIsNoRows` |
| `lakerunner.sweeper.legacy_table_sync_duration_seconds` | Duration of legacy table synchronization runs in seconds | s | - |
| `lakerunner.workqueue.duration` | The duration in seconds for a work item to be processed | s | - |
| `lakerunner.workqueue.lag` | The lag in seconds for a work item to be processed in the work queue | s | - |
| `lakerunner.workqueue.request.delay` | The delay in ms for a request for new work to be returned | s | - |

## Gauges

| Metric Name | Description | Unit | Attributes |
|-------------|-------------|------|------------|
| `lakerunner.duckdb.memory.block_size` | DuckDB block size | By | - |
| `lakerunner.duckdb.memory.database_size` | DuckDB database size | By | - |
| `lakerunner.duckdb.memory.free_blocks` | DuckDB free blocks | 1 | - |
| `lakerunner.duckdb.memory.memory_limit` | DuckDB memory limit | By | - |
| `lakerunner.duckdb.memory.memory_usage` | DuckDB memory usage | By | - |
| `lakerunner.duckdb.memory.total_blocks` | DuckDB total blocks | 1 | - |
| `lakerunner.duckdb.memory.used_blocks` | DuckDB used blocks | 1 | - |
| `lakerunner.duckdb.memory.wal_size` | DuckDB WAL size | By | - |
| `lakerunner.exists` | Indicates if the service is running (1) or not (0) | - | - |

## Metric Groupings by Component

### File Reader Components

- `lakerunner.processing.input.filetype` - Track file types and whether input files are pre-sorted
- `lakerunner.reader.rows.*` - Track row processing through the reader stack

### S3 Operations

- `lakerunner.s3.*` - All S3 upload/download operations and error tracking

### Work Queue Management

- `lakerunner.inqueue.*` - Inqueue (ingestion queue) processing metrics
- `lakerunner.workqueue.*` - Work queue request handling and processing duration

### Processing Pipelines

- `lakerunner.processing.input.filetype` - File type and sorting status tracking across all processing pipelines
- `lakerunner.processing.segments.*` - Segment processing counters (in/out/filtered)
- `lakerunner.processing.records.*` - Record processing counters (in/out)
- `lakerunner.processing.bytes.*` - Byte processing counters (in/out)
- `lakerunner.pipeline.bufferpool.*` - Memory pool usage

### Background Maintenance

- `lakerunner.sweeper.*` - Cleanup and maintenance operations

### Database Operations

- `lakerunner.duckdb.memory.*` - DuckDB memory usage and configuration

### Service Health

- `lakerunner.exists` - Service availability indicator

## Service-Level Attributes

Each service sets global attributes during startup:

| Service | signal | action |
|---------|--------|---------|
| compact-logs | logs | compact |
| compact-metrics | metrics | compact |
| compact-traces | traces | compact |
| ingest-logs | logs | ingest |
| ingest-metrics | metrics | ingest |
| ingest-traces | traces | ingest |
| rollup-metrics | metrics | rollup |

These attributes are automatically added to relevant metrics via `commonAttributes` sets.
