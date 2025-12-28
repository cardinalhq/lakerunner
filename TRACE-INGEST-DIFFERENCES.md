# Trace Ingestion Pipeline Comparison Report

This document compares the old (filereader/parquetwriter) and new (DuckDB) trace ingestion pipelines.

## Test Configuration

- Test file: `testdata/traces/otel-traces.binpb.gz`
- Records processed: 246 spans
- Platform: darwin/arm64 (Apple M1 Ultra)

## Performance Metrics (Benchmark Results)

| Metric | Old Pipeline | DuckDB Pipeline | Difference |
|--------|-------------|-----------------|------------|
| **Execution Time** | 19.7 ms/op | 44.6 ms/op | 2.26x slower |
| **Memory Allocated** | 77.27 MB/op | 0.44 MB/op | **174x less (99.4% reduction)** |
| **Allocation Count** | 79,899/op | 3,934/op | **20x fewer** |

## Memory Profile

| Metric | Old Pipeline | DuckDB Pipeline |
|--------|-------------|-----------------|
| Total Allocations | 97.53 MB | 0.44 MB |
| Heap In Use | 37.27 MB | 4.34 MB |

## Schema Comparison

| Metric | Old Pipeline | DuckDB Pipeline |
|--------|-------------|-----------------|
| Column Count | 91 | 94 |
| Common Columns | 83 | 83 |

### Columns only in old pipeline

- `_cardinalhq_bucket_prefix`
- `_cardinalhq_collector_id`
- `_cardinalhq_customer_id`
- `_cardinalhq_fingerprint`
- `_cardinalhq_span_duration`
- `attr_cardinalhq_stats_collector_id`
- `attr_cardinalhq_stats_collector_name`
- `chq_fingerprint` (pre-computed)

### Columns only in new pipeline

- `attr__cardinalhq_collector_id` (double underscore naming)
- `attr__cardinalhq_customer_id`
- `attr__cardinalhq_fingerprint`
- `attr__cardinalhq_span_duration`
- `chq_customer_id`
- `chq_telemetry_type`
- `resource__cardinalhq_bucket_prefix`
- `resource__cardinalhq_collector_id`
- `resource__cardinalhq_customer_id`
- `scope_name`
- `scope_version`

### Note on `duckdb_schema`

The comparison test shows `duckdb_schema` in the new pipeline's column list, but this is **not an actual data column**. It's a Parquet metadata entry that DuckDB embeds in files it writes to store schema information. The `parquet_schema()` function returns this internal metadata alongside actual columns. Using `DESCRIBE` instead shows 94 actual data columns (no `duckdb_schema`).

## Correctness Verification

- Row count: **MATCH** (246 rows in both outputs)
- Both pipelines produce valid Parquet output

## Key Advantages of DuckDB Pipeline

1. **Massive memory reduction (99.4%)** - Critical for large files and concurrent processing
2. **Schema flexibility** - DuckDB automatically handles schema merging across files with different attributes
3. **Simpler codebase** - SQL-based transformations instead of Go data structures
4. **Native Parquet support** - Efficient COPY TO with ZSTD compression

## Trade-offs

1. **Latency overhead** - DuckDB startup and extension loading adds ~25ms overhead per invocation
2. **Column naming differences** - Some internal columns use different naming conventions

## Recommendations

The DuckDB pipeline is recommended for production use due to:

- Dramatically lower memory footprint enables processing larger files
- Better scalability for concurrent workers
- Schema merging handles heterogeneous input files automatically

The latency overhead is acceptable for batch processing workloads where memory efficiency is critical.

## Running the Comparison Test

```bash
# Run the comparison test
go test -v -run TestTraceIngestPipelineComparison ./internal/metricsprocessing/

# Run benchmarks
go test -bench=BenchmarkTraceIngestPipelines -benchmem -run=^$ ./internal/metricsprocessing/
```
