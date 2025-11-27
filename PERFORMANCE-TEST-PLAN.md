# Log Ingestion & Compaction Performance Test Plan

## Overview

This plan identifies critical performance bottlenecks in log processing and defines focused tests to measure throughput and resource efficiency. All tests measure **logs/second** and **bytes/second per core** as primary metrics.

## Data Flow Summary

### Log Ingestion (cmd/ingest_logs.go)

1. **Download**: S3 → local temp files (logs_ingest_processor.go:189)
2. **Read Stack**: Create readers for downloaded files (logs_ingest_processor.go:199)
3. **Merge Sort**: Combine multiple readers with timestamp sorting (logs_ingest_processor.go:368)
4. **Binning**: Group by dateint + fingerprinting (logs_ingest_processor.go:228, 379-483)
5. **Write**: Parquet files in temp dir (logs_ingest_processor.go:448)
6. **Upload**: S3 upload + DB insert (logs_ingest_processor.go:238)
7. **Cleanup**: Remove temp files (logs_ingest_processor.go:150)

### Log Compaction (cmd/compact_logs.go)

1. **Download**: Multiple segments from S3 (readerstack_logs.go:70)
2. **Read Stack**: Parquet readers for each segment (readerstack_logs.go:103)
3. **Merge Sort**: Combine with timestamp sorting (readerstack_logs.go:119)
4. **Write**: Sorted output to temp Parquet files (writer_logs.go:67)
5. **Upload**: S3 upload + atomic DB update (log_compaction_processor.go:169, 188)
6. **Cleanup**: Remove old segments + temp files (log_compaction_processor.go:86)

## Critical Measurement Points

Each stage should measure:

- **Throughput**: logs/sec, bytes/sec
- **CPU**: per-core utilization
- **Memory**: peak RSS, allocations
- **I/O**: disk read/write bandwidth
- **Duration**: wall clock time per stage

## Component-Level Tests

### Test 1: S3 Download Performance

**Location**: `internal/cloudstorage/s3_client.go:28-31`

**What to measure**:

- Download bandwidth (MB/s per connection)
- Effect of file size (1MB, 10MB, 100MB, 500MB)
- Effect of parallel downloads (1, 2, 4, 8, 16 concurrent)

**Test setup**:

```go
// internal/cloudstorage/s3_client_bench_test.go
BenchmarkDownloadSingleFile-{1,2,4,8}cores
BenchmarkDownloadParallel-{2,4,8,16}files
```

**Success metrics**:

- Saturate network bandwidth (>500 MB/s on modern networks)
- Linear scaling up to network/CPU limit
- Baseline: bytes/sec per core

### Test 2: Parquet Reader Performance
**Location**: `internal/filereader/parquet_reader.go`

**What to measure**:
- Read throughput (logs/sec, bytes/sec)
- Memory per reader
- Effect of batch size (100, 1000, 5000, 10000)
- Effect of column projection (all columns vs subset)

**Test setup**:
```go
// internal/filereader/parquet_reader_bench_test.go
BenchmarkParquetReadSequential-{1,2,4,8}cores
BenchmarkParquetReadBatchSize-{100,1000,5000,10000}
```

**Success metrics**:
- >1M logs/sec single-threaded for simple schemas
- Memory proportional to batch size
- Baseline: logs/sec per core

### Test 3: Merge Sort Reader Performance
**Location**: `internal/filereader/mergesort_reader.go`

**What to measure**:
- Merge throughput with varying input count (2, 4, 8, 16, 32 readers)
- CPU cost of sorting vs passthrough
- Memory overhead per input stream

**Test setup**:
```go
// internal/filereader/mergesort_reader_bench_test.go
BenchmarkMergeSort-{2,4,8,16,32}inputs
BenchmarkMergeSortVsPassthrough
```

**Success metrics**:
- <20% overhead vs single reader for 4 inputs
- Log(N) complexity verification
- Baseline: logs/sec per input stream

### Test 4: Fingerprinting Performance
**Location**: `internal/oteltools/pkg/fingerprinter/`

**What to measure**:
- Fingerprint generation rate (fingerprints/sec)
- Memory for trie cluster manager
- Effect of message length (10B, 100B, 1KB, 10KB)
- Cache hit ratio impact

**Test setup**:
```go
// internal/fingerprint/fingerprint_bench_test.go
BenchmarkFingerprintGeneration-{1,2,4,8}cores
BenchmarkFingerprintByMessageSize-{10,100,1000,10000}bytes
```

**Success metrics**:
- >100K fingerprints/sec per core
- Linear scaling with cores
- Baseline: fingerprints/sec per core

### Test 5: Parquet Writer Performance
**Location**: `internal/parquetwriter/factories/logs_writer.go`

**What to measure**:
- Write throughput (logs/sec, bytes/sec)
- Compression ratio and CPU cost
- Effect of batch size
- Memory usage and GC pressure

**Test setup**:
```go
// internal/parquetwriter/factories/logs_writer_bench_test.go
BenchmarkLogsWriter-{1,2,4,8}cores
BenchmarkLogsWriterBatchSize-{100,1000,5000,10000}
BenchmarkLogsWriterCompression-{none,snappy,zstd}
```

**Success metrics**:
- >500K logs/sec per core
- >2:1 compression ratio with <30% CPU overhead
- Baseline: logs/sec per core

### Test 6: S3 Upload Performance
**Location**: `internal/cloudstorage/s3_client.go:34`

**What to measure**:
- Upload bandwidth (MB/s per connection)
- Effect of file size (1MB, 10MB, 100MB, 500MB)
- Effect of parallel uploads (1, 2, 4, 8, 16 concurrent)

**Test setup**:
```go
// internal/cloudstorage/s3_upload_bench_test.go
BenchmarkUploadSingleFile-{1,2,4,8}cores
BenchmarkUploadParallel-{2,4,8,16}files
```

**Success metrics**:
- Saturate network bandwidth
- Linear scaling up to network limit
- Baseline: bytes/sec per core

## Integration Tests

### Test 7: Download → Read Pipeline
**Combines**: Tests 1 + 2

**What to measure**:
- End-to-end throughput from S3 to parsed rows
- Whether download or read is bottleneck
- Effect of parallel download streams

**Test setup**:
```go
// internal/metricsprocessing/download_read_bench_test.go
BenchmarkDownloadAndRead-{1,2,4,8}files
```

**Success metrics**:
- Identify bottleneck (network vs CPU)
- Baseline: logs/sec end-to-end

### Test 8: Read → Fingerprint → Write Pipeline
**Combines**: Tests 2 + 4 + 5

**What to measure**:
- Processing throughput with all CPU-intensive steps
- Bottleneck identification (read/fingerprint/write)
- Memory profile across pipeline

**Test setup**:
```go
// internal/metricsprocessing/read_process_write_bench_test.go
BenchmarkReadFingerprintWrite-{1,2,4,8}cores
```

**Success metrics**:
- >200K logs/sec per core
- Identify dominant CPU consumer
- Baseline: logs/sec per core

### Test 9: Merge Sort + Write Pipeline
**Combines**: Tests 3 + 5

**What to measure**:
- Multi-stream merge with output
- Memory efficiency with many input streams
- Scalability to 16+ input files

**Test setup**:
```go
// internal/metricsprocessing/merge_write_bench_test.go
BenchmarkMergeAndWrite-{2,4,8,16,32}inputs
```

**Success metrics**:
- Linear scaling up to 8 inputs
- <2x memory overhead vs single stream
- Baseline: logs/sec per input

## End-to-End Tests

### Test 10: Full Ingestion Pipeline
**Components**: All ingestion stages (Download → Read → Fingerprint → Bin → Write → Upload)

**What to measure**:
- Total throughput (logs/sec for full pipeline)
- Time breakdown by stage (% of total time)
- Resource utilization (CPU, memory, disk, network)
- Scaling with input file count (1, 4, 8, 16 files)

**Test setup**:
```go
// internal/metricsprocessing/logs_ingest_e2e_bench_test.go
BenchmarkFullIngestion-{1,4,8,16}files
BenchmarkIngestionStageBreakdown
```

**Instrumentation**:
```go
type IngestionMetrics struct {
    DownloadDuration   time.Duration
    DownloadBytes      int64
    ReadDuration       time.Duration
    FingerprintDuration time.Duration
    WriteDuration      time.Duration
    UploadDuration     time.Duration
    TotalLogs          int64
    PeakMemoryMB       int64
}
```

**Success metrics**:
- >100K logs/sec end-to-end per core
- Identify slowest stage (target for optimization)
- Memory < 2GB for 100M logs
- Baseline: logs/sec per core for full pipeline

### Test 11: Full Compaction Pipeline
**Components**: All compaction stages (Download → Read → Merge → Write → Upload → DB)

**What to measure**:
- Total throughput (logs/sec for full pipeline)
- Time breakdown by stage
- Effect of input segment count (2, 4, 8, 16, 32 segments)
- Memory scaling with input count

**Test setup**:
```go
// internal/metricsprocessing/logs_compaction_e2e_bench_test.go
BenchmarkFullCompaction-{2,4,8,16,32}segments
BenchmarkCompactionStageBreakdown
```

**Instrumentation**:
```go
type CompactionMetrics struct {
    DownloadDuration   time.Duration
    MergeDuration      time.Duration
    WriteDuration      time.Duration
    UploadDuration     time.Duration
    DBUpdateDuration   time.Duration
    TotalLogs          int64
    InputSegments      int
    OutputFiles        int
    CompressionRatio   float64
}
```

**Success metrics**:
- >200K logs/sec end-to-end per core
- Sublinear memory growth with segment count
- Baseline: logs/sec per core for full compaction

## Resource Scaling Tests

### Test 12: CPU Scaling
**What to measure**:
- Throughput vs core count (1, 2, 4, 8, 16 cores)
- Parallel efficiency at each level

**Test setup**:
```go
// Run all benchmarks with GOMAXPROCS=1,2,4,8,16
BenchmarkE2EIngestion-{1,2,4,8,16}cores
BenchmarkE2ECompaction-{1,2,4,8,16}cores
```

**Success metrics**:
- >80% parallel efficiency up to 8 cores
- Identify serialization bottlenecks

### Test 13: Memory Scaling
**What to measure**:
- Peak memory vs input size (10MB, 100MB, 1GB, 10GB input)
- Memory efficiency (bytes RAM per log processed)

**Test setup**:
```go
BenchmarkMemoryScaling-{10MB,100MB,1GB,10GB}
```

**Success metrics**:
- Constant memory per log (not O(N))
- <5GB peak for 10GB input

## Test Data Requirements

### Real Production Data Sources

**IMPORTANT**: Performance tests use REAL data from a test account (no PII). Data is downloaded on-demand and NOT checked into the repository.

**Raw Input Data** (for ingestion tests):
- S3 Path: `s3://chq-saas-us-east-2-4d79e03f/otel-raw/65928f26-224b-4acb-8e57-9ee628164694/`
- Format: Raw OTEL logs (JSON.gz, Proto, or Parquet)

**Cooked Data** (for compaction tests):
- S3 Path: `s3://chq-saas-us-east-2-4d79e03f/db/65928f26-224b-4acb-8e57-9ee628164694/`
- Format: Optimized Parquet files

### Download Test Data

```bash
# Download 10 raw files for ingestion tests
./scripts/download-perf-testdata.sh raw 10

# Download 20 cooked files for compaction tests
./scripts/download-perf-testdata.sh cooked 20

# Set environment variable for tests
export PERFTEST_DATA_DIR=/tmp/lakerunner-perftest
```

### AWS Credentials Required

Tests need read-only S3 access. Set credentials via:
```bash
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
export AWS_REGION=us-east-2
```

### Ground Rules

1. **READ-ONLY**: Make NO changes to cloud infrastructure
2. **S3 ONLY**: Access ONLY S3, no other cloud components
3. **NO COMMITS**: Downloaded data must NOT be checked into repository

See `internal/perftest/TESTDATA.md` for complete details.

## Implementation Strategy

### Day 1 Plan (8 hours)
1. **Hour 1-2**: Component tests 1-3 (S3, Parquet, MergeSort)
2. **Hour 3-4**: Component tests 4-6 (Fingerprint, Writer, Upload)
3. **Hour 5-6**: Integration tests 7-9
4. **Hour 7-8**: E2E tests 10-11 + initial results

### Instrumentation Pattern
```go
type BenchmarkResult struct {
    LogsPerSec     float64
    BytesPerSec    float64
    LogsPerSecCore float64  // Primary metric
    CPUUtilization float64
    PeakMemoryMB   int64
    Duration       time.Duration
}

func (b *BenchmarkResult) Report() string {
    return fmt.Sprintf("%.0f logs/sec/core | %.2f MB/s | %.1f%% CPU | %d MB peak",
        b.LogsPerSecCore, b.BytesPerSec/1e6, b.CPUUtilization*100, b.PeakMemoryMB)
}
```

## Success Criteria

### Performance Targets
- **Ingestion**: >100K logs/sec/core end-to-end
- **Compaction**: >200K logs/sec/core end-to-end
- **Network**: Saturate available bandwidth (>500 MB/s)
- **Memory**: <5GB for 100M logs
- **Scalability**: >80% efficiency up to 8 cores

### Actionable Outputs
1. **Bottleneck identification**: Which stage dominates runtime?
2. **Resource recommendations**: Optimal core/memory config
3. **Optimization priorities**: Ranked by potential impact
4. **Scaling characteristics**: Where do we hit limits?

## Next Steps After Testing

Based on results, prioritize:
1. **If network-bound**: Parallel downloads, connection pooling
2. **If CPU-bound**: SIMD, better algorithms, parallel stages
3. **If memory-bound**: Streaming, smaller batches, better GC
4. **If I/O-bound**: Async I/O, buffering, tmpfs usage
