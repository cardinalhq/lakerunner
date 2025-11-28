# Performance Testing Framework

## Overview

This package provides baseline measurement infrastructure for performance testing log ingestion and compaction pipelines.

## Quick Start

```bash
# 1. Download test data (requires AWS credentials)
./scripts/download-perf-testdata.sh raw 10
./scripts/download-perf-testdata.sh cooked 20

# 2. Set environment variable
export PERFTEST_DATA_DIR=/tmp/lakerunner-perftest

# 3. Run baseline benchmarks
cd internal/metricsprocessing
go test -bench=BenchmarkBaseline -benchtime=3s -benchmem
```

## Components

### Measurement Tools (`baseline.go`)

**Timer** - Basic performance measurement:
```go
timer := perftest.NewTimer()
// ... do work ...
timer.AddLogs(1000)
timer.AddBytes(50000)
metrics := timer.Stop()
fmt.Println(metrics.Report("My Test"))
```

**StageTimer** - Multi-stage pipeline measurement:
```go
st := perftest.NewStageTimer()

st.StartStage("download")
// ... download work ...
st.EndStage("download", logsProcessed, bytesProcessed)

st.StartStage("process")
// ... process work ...
st.EndStage("process", logsProcessed, bytesProcessed)

fmt.Println(st.StageReport())
```

**MemorySampler** - Background memory monitoring:
```go
timer := perftest.NewTimer()
sampler := perftest.NewMemorySampler(timer, 100*time.Millisecond)
sampler.Start()
defer sampler.Stop()

// ... do work ...
// Memory is sampled every 100ms automatically
```

### Test Data Sources

See `TESTDATA.md` for details on accessing real production-format test data.

## Baseline Benchmarks

Located in `internal/metricsprocessing/baseline_bench_test.go`:

- `BenchmarkBaselineParquetRead` - Parquet reading performance
- `BenchmarkBaselineParquetWrite` - Parquet writing performance
- `BenchmarkBaselineMergeSort` - Merge sort performance (4 inputs)
- `BenchmarkBaselineReadWritePipeline` - End-to-end read→write

Run with:
```bash
go test -bench=BenchmarkBaseline -benchtime=5s -benchmem -cpuprofile=cpu.prof
```

## Metrics Reported

All benchmarks report:
- **logs/sec** - Total throughput
- **logs/sec/core** - Per-core throughput (primary metric)
- **MB/sec** - Byte throughput
- **MB/sec/core** - Per-core byte throughput
- **Memory** - Start, peak, total allocations
- **GC** - Number of collections
- **Duration** - Wall clock time

## Example Output

```
BenchmarkBaselineParquetRead-10
Parquet Read Results:
  Duration:        1.234s
  Logs:            100000 (81037/sec, 8104/sec/core)
  Bytes:           5242880 (4.25 MB/s, 0.42 MB/s/core)
  Memory:          10 MB start, 45 MB peak, 128 MB allocs
  GC:              5 collections
  Cores:           10
```

## Adding New Benchmarks

1. Create benchmark function in `internal/metricsprocessing/`:
```go
func BenchmarkMyComponent(b *testing.B) {
    timer := perftest.NewTimer()
    sampler := perftest.NewMemorySampler(timer, 100*time.Millisecond)
    sampler.Start()
    defer sampler.Stop()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // ... your code ...
        timer.AddLogs(logsProcessed)
        timer.AddBytes(bytesProcessed)
    }
    b.StopTimer()

    metrics := timer.Stop()
    if b.N == 1 {
        b.Logf("\n%s", metrics.Report("My Component"))
    }
}
```

2. Report standard metrics:
```go
b.ReportMetric(float64(totalLogs)/b.Elapsed().Seconds(), "logs/sec")
b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1e6, "MB/sec")
```

## Best Practices

1. **Use real data**: Download from S3, don't use synthetic data
2. **Warm up**: Run benchmarks with `-benchtime=3s` minimum
3. **Profile**: Use `-cpuprofile` and `-memprofile` for analysis
4. **Isolate**: Run one benchmark at a time for accurate results
5. **Document**: Log detailed output on first iteration

## Next Steps

After establishing baseline measurements:
1. Identify bottlenecks from stage breakdowns
2. Profile hot paths with pprof
3. Target optimizations at slowest stages
4. Re-measure to validate improvements
5. Document performance characteristics

See `PERFORMANCE-TEST-PLAN.md` for the complete testing strategy.
