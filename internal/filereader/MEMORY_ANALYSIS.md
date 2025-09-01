# Memory Analysis Tests

This directory contains comprehensive memory analysis tests for the parquet readers that are excluded from regular test runs to avoid slowing down CI/CD pipelines.

## Running Memory Analysis Tests

To run the memory analysis tests, use the `memoryanalysis` build tag:

```bash
# Run all memory analysis tests
go test -tags=memoryanalysis ./internal/filereader/ -v

# Run specific memory analysis test
go test -tags=memoryanalysis -run=TestHighVolumeMemoryFootprint ./internal/filereader/ -v

# Run with timeout for long tests
go test -tags=memoryanalysis -run=TestDuckDBObjectCacheLongRun ./internal/filereader/ -v -timeout=10m
```

## Available Tests

### Core Memory Analysis
- `TestLinearMemoryGrowth` - Tests if memory leaks scale linearly (5 vs 25 iterations)
- `TestMemoryGrowthPerIteration` - Calculates per-iteration memory costs
- `TestHighVolumeMemoryFootprint` - 200-iteration test simulating millions of files

### Resource Cleanup Analysis
- `TestResourceCleanupValidation` - Validates proper resource cleanup between iterations
- `TestResourceCleanupWithExplicitDestructions` - Tests forced cleanup attempts
- `TestStabilizedMemoryMeasurement` - Uses extended stabilization periods

### DuckDB Analysis
- `TestDuckDBBatchingComparison` - Compares original vs batched DuckDB readers
- `TestDuckDBMemoryPatterns` - Tests different batch sizes (10, 50, 100, 500, 1000)
- `TestDuckDBvsArrowMemoryComparison` - Direct DuckDB vs Arrow comparison
- `TestDuckDBObjectCacheEffect` - Tests object cache effectiveness
- `TestDuckDBObjectCacheLongRun` - 50-iteration object cache analysis

### System Memory Profiling
- `TestSystemLevelMemoryBenchmark` - System-level RSS memory measurements
- `TestMemoryProfilingAccuracy` - Validates memory measurement accuracy

## Key Findings Summary

### Memory Efficiency Rankings (per row)
1. **Arrow Reader**: ~53 KB per row (best)
2. **DuckDB with Object Cache**: ~29-35 KB per row (competitive)
3. **ParquetRaw Reader**: ~140 KB per row (moderate)
4. **DuckDB without Object Cache**: ~101-393 KB per row (poor)

### Production Recommendations
- Use **Arrow Reader** for high-volume workloads (13.6 GB per million files)
- **DuckDB with object cache** viable for SQL capabilities (still ~7x more memory than Arrow)
- Avoid **ParquetRaw** for millions of files (136 GB per million files)

### Memory Leak Patterns
- Memory growth is **non-linear** due to caching and pooling
- DuckDB has significant query overhead but benefits from object cache
- Arrow shows minimal native memory leaks
- All readers have some native memory retention

## Implementation Notes

These tests are tagged with `//go:build memoryanalysis` to prevent them from running during:
- `make test`
- `go test ./...`
- CI/CD pipelines
- Regular development testing

The tests use system-level RSS memory measurements to capture native memory allocations from DuckDB and Arrow libraries that bypass Go's heap.