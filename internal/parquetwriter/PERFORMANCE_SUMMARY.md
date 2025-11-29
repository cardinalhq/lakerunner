# Parquet Writer Performance Summary

## TL;DR - Major Findings

**Go-Parquet Direct-to-Parquet (Production Default as of 2025)**:
- **2x faster** than previous CBOR-based implementation (100-113K vs 50-56K logs/sec)
- **64-224% faster** than Arrow backend across all batch sizes
- **Minimal memory overhead** - 330-371 MB heap delta (very consistent)
- **Predictable GC** - 4-10 GC runs with sub-millisecond pauses (0.17-0.59ms)
- **Consistent throughput** - 97-113K logs/sec across all batch sizes
- **Production ready** - identical output quality (ZSTD + dictionary encoding)

**Key Decision**: Removing the CBOR intermediate codec and writing directly to Parquet
doubled throughput. Go-parquet now significantly outperforms Arrow at all batch sizes.

**Performance Comparison (vs Arrow)**:
- **10K rows**: go-parquet **160% faster** (97,359 vs 37,471 logs/sec)
- **100K rows**: go-parquet **147% faster** (112,927 vs 45,652 logs/sec)
- **400K rows**: go-parquet **64% faster** (111,705 vs 68,240 logs/sec)

**Memory Comparison**:
- Go-parquet: 330-371 MB heap delta, 4-10 GC runs
- Arrow: 462-1,928 MB heap delta, 14-59 GC runs (5-20x more allocations)

**Production Recommendation**: Use go-parquet (via `NewUnifiedWriter`) for all workloads.

---

## Implementation Architecture

### Current Production Implementation (Go-Parquet Direct)

The production `UnifiedWriter` uses `FileSplitter` which writes directly to Parquet
using `github.com/parquet-go/parquet-go`:

1. **No intermediate format**: Rows are written directly to Parquet files
2. **Streaming writes**: Memory is released as data is flushed to disk
3. **Schema upfront**: Complete schema must be provided at creation (from reader)
4. **String conversion**: Configured prefixes (`resource_`, `scope_`, `attr_`) are
   automatically converted to strings to avoid type conflicts
5. **File splitting**: Automatic splitting based on `RecordsPerFile` with optional
   `NoSplitGroups` to keep related data together

### Backend Comparison Tests

Both backends (go-parquet and arrow) are tested to ensure identical output:
- Row-by-row comparison using DuckDB to read back parquet files
- String conversion verification for all configured prefixes
- Large dataset testing (1000+ rows)
- All tests pass - backends produce identical output

The backend abstraction (`BackendArrow`, `BackendGoParquet`) is maintained for
testing and benchmarking purposes only. Production code uses `NewUnifiedWriter`
which directly uses go-parquet.

---

## Performance History

### 2025: Direct-to-Parquet (Current)

**Removed CBOR intermediate codec**, achieving 2x speedup:

| Rows   | Old (CBOR) | New (Direct) | Speedup |
|--------|-----------|--------------|---------|
| 10k    | 49,030    | 97,359       | +99%    |
| 100k   | 53,476    | 112,927      | +111%   |
| 400k   | 55,769    | 111,705      | +100%   |

**Result**: Consistently ~2x faster throughput across all batch sizes.

### Pre-2025: CBOR Intermediate Format

Original implementation used CBOR (Concise Binary Object Representation) as an
intermediate format:
1. Buffer rows to CBOR file
2. Read CBOR file back
3. Convert to Parquet

This double-pass approach limited throughput to ~50-56K logs/sec.

---

## Current Benchmark Results (2025)

See `SYNTHETIC_BENCHMARK.md` for detailed benchmark results showing:
- Go-parquet: 97-113K logs/sec (consistent across batch sizes)
- Arrow: 32-68K logs/sec (improves with batch size but never catches up)
- Memory: Go-parquet uses 5-20x less allocations than Arrow
- GC: Go-parquet has 3.5-5.9x fewer GC runs than Arrow

---

## Production Deployment

### Current Production Stack

1. **Writer**: `NewUnifiedWriter(config)` via factories
   - `NewLogsWriter()` - logs data (no grouping)
   - `NewMetricsWriter()` - metrics data (grouped by metric name + TID)
   - `NewTracesWriter()` - trace data (grouped appropriately)

2. **Backend**: Go-parquet (via `FileSplitter`)
   - Direct writes to Parquet files
   - Schema provided upfront from reader
   - String conversion for configured prefixes
   - Automatic file splitting with group support

3. **Performance**: 100-113K logs/sec per core
   - Minimal memory overhead (330-371 MB heap delta)
   - Sub-millisecond GC pauses (0.17-0.59ms)
   - Predictable, consistent throughput

### Hardware Sizing

**Recommended per worker**:
- **RAM**: 1.5-2 GB per core
- **CPU**: 1 core = ~100-113K logs/sec throughput
- **Storage**: Temporary directory with adequate space for Parquet staging

### Configuration

```go
config := parquetwriter.WriterConfig{
    TmpDir:         "/tmp/parquet",
    Schema:         readerSchema, // From filereader
    RecordsPerFile: 50000,        // Split at 50K rows
    NoSplitGroups:  true,         // For metrics (keep groups together)
    GroupKeyFunc:   groupFunc,    // For metrics (group by name+TID)
    StatsProvider:  statsProvider, // For metadata collection
}

writer, err := parquetwriter.NewUnifiedWriter(config)
```

---

## Why Not Arrow?

Arrow's columnar approach creates more allocations than it saves in processing time:

**Performance**: 39-224% slower than go-parquet at all batch sizes
**Memory**: 5-20x more allocations, 3.5-5.9x more GC runs
**File size**: Only 3-4% smaller (doesn't justify performance cost)
**Cost analysis**: At 400K rows, Arrow saves 1 MB but takes 85% longer to process

The 3-4% storage savings on S3 ($0.000023/month per file) doesn't offset the
significantly higher CPU costs from slower processing.

---

## Allocator Testing (Historical)

### Default Go Allocator (Production Choice) ✅

This is what the current implementation uses with go-parquet:
- **Throughput**: 97-113K logs/sec (measured with synthetic benchmark)
- **Memory**: 330-371 MB heap delta (consistent)
- **GC**: 4-10 runs with 0.17-0.59ms pauses

### Arrow with CGO Allocator (Rejected)

Tested CGO-based Arrow allocators but found:
- **13% slower** throughput (CGO call overhead)
- **Minimal memory benefit** (90% still on Go heap)
- **More complexity** (CGO dependencies, build requirements)

The default Go allocator with go-parquet provides the best performance.

---

## Testing Strategy

### Unit Tests
- Backend comparison tests verify identical output
- String conversion tests verify consistency
- File splitting tests verify grouping logic

### Benchmarks
- Synthetic data benchmarks (various row counts)
- Real OTEL data benchmarks
- Chunk size comparisons
- Memory profiling

### Performance Baseline

Current benchmarks in `SYNTHETIC_BENCHMARK.md` serve as the baseline for future
improvements. Any changes should maintain or improve:
- Throughput: ≥100K logs/sec
- Memory: ≤400 MB heap delta
- GC: ≤10 runs with <1ms pauses

---

## Future Optimization Opportunities

1. **Schema caching**: Reuse parquet schema objects across writers
2. **Buffer pool**: Reuse internal buffers across writes
3. **Parallel encoding**: Encode multiple row groups concurrently
4. **Adaptive splitting**: Dynamic `RecordsPerFile` based on memory pressure

Current performance (100-113K logs/sec) is already excellent, but these
optimizations could push it even higher.
