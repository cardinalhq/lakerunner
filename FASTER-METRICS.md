# Metric Ingestion Performance Optimization Plan

## Baseline Measurements (Apple M2 Pro, 100K unique metrics, ~53MB uncompressed)

| Stage | Time (ms) | Delta (ms) | % of Total | Allocs | Memory |
| ------- | ----------- | ------------ | ------------ | -------- | -------- |
| 1. GzipRead | 27 | 27 | 1.2% | 232 | 315MB |
| 2. ProtoReader | 604 | 577 | 26.7% | 15.9M | 794MB |
| 3. +Translation | 1,275 | 671 | 29.7% | 17.1M | 924MB |
| 4. +DiskSort | 2,124 | 849 | 37.5% | 35.4M | 1,267MB |
| 5. +Aggregation | 2,263 | 139 | 6.1% | 38.5M | 1,387MB |

**Input**: 100,000 metric datapoints (100K unique TIDs)
**Output**: 100,000 aggregated rows (no reduction - all unique)

## CPU Profile Hotspots

| Function | CPU % | Cumulative % | Issue |
| ---------- | ------- | -------------- | ------- |
| syscall.syscall6 | 21.6% | 21.6% | Disk sort file I/O |
| runtime.madvise | 16.0% | 37.6% | Memory pressure |
| syscall.syscall | 7.3% | 44.9% | More disk I/O |
| runtime.gcDrain | 0.7% | 20.3% | GC drain |
| fingerprinter.ComputeTID | 1.2% | 13.8% | TID hashing per row |
| runtime.scanobject | 3.7% | 16.6% | GC scanning |

**~29% of CPU time is in disk I/O syscalls** from DiskSortingReader
**~20% of CPU time is in GC** due to high allocation rate

## Memory Profile Hotspots (3.91GB total)

| Function | Memory % | GB | Issue |
| ---------- | ---------- | ---- | ------- |
| io.ReadAll | 22.4% | 0.88 | Proto data loading |
| NumberDataPoint.Unmarshal | 14.9% | 0.58 | Proto parsing |
| fingerprinter.ComputeTID | 9.3% | 0.36 | Hash allocations |
| SpillCodec.readString | 6.9% | 0.27 | Disk sort read-back |
| SpillCodec.readUvarint | 6.0% | 0.24 | Disk sort read-back |
| SpillCodec.DecodeRowFrom | 5.5% | 0.21 | Row decoding |
| DDSketch.extendRange | 3.4% | 0.13 | Sketch storage growth |

**Total: 3.91GB allocated for processing 53MB input (74x amplification)**

---

## Optimization Opportunities (Prioritized)

### P0: Disk Sorting I/O (29% CPU, 38% wall time)

**Problem**: Every row is written to disk, sorted, then read back. For 100K rows, this is 200K disk operations.

**Solutions**:

1. **Memory-based sorting for small inputs** - If input fits in memory threshold (e.g., <50MB), sort in memory instead of disk.
   - **File**: `internal/filereader/disk_sorting_reader.go`
   - **Impact**: Eliminate ~850ms (38% of pipeline time)

2. **Buffered disk writes** - Increase buffer size from 64KB to 1MB+
   - Current: `bufio.NewWriterSize(tempFile, 64*1024)`
   - **Impact**: Reduce syscall count by 16x

3. **mmap temp file** - Use memory-mapped I/O instead of read/write syscalls
   - **Impact**: Kernel-managed buffering, fewer syscalls

### P2: TID Computation (9.3% memory, 14% CPU cumulative)

### P3: GC Pressure (20% CPU cumulative)

**Problem**: 38.5M allocations cause significant GC overhead.

**Solutions**:

1. **Object pooling** - Pool Row maps, DDSketch instances, byte buffers
   - Already have batch pooling, extend to more objects
   - **Impact**: Reduce GC time by 50%+

2. **Arena allocation** - Batch allocate strings from arena
   - Reduces individual allocations

### P4: SpillCodec Allocations (19% memory)

**Problem**: Every string/varint value is newly allocated on read-back from disk sort.

**Solutions**:

1. **String interning** - Intern common strings (metric names, label keys)
   - **File**: `pipeline/rowcodec/spill_codec.go`
   - **Impact**: ~30% memory reduction for repeated strings

2. **Reuse read buffers** - Pool byte slices for varint/string reading
   - **Impact**: Reduce allocations

### P5: Translation Row Copying (30% wall time)

**Problem**: `TranslatingReader.Next()` copies every row to output batch.

**Solutions**:

1. **In-place translation** - Modify rows in place instead of copying
   - **File**: `internal/filereader/translating.go:87-105`
   - Current: Creates new row, copies all keys
   - Better: Take ownership of input row, modify in place

2. **Zero-copy batch transfer** - Use `TakeRow()` pattern
   - Already used in AggregatingMetricsReader

---

## Implementation Order

1. **Memory-based sorting threshold** - Biggest single impact (~38% time reduction)
2. **Pool FNV hashers for TID** - Simple change, ~10% CPU reduction
3. **In-place translation** - Moderate complexity, ~15% time reduction
4. **String interning in SpillCodec** - Moderate complexity, ~19% memory reduction
5. **Object pooling for GC reduction** - Broad impact, ~20% CPU reduction

## Benchmark Commands

```bash
# Run all metric ingest benchmarks
go test -v -run=XXX -bench='BenchmarkMetricIngest' -benchtime=3s -benchmem ./internal/metricsprocessing/

# Run with CPU profile
go test -run=XXX -bench='BenchmarkMetricIngest_5_FullPipeline' -benchtime=5s -cpuprofile=/tmp/cpu.prof ./internal/metricsprocessing/
go tool pprof -http=:8080 /tmp/cpu.prof

# Run with memory profile
go test -run=XXX -bench='BenchmarkMetricIngest_5_FullPipeline' -benchtime=3s -memprofile=/tmp/mem.prof ./internal/metricsprocessing/
go tool pprof -http=:8080 /tmp/mem.prof

# Analyze cardinality of test file
go test -v -run=TestMetricFileCardinality ./internal/metricsprocessing/
```

## Success Metrics

| Metric | Current | Target | Notes |
| -------- | --------- | -------- | ------- |
| Single file time | 2,263ms | <800ms | 3x improvement |
| Memory per file | 1,387MB | <500MB | 3x reduction |
| Allocations | 38.5M | <12M | 3x reduction |
| Throughput | 44K rows/s | 125K rows/s | 3x improvement |
