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

## Completed Optimizations

| Change | Impact | Commit |
| ------ | ------ | ------ |
| Pool FNV hasher in ComputeTID | -88ms (4%), zero allocs | `a615f54b` |
| Zero-copy row transfer in TranslatingReader | -57ms (3%) | `4cf385e7` |
| String interning + embed byteReader in SpillCodec | -57ms (3%), -11M allocs (28%), -311MB (22%) | `291941e7` |
| **Total** | 2263ms → 1991ms (**12% faster**), 38.5M → 27.7M allocs | |

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

### P3: Specific Pooling Opportunities

**Problem**: 38.5M allocations cause ~20% CPU overhead from GC.

| Target | Memory | File | Fix |
| ------ | ------ | ---- | --- |
| ~~SpillCodec.readString~~ | ~~191MB~~ | ~~`pipeline/rowcodec/spillcodec.go`~~ | ✅ Done: string interning |
| ~~SpillCodec.readUvarint~~ | ~~97MB~~ | ~~`pipeline/rowcodec/spillcodec.go`~~ | ✅ Done: embedded byteReader |
| DDSketch.extendRange | 163MB | `vendor/github.com/DataDog/sketches-go` | Pre-size sketch or pool instances |
| SpillCodec.writeString | 148MB | `pipeline/rowcodec/spillcodec.go:675` | Pool write buffer |
| addNumberDatapointFields | 35MB | `internal/oteltools/pkg/decoder/` | Refactor closure to method |
| strings.Builder.grow | 34MB | Various | Pool `strings.Builder` instances |

#### SpillCodec Buffer Pooling

```go
// Current: allocates new buffer per string read
func (c *SpillCodec) readString() (string, error) {
    buf := make([]byte, length)  // ALLOCATION
    // ...
    return string(buf), nil       // ANOTHER ALLOCATION
}

// Fix: pool buffer, use unsafe.String
var bufPool = sync.Pool{New: func() any { return make([]byte, 0, 256) }}

func (c *SpillCodec) readString() (string, error) {
    buf := bufPool.Get().([]byte)[:0]
    buf = append(buf, ...)
    s := unsafe.String(unsafe.SliceData(buf), len(buf))
    // Note: must copy if string escapes, or use string interning
}
```

#### byteReader Embedding

```go
// Current: creates new byteReader per varint
func (c *SpillCodec) readUvarint() (uint64, error) {
    br := byteReader{r: c.r}  // ALLOCATION (escapes)
    return binary.ReadUvarint(&br)
}

// Fix: embed in struct
type SpillCodec struct {
    r        io.Reader
    br       byteReader  // reused
}

func (c *SpillCodec) readUvarint() (uint64, error) {
    c.br.r = c.r
    return binary.ReadUvarint(&c.br)
}

---

## Implementation Order

1. **Memory-based sorting threshold** (P0) - Biggest single impact (~38% time reduction)
2. **SpillCodec buffer pooling** (P3) - Pool readString/writeString buffers
3. **byteReader embedding** (P3) - Eliminate readUvarint allocations
4. **DDSketch pre-sizing** (P3) - Reduce extendRange allocations

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
