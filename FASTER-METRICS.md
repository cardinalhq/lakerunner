# Metric Ingestion Performance Optimization Plan

## Current Measurements (Apple M2 Pro, 100K unique metrics, ~53MB uncompressed)

### With Lazy In-Memory Sorting (New)

| Stage | Time (ms) | Delta (ms) | % of Total | Allocs | Memory |
| ------- | ----------- | ------------ | ------------ | -------- | -------- |
| 1-4. SortingReader (lazy) | 1,284 | 1,284 | 91.3% | 17.3M | 798MB |
| 5. +Aggregation | 1,407 | 123 | 8.7% | 20.4M | 919MB |

*Lazy loading: stores only indices (~56 bytes/datapoint) during sort, builds rows on demand.*

### With Disk Sorting (Previous)

| Stage | Time (ms) | Delta (ms) | % of Total | Allocs | Memory |
| ------- | ----------- | ------------ | ------------ | -------- | -------- |
| 1. GzipRead | 27 | 27 | 1.4% | 232 | 315MB |
| 2. ProtoReader | 550 | 523 | 27.9% | 14.9M | 752MB |
| 3. +Translation | 1,039 | 489 | 26.2% | 15.3M | 763MB |
| 4. +DiskSort | 1,865 | 826 | 44.2% | 23.6M | 924MB |
| 5. +Aggregation | 1,971 | 106 | 5.7% | 26.7M | 1,036MB |

## Original Baseline (for reference)

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
| DDCache page-based storage + open-addressed map | -25% memory overhead per cached sketch | `d3c33b01` |
| Lazy in-memory sorting (replaces DiskSortingReader) | -566ms (29%), -6.3M allocs (24%), -113MB (11% less) | `pending` |
| **Total** | 2263ms → 1407ms (**38% faster**), 38.5M → 20.4M allocs (**47% fewer**), 1387MB → 919MB (**34% less**) | |

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

### ~~P0: Disk Sorting I/O (29% CPU, 38% wall time)~~ ✅ DONE

**Problem**: Every row is written to disk, sorted, then read back. For 100K rows, this is 200K disk operations.

**Solution**: `SortingIngestProtoMetricsReader` with lazy row building:
- Eliminates disk I/O entirely
- Saves ~566ms (29% of total time)
- Stores only indices (~56 bytes/datapoint) during sort, not full rows
- Actually uses **11% less memory** than disk sort (no row materialization during sort phase)
- `ComputeTIDFromOTEL()` computes TID directly from OTEL structures
- File: `internal/filereader/ingest_proto_metrics_sorting.go`

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

1. ~~**Memory-based sorting threshold** (P0)~~ ✅ Done - In-memory sorting saves ~550ms
2. **SpillCodec buffer pooling** (P3) - Pool readString/writeString buffers
3. ~~**byteReader embedding** (P3)~~ ✅ Done - Embedded in SpillCodec
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

| Metric | Original | Current | Target | Progress |
| -------- | --------- | --------- | -------- | ------- |
| Single file time | 2,263ms | **1,407ms** | <800ms | 38% faster |
| Allocations | 38.5M | **20.4M** | <12M | 47% fewer |
| Peak memory | 1,387MB | **919MB** | <500MB | 34% less |
| Throughput | 44K rows/s | **71K rows/s** | 125K rows/s | 61% faster |
