# Metric Rollup Performance Optimization

## Baseline Profile (2025-12-19)

**Environment:**
- RAM: 2GB
- GOMEMLIMIT: 1538MB
- GOGC: 50
- Profile duration: 30 seconds
- CPU utilization during profile: 84.31%

---

## CPU Profile Baseline

### Top Flat CPU Consumers

| Function | Flat | Flat % | Cum | Cum % |
|----------|------|--------|-----|-------|
| `runtime.memclrNoHeapPointers` | 2.11s | 8.31% | 2.11s | 8.31% |
| `runtime.scanobject` | 1.73s | 6.81% | 4.55s | 17.92% |
| `maps.ctrlGroup.matchH2` | 1.31s | 5.16% | 1.31s | 5.16% |
| `runtime.findObject` | 0.99s | 3.90% | 1.48s | 5.83% |
| `maps.(*Iter).Next` | 0.87s | 3.43% | 1.19s | 4.69% |
| `runtime.mapaccess1_fast64` | 0.82s | 3.23% | 1.44s | 5.67% |
| `unique.(*canonMap).Load` | 0.82s | 3.23% | 1.61s | 6.34% |
| `parquet-go.ReadRows` | 0.81s | 3.19% | 1.63s | 6.42% |
| `runtime.memmove` | 0.70s | 2.76% | 0.70s | 2.76% |
| `maps.h2` | 0.67s | 2.64% | 0.67s | 2.64% |
| `ArrowBackend.appendValue` | 0.61s | 2.40% | 1.14s | 4.49% |
| `aeshashbody` | 0.58s | 2.28% | 0.58s | 2.28% |

### Key Cumulative Hotspots

| Function | Cum | Cum % | Location |
|----------|-----|-------|----------|
| `processMetricsWithAggregation` | 20.74s | 81.69% | metricsprocessing |
| `writeFromReader` | 19.68s | 77.51% | metricsprocessing |
| `AggregatingMetricsReader.Next` | 13.34s | 52.54% | filereader |
| `readNextBatchFromUnderlying` | 13.07s | 51.48% | filereader |
| `CookedMetricTranslatingReader.Next` | 11.73s | 46.20% | filereader |
| `ParquetRawReader.Next` | 11.52s | 45.37% | filereader |
| `MergesortReader.Next` | 6.44s | 25.36% | filereader |
| `FileSplitter.WriteBatchRows` | 6.34s | 24.97% | parquetwriter |
| `ArrowBackend.WriteBatch` | 4.85s | 19.10% | parquetwriter |
| `scanobject` (GC) | 4.55s | 17.92% | runtime |
| `mallocgc` | 4.08s | 16.07% | runtime |
| `ArrowBackend.flushRecordBatch` | 3.48s | 13.71% | parquetwriter |
| `Schema.Reconstruct` | 3.10s | 12.21% | parquet-go |
| `normalizeRow` | 2.76s | 10.87% | filereader/schema.go |
| `zstd.Encoder.EncodeAll` | 2.40s | 9.45% | compress/zstd |
| `NewRowKeyFromBytes` | 2.26s | 8.90% | pipeline/wkk |

---

## Allocation Profile Baseline

**Total allocations during profile: ~2TB**

### Top Allocators (alloc_space)

| Function | Allocated | % | Notes |
|----------|-----------|---|-------|
| `zstd.(*fastBase).ensureHist` | 827 GB | 40.56% | Zstd history buffers |
| `zstd.encoderOptions.encoder` | 300 GB | 14.69% | Zstd encoder creation |
| `ParquetRawReader.Next` | 240 GB | 11.76% | Parquet reading |
| `arrow.GoAllocator.Allocate` | 194 GB | 9.52% | Arrow memory |
| `pqarrow.writeDenseArrow` | 84 GB | 4.13% | Arrow parquet writing |
| `bytes.growSlice` | 49 GB | 2.40% | Slice growth |
| `normalizeRow` | 47 GB | 2.29% | Row normalization |
| `parquet-go.bufferPool.newBuffer` | 47 GB | 2.29% | Parquet buffers |
| `byteArrayType.AssignValue` | 45 GB | 2.22% | Parquet string assign |
| `pipeline.init.func1` | 36 GB | 1.75% | Batch pool init |
| `zstd.(*blockEnc).init` | 35 GB | 1.72% | Zstd block encoding |
| `convertValue` | 31 GB | 1.54% | Value type conversion |
| `DDSketch.DecodeDDSketch` | 27 GB | 1.31% | Histogram decoding |

### Heap Profile (inuse_space at snapshot)

| Function | In Use | % |
|----------|--------|---|
| `parquet-go.bufferPool.newBuffer` | 65 MB | 41.40% |
| `arrow.GoAllocator.Allocate` | 47 MB | 30.09% |
| `fingerprinter/wordlist.init` | 11 MB | 7.17% |
| `reflect.mapassign_faststr` | 11 MB | 7.05% |
| `parquet-go.ReadRows` | 6 MB | 3.83% |

---

## Analysis Summary

### Problem #1: Zstd Compression (55%+ of allocations)

Zstd encoders allocate massive history buffers (~1GB each). Every parquet file write creates a new encoder.

**Location:** `backend_arrow.go:362-367`
```go
writerProps := parquet.NewWriterProperties(
    parquet.WithCompression(compress.Codecs.Zstd),
    ...
)
```

### Problem #2: Map Operations (~20% of CPU)

Rows are `map[RowKey]any`, causing heavy overhead:
- Map iteration in `normalizeRow`
- `columnPresence` map created every `WriteBatch` call
- Map access/assignment throughout pipeline

**Locations:**
- `schema.go:262-313` (normalizeRow)
- `backend_arrow.go:219-221` (columnPresence allocation)

### Problem #3: String Interning (8.90% CPU)

`NewRowKeyFromBytes` calls `unique.Make` for keys not in `commonKeys`.

**Location:** `wkk/intern.go:112-121`

### Problem #4: GC Pressure (17.92% in scanobject)

With 2TB allocations over 30s, GC is doing significant work despite GOGC=50.

---

## Optimization Opportunities (Priority Order)

1. **Pool zstd encoders** - Biggest win, 55% of allocations
2. **Expand `commonKeys`** - Add all metric rollup keys to fast path
3. **Reuse `columnPresence` map** - Avoid allocation per batch
4. **Optimize `normalizeRow`** - Pre-allocate or eliminate keysToDelete
5. **Consider struct-based rows** - Reduce map overhead for hot paths

---

## Progress Log

| Date | Change | CPU Impact | Memory Impact | Notes |
|------|--------|------------|---------------|-------|
| 2025-12-19 | Baseline | - | - | Initial measurement |
| 2025-12-19 | Pooled zstd codec | GC: 17.92%→10.66% | 2TB→50GB (40x) | `pooled_zstd.go` |

---

## Optimization #1: Pooled Zstd Codec (2025-12-19)

**File:** `internal/parquetwriter/pooled_zstd.go`

**Problem:** Arrow-go's default zstd codec creates a new encoder for every `EncodeLevel` and `NewWriterLevel` call, allocating ~1GB in history buffers each time.

**Solution:** Custom codec that pools encoders by compression level using `sync.Pool`. Encoders are returned to pool on `Close()`.

### Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Allocations | ~2TB | ~50GB | **40x reduction** |
| `zstd.ensureHist` | 827 GB | 3.8 GB | **99.5% reduction** |
| `zstd.encoderOptions.encoder` | 300 GB | 324 MB | **99.9% reduction** |
| CPU utilization | 84.31% | 75.92% | Less work |
| GC scanobject | 17.92% | 10.66% | **41% reduction** |
| memclrNoHeapPointers | 8.31% | 3.41% | **59% reduction** |

### New Top Allocators (after pooling)

| Function | Allocated | % |
|----------|-----------|---|
| `arrow.GoAllocator.Allocate` | 10.9 GB | 21.53% |
| `ParquetRawReader.Next` | 10.1 GB | 19.98% |
| `pqarrow.writeDenseArrow` | 5.0 GB | 9.90% |
| `zstd.ensureHist` | 3.8 GB | 7.55% |
| `normalizeRow` | 2.7 GB | 5.42% |

---

## Profile Files

- CPU (baseline): `/Users/mgraff/pprof/pprof.lakerunner.samples.cpu.009.pb.gz`
- CPU (pooled zstd): `/Users/mgraff/pprof/pprof.lakerunner.samples.cpu.010.pb.gz`
- Heap (baseline): `/Users/mgraff/pprof/pprof.lakerunner.alloc_objects.alloc_space.inuse_objects.inuse_space.029.pb.gz`
- Heap (pooled zstd): `/Users/mgraff/pprof/pprof.lakerunner.alloc_objects.alloc_space.inuse_objects.inuse_space.030.pb.gz`
