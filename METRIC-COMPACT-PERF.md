# Metric Compact Performance Optimization

## Baseline Capture: 2025-12-20

### Environment
- **RAM**: 4GB
- **GOMEMLIMIT**: 3538MB
- **GOGC**: 200
- **Cores**: 2
- **Branch**: optimize-metric-compact (commit 2a50a445)

### Runtime Stats (at capture)
- HeapAlloc: 23.3MB
- HeapInuse: 32.5MB
- HeapSys: 1771.2MB
- NumGC: 1242
- PauseTotalNs: 0.09s

---

## CPU Profile (30s capture, 74.75% sample coverage)

### Top Functions by Cumulative Time

| Function | Flat | Flat% | Cum | Cum% |
|----------|------|-------|-----|------|
| ProcessBundle (entry) | 0 | 0% | 21.02s | 93.22% |
| performCompaction | 0 | 0% | 21.00s | 93.13% |
| writeFromReader | 0 | 0% | 19.63s | 87.05% |
| AggregatingMetricsReader.Next | 0.01s | 0.04% | 15.29s | 67.80% |
| readNextBatchFromUnderlying | 0.01s | 0.04% | 13.65s | 60.53% |
| CookedMetricTranslatingReader.Next | 0 | 0% | 12.86s | 57.03% |
| ParquetRawReader.Next | 0.16s | 0.71% | 12.61s | 55.92% |
| MergesortReader.Next | 0.05s | 0.22% | 6.34s | 28.12% |
| parquet-go GenericReader.Read | 0 | 0% | 4.72s | 20.93% |
| FileSplitter.WriteBatchRows | 0.23s | 1.02% | 4.34s | 19.25% |
| parquet-go Schema.Reconstruct | 0 | 0% | 3.36s | 14.90% |
| runtime.mapassign_fast64ptr | 0.58s | 2.57% | 2.81s | 12.46% |
| normalizeRow | 0.25s | 1.11% | 2.73s | 12.11% |
| wkk.NewRowKeyFromBytes | 0.05s | 0.22% | 2.69s | 11.93% |
| runtime.mallocgc | 0.21s | 0.93% | 2.53s | 11.22% |

### Top Functions by Flat Time (actual work)

| Function | Flat | Flat% |
|----------|------|-------|
| maps.ctrlGroup.matchH2 | 1.57s | 6.96% |
| maps.Iter.Next | 1.23s | 5.45% |
| runtime.memclrNoHeapPointers | 0.93s | 4.12% |
| unique.canonMap.Load | 0.79s | 3.50% |
| aeshashbody | 0.71s | 3.15% |
| rowGroupRows.ReadRows | 0.69s | 3.06% |
| maps.ctrlGroup.matchFull | 0.64s | 2.84% |
| runtime.mapassign_fast64ptr | 0.58s | 2.57% |
| runtime.scanobject | 0.54s | 2.39% |
| runtime.memmove | 0.45s | 2.00% |

### CPU Analysis Summary

**Map operations dominate**: ~30% of CPU spent in map iteration, assignment, and hashing
- `matchH2` + `Iter.Next` + `mapassign_fast64ptr` + `mapassign_faststr` = ~20% flat

**Parquet reading**: ~25% cumulative
- Schema reconstruction is expensive (14.90% cum)
- Row group reading significant

**Memory operations**: ~15% flat
- `memclrNoHeapPointers`, `memmove`, `mallocgc`

**GC pressure visible**: ~5% in scanobject

---

## Heap Profile (in-use memory)

Total: **176.12MB** in use

| Component | Size | % |
|-----------|------|---|
| parquet-go bufferPool.newBuffer | 60.23MB | 34.20% |
| Arrow GoAllocator.Allocate | 53.90MB | 30.61% |
| fingerprinter wordlist.init | 18.47MB | 10.49% |
| reflect.mapassign_faststr0 | 12.61MB | 7.16% |
| ParquetRawReader.Next | 11.56MB | 6.57% |
| rowGroupRows.ReadRows | 5.02MB | 2.85% |

### Heap Analysis Summary

- **Parquet read buffers**: 60MB resident (buffer pooling working but still significant)
- **Arrow write buffers**: 54MB resident
- **Static init cost**: 18.5MB in fingerprinter wordlist (unavoidable)
- **Map allocations**: 12.6MB in reflect map operations

---

## Allocation Profile (total bytes allocated)

Total: **275.09GB** allocated during 30s window (~9.2GB/s allocation rate)

### Top Allocators by Cumulative Bytes

| Function | Flat | Flat% | Cum | Cum% |
|----------|------|-------|-----|------|
| writeFromReader | 0 | 0% | 176.45GB | 64.14% |
| CookedMetricTranslatingReader.Next | 0 | 0% | 108.55GB | 39.46% |
| ParquetRawReader.Next | 21.29GB | 7.74% | 108.55GB | 39.46% |
| AggregatingMetricsReader.Next | 0 | 0% | 103.89GB | 37.77% |
| ArrowBackend.flushRecordBatch | 0 | 0% | 91.57GB | 33.29% |
| activeReader.advance | 0.50GB | 0.18% | 87.50GB | 31.81% |
| readNextBatchFromUnderlying | 0.12GB | 0.04% | 86.51GB | 31.45% |
| Arrow FileWriter.Write | 0 | 0% | 75.12GB | 27.31% |
| FileSplitter.WriteBatchRows | 0.13GB | 0.05% | 72.56GB | 26.38% |
| MergesortReader.Next | 13.44GB | 4.88% | 65.34GB | 23.75% |
| Arrow GoAllocator.Allocate | 65.31GB | 23.74% | 65.31GB | 23.74% |
| ArrowBackend.WriteBatch | 1.73GB | 0.63% | 60.08GB | 21.84% |
| writeDenseArrow | 32.23GB | 11.72% | 59.05GB | 21.47% |
| parquet-go GenericReader.Read | 0.03GB | 0.01% | 55.95GB | 20.34% |
| createMetricReaderStack | 0 | 0% | 51.19GB | 18.61% |
| NewMergesortReader/primeReaders | 0.50GB | 0.18% | 50.57GB | 18.38% |
| normalizeRow | 25.52GB | 9.28% | 33.91GB | 12.33% |
| Schema.Reconstruct | 0 | 0% | 26.68GB | 9.70% |

### Top Allocators by Flat Bytes

| Function | Flat | Flat% |
|----------|------|-------|
| Arrow GoAllocator.Allocate | 65.31GB | 23.74% |
| pqarrow.writeDenseArrow | 32.23GB | 11.72% |
| normalizeRow | 25.52GB | 9.28% |
| ParquetRawReader.Next | 21.29GB | 7.74% |
| MergesortReader.Next | 13.44GB | 4.88% |
| rowGroupRows.ReadRows | 9.64GB | 3.50% |

### Allocation Analysis Summary

**Critical insight**: 275GB allocated in 30s = massive churn

Top allocation sources:
1. **Arrow memory** (65GB flat, 24%): Write-side buffer allocations
2. **writeDenseArrow** (32GB flat, 12%): Parquet write encoding
3. **normalizeRow** (25GB flat, 9%): Row normalization creating new maps
4. **ParquetRawReader.Next** (21GB flat, 8%): Parquet read allocations
5. **MergesortReader.Next** (13GB flat, 5%): Merge sort row copying

---

## Key Optimization Opportunities

### 1. normalizeRow (25.52GB flat, 12.11% CPU cum) - OPTIMIZED
- ~~Creates new maps for each normalized row~~
- **Fixed**: Skip conversion when value already matches target type (avoids interface{} boxing)
- **Fixed**: Delete nulls during iteration instead of collecting keys first

### 2. MergesortReader.Next (13.44GB flat, 28% CPU cum) - OPTIMIZED
- ~~Row copying during merge operations~~
- **Fixed**: Cache `needsNormalize` flag instead of checking per-row
- **Fixed**: Add `ColumnCount()` to avoid allocating slice just to check length
- **Fixed**: Use `maps.Copy()` for row copying

### 3. Arrow Write Buffers (65GB + 32GB)
- Heavy allocation in write path
- Consider pre-allocated buffer pools

### 4. ParquetRawReader.Next (21GB flat)
- Per-row map allocations from parquet-go
- Consider batch processing or row pooling

### 5. Schema.Reconstruct (26.68GB cum)
- Repeated schema reconstruction
- Cache reconstructed schemas per file

### 6. Map Operations (~30% CPU)
- Heavy use of `map[string]interface{}`
- Consider struct-based rows or specialized containers

---

## Tracking

| Date | Change | CPU Impact | Alloc Impact | Notes |
|------|--------|------------|--------------|-------|
| 2025-12-20 | Baseline | - | 275GB/30s | Initial capture |
| 2025-12-20 | normalizeRow optimization | 42% faster | 100% fewer allocs | Skip type check when types match |
| 2025-12-20 | MergesortReader optimization | 11% faster | 32% fewer allocs, 75% less memory | Cache schema check, use maps.Copy |

### normalizeRow Benchmark Results

**Before:**
```
NoConversions:    766 ns/op    224 B/op    15 allocs/op
WithConversions:  777 ns/op    224 B/op    15 allocs/op
ManyNulls:        702 ns/op    224 B/op     5 allocs/op
BatchSim (1000):  768 µs/op    224 KB/op   15000 allocs/op
```

**After:**
```
NoConversions:    440 ns/op      0 B/op     0 allocs/op  (-42%, -100% allocs)
WithConversions:  468 ns/op      8 B/op     1 allocs/op  (-40%, -93% allocs)
ManyNulls:        541 ns/op      0 B/op     0 allocs/op  (-23%, -100% allocs)
BatchSim (1000):  443 µs/op      0 B/op     0 allocs/op  (-42%, -100% allocs)
```

**Changes made:**
1. Added `valueMatchesType()` to check if value already has correct type before converting
2. Removed `keysToDelete` slice - delete nulls directly during map iteration (Go 1.21+)

### MergesortReader Benchmark Results

**Before:**
```
SingleReader (1000 rows):  690 µs    85 KB    3111 allocs
TwoReaders (1000 rows):    693 µs    87 KB    3134 allocs
FiveReaders (1000 rows):   706 µs    88 KB    3177 allocs
```

**After:**
```
SingleReader (1000 rows):  614 µs    21 KB    2112 allocs  (-11%, -75% mem, -32% allocs)
TwoReaders (1000 rows):    620 µs    22 KB    2133 allocs  (-10%, -75% mem, -32% allocs)
FiveReaders (1000 rows):   632 µs    24 KB    2179 allocs  (-10%, -72% mem, -31% allocs)
```

**Changes made:**
1. Added `ColumnCount()` method to avoid allocating slice just to check if schema has columns
2. Cache `needsNormalize` flag at construction time instead of checking per-row
3. Use `maps.Copy()` instead of manual loop for row copying

---

## Profile Files

Profiles saved to:
- `/tmp/metric-compact-cpu.prof`
- `/tmp/metric-compact-heap.prof`
- `/tmp/metric-compact-allocs.prof`

To analyze:
```bash
go tool pprof -http=:8080 /tmp/metric-compact-cpu.prof
go tool pprof -http=:8080 /tmp/metric-compact-allocs.prof
```
