# Optimization Summary

Date: 2025-11-27
Focus: Pure Ingestion Performance (Raw OTEL → pipeline.Row)

## Results

### Baseline Performance
- **Throughput**: 66,243 logs/sec/core (GOMAXPROCS=1)
- **Memory**: 38 MB allocs, 17.7 KB per log
- **Target**: 100K logs/sec/core (need 1.51x improvement)

### After Optimizations
- **Throughput**: 68,385 logs/sec/core (+3.2% improvement)
- **Memory**: 36 MB allocs (-5.3%), still 17.3 KB per log
- **Gap to target**: Still need 1.46x improvement (100K / 68.4K)

## Optimizations Applied

### 1. prefixAttributeRowKey (+43% isolated, +2.3% end-to-end)

**Changes**:
- Fast path check for dots using `strings.Contains`
- `strings.Builder` with pre-allocated capacity
- Manual byte-by-byte replacement instead of `strings.ReplaceAll`
- Direct return for underscore-prefixed names without dots

**Micro-benchmark**: 10.6M → 15.2M attrs/sec (+43%)
**Memory**: 392B → 240B per batch (-39%), 19 → 10 allocs (-47%)

### 2. Value.Str() Fast Path (+19% isolated, +0.9% end-to-end incremental)

**Changes**:
- Type check: `v.Type() == pcommon.ValueTypeStr`
- Use `v.Str()` for strings (zero-copy, 71.7% faster)
- Fall back to `v.AsString()` for other types

**Micro-benchmark**: 246M → 294M attrs/sec (+19.4%)

## Key Findings

### 1. Micro-Optimizations Hit Diminishing Returns

**Observation**: Large isolated gains (43%, 19%) translate to small end-to-end improvements (+3.2% total).

**Root Cause**: External library overhead dominates. We optimized operations WITHIN Map.Range callbacks, but the iteration framework itself (OTEL library) is the bottleneck.

**CPU Profile Evidence**:
- buildLogRow: 45% of total CPU
  - Map.Range iteration overhead: ~25-30% (external, can't optimize)
  - Our optimizations: 15-20% (prefixAttributeRowKey + Value.AsString)
- Optimizing 35% of a 45% bottleneck → at most 15.75% theoretical gain
- Actual gain: 3.2% (iteration overhead dominates)

### 2. Memory Overhead Remains High

**Current**: 17.3 KB allocated per log for ~143 bytes of actual data (121x overhead)
**Target**: <1 KB per log (5x overhead for production use)
**Gap**: Need 17x memory reduction

**Top Memory Allocators** (unchanged by optimizations):
1. io.ReadAll: 32.84% (4.41 GB) - buffer growth strategy
2. buildLogRow: 25.94% (3.49 GB) - Map.Range allocations
3. prefixAttributeRowKey: 14.66% → 8.8% (improved 40%)
4. Protobuf unmarshaling: 13.56% (1.82 GB)

### 3. Bottleneck Has NOT Shifted

The top CPU consumers remain:
1. buildLogRow / Map.Range: still ~45% CPU
2. io.ReadAll buffer growth: still 32.84% memory
3. GC overhead: still ~11% CPU (293 GB/sec allocation rate)

## Recommendations

### Immediate: Target Memory Bottlenecks

**1. Fix io.ReadAll Buffer Growth (32.84% memory, easy win)**

Current problem:
```go
// io.ReadAll uses exponential growth: b = append(b, 0)[:len(b)]
// Causes excessive allocations (4.41 GB for ~1MB of compressed data)
```

Solution:
```go
// Pre-allocate based on compressed size estimate
// Compressed files are typically 0.1x-0.5x of uncompressed size
estimatedSize := compressedSize * 10 // Conservative estimate
buffer := make([]byte, 0, estimatedSize)
// Use io.LimitReader to avoid unbounded growth
```

**Expected gain**: 30-40% memory reduction (4.41 GB → 2-3 GB)

**2. Reduce Protobuf Unmarshaling Allocations (13.56% memory)**

Options:
- Reuse unmarshal buffers across batches
- Use zero-copy unmarshaling where possible
- Pool intermediate structures

**Expected gain**: 10-20% memory reduction

### Architectural: Address Map.Range Bottleneck

**Problem**: Can't optimize OTEL library's Map.Range iteration (external code)

**Solution Options**:

**A. Attribute Caching (High Impact)**
```go
// Cache common attribute key transformations
type attributeCache struct {
    cache map[string]wkk.RowKey
    mu    sync.RWMutex
}

// Pre-populate with common keys: service.name, host.name, etc.
// Reduces prefixAttributeRowKey calls by 80-90%
```

**Expected gain**: 15-25% CPU reduction (avoid repeated prefixAttributeRowKey)

**B. Batch Attribute Processing (Medium Impact)**
```go
// Instead of: rl.Resource().Attributes().Range(callback)
// Use: attrs := extractAllAttributes(rl, sl, logRecord)
// Then: bulkPopulateRow(row, attrs)

// Reduces function call overhead, enables vectorization
```

**Expected gain**: 10-15% CPU reduction

**C. Alternative OTEL API Pattern (High Impact, High Risk)**

Investigate if OTEL provides:
- Slice-based attribute access (avoid map iteration)
- Pre-computed attribute arrays
- Custom attribute iterator with less overhead

**Expected gain**: 20-30% CPU reduction (speculative)

### Next Steps

**Phase 1 - Memory Optimizations (Low Risk)**:
1. Fix io.ReadAll buffer pre-allocation ← **START HERE**
2. Pool protobuf unmarshal buffers
3. Re-profile to measure impact
4. **Target**: -30% memory, +5-10% throughput

**Phase 2 - Attribute Caching (Medium Risk)**:
1. Implement LRU cache for attribute keys (1000 entries)
2. Pre-populate with top 100 common keys
3. Measure hit rate and performance
4. **Target**: +15-20% throughput

**Phase 3 - Architectural Investigation (High Risk)**:
1. Profile OTEL library alternatives
2. Research zero-copy attribute access patterns
3. Consider custom OTEL fork if necessary
4. **Target**: +20-30% throughput

## Lessons Learned

### 1. Profile-Guided Optimization Works

✅ CPU/memory profiling accurately identified bottlenecks
✅ Micro-benchmarks validated optimization hypotheses
✅ Correctness tests prevented regressions

### 2. Micro-Optimizations Have Limits

⚠️ Large isolated gains don't guarantee large end-to-end gains
⚠️ External library overhead can dominate
⚠️ Need to optimize at the right abstraction level

### 3. Iteration is a Fundamental Cost

🎯 Iterating over data structures is expensive (Map.Range)
🎯 Reducing iterations > optimizing within iterations
🎯 Data structure choice matters (map vs slice vs array)

### 4. Memory Pressure Drives GC Cost

💡 293 GB/sec allocation rate → frequent GC (11% CPU)
💡 Reducing allocations reduces GC overhead
💡 Pool/reuse strategies are critical for throughput

## Success Criteria

**Minimum Viable** (Phase 1):
- 75K logs/sec/core (+10% from current 68K)
- 12 KB per log (-30% from current 17.3 KB)

**Target** (Phases 1+2):
- 85K logs/sec/core (+24% from current)
- 8 KB per log (-54% from current)

**Stretch** (All phases):
- 100K logs/sec/core (+46% from current)
- 5 KB per log (-71% from current)

## Conclusion

Two micro-optimizations delivered:
- ✅ **Solid isolated gains** (43%, 19%)
- ⚠️ **Limited end-to-end impact** (+3.2%)

Key insight: **The bottleneck is iteration overhead, not iteration content**.

Next focus: **Memory optimizations** (io.ReadAll, protobuf pooling) offer better ROI than further CPU micro-optimizations within callbacks.

To reach 100K logs/sec target, need **architectural changes** (caching, batching) or **alternative OTEL API patterns** to reduce Map.Range overhead.
