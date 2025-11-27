# Optimization Journal

Date: 2025-11-27
System: Apple M2 Pro, 10 cores, Darwin 25.1.0
Go Version: go1.25

## Baseline Performance

**Pure Ingestion (Raw OTEL → pipeline.Row)**:
- **Throughput**: 66,243 logs/sec/core (GOMAXPROCS=1)
- **Memory**: 17.7 KB allocated per log (123x overhead)
- **Target**: 100K logs/sec/core, <1 KB per log (5x overhead)

## Top Bottlenecks Identified

1. **prefixAttributeRowKey** - 20% CPU, 1.97 GB memory
2. **buildLogRow (Map.Range)** - 45% CPU, 3.49 GB memory
3. **Value.AsString** - 15% CPU, 2.42 GB memory
4. **io.ReadAll** - 32.84% memory (4.41 GB)

## Optimization Attempts

### 1. prefixAttributeRowKey

**Target**: 10%+ improvement in CPU or memory

**Baseline** (as-is):
```
BenchmarkPrefixAttributeRowKey/short_no_dots      52.80 ns/op   24 B/op   1 allocs/op
BenchmarkPrefixAttributeRowKey/medium_one_dot     83.31 ns/op   40 B/op   2 allocs/op
BenchmarkPrefixAttributeRowKey/long_many_dots    114.7  ns/op   80 B/op   2 allocs/op
BenchmarkPrefixAttributeRowKey_Batch             941.5  ns/op  392 B/op  19 allocs/op  (10.6M attrs/sec)
```

**Analysis**:
- 2 allocations per call (strings.ReplaceAll + final string concatenation)
- Batch test: 39.2 bytes/attr average (392 B / 10 attrs)
- ReplaceAll allocates even when there are no dots to replace
- String concatenation (prefix + "_" + ...) allocates

**Optimization Attempt #1**: Use strings.Builder and avoid ReplaceAll when no dots

**Changes**:
1. Added fast path: `strings.Contains` to check if dots exist before attempting replacement
2. Used `strings.Builder` with pre-allocated capacity for concatenation
3. Manual byte-by-byte replacement instead of `strings.ReplaceAll` (avoids allocation)
4. Return name directly if underscore-prefixed and no dots

**Results** (Optimization #1):
```
BenchmarkPrefixAttributeRowKey/short_no_dots      42.02 ns/op   24 B/op   1 allocs/op  (-20.4% time)
BenchmarkPrefixAttributeRowKey/medium_one_dot     64.72 ns/op   24 B/op   1 allocs/op  (-22.3% time, -40% mem, -50% allocs)
BenchmarkPrefixAttributeRowKey/long_many_dots     95.44 ns/op   48 B/op   1 allocs/op  (-16.8% time, -40% mem, -50% allocs)
BenchmarkPrefixAttributeRowKey_Batch             658.9  ns/op  240 B/op  10 allocs/op  (+43% throughput, -38.8% mem, -47.4% allocs)
```

**Analysis**:
- ✅ **43% throughput improvement** (10.6M → 15.2M attrs/sec) - SIGNIFICANT
- ✅ **30% faster** for batch workload (941.5 ns → 658.9 ns)
- ✅ **39% less memory** per log (392 B → 240 B for 10 attrs)
- ✅ **47% fewer allocations** (19 → 10 allocs for 10 attrs)
- ✅ Individual improvements: 17-32% faster, 40% less memory for attrs with dots

**Impact on pure ingestion**:
- Baseline: 66,243 logs/sec/core, 38 MB allocs
- After opt: 67,754 logs/sec/core, 36 MB allocs
- **+2.3% throughput, -5.3% memory**

**Analysis**: Micro-benchmark showed 43% gain, but only 2.3% end-to-end improvement. This indicates prefixAttributeRowKey is not the dominant bottleneck. buildLogRow (45% CPU) is the main target.

**Verdict**: ✅ ACCEPT - Isolated component shows 43% gain (exceeds target), but need to optimize larger bottlenecks for end-to-end impact

---

### 2. buildLogRow Attribute Iteration

**Target**: 10%+ improvement in CPU or memory

**Baseline**: TBD (micro-benchmark needed)

**Changes**: TBD

**Results**: TBD

---

### 3. Value.AsString Conversions (buildLogRow optimization)

**Target**: 10%+ improvement in CPU or memory

**Baseline**:
```
String value AsString():     2.052 ns/op   0 B/op   0 allocs/op
String value Str() direct:   0.581 ns/op   0 B/op   0 allocs/op  (-71.7% time)
Int value AsString():       17.40  ns/op   5 B/op   1 allocs/op

Realistic (90% strings, 10% other types):
  baseline_AsString:        40.62 ns/op   246M attrs/sec   4 B/op   1 allocs/op
  optimized_fast_path:      34.02 ns/op   294M attrs/sec   4 B/op   1 allocs/op
```

**Analysis**:
- Str() is 71.7% faster than AsString() for string values
- Realistic workload: +19.4% throughput (246M → 294M attrs/sec)
- Most OTEL attributes are strings (~90% in production)

**Optimization Attempt #1**: Use Value.Str() fast path for string types

**Changes**:
1. Check `v.Type() == pcommon.ValueTypeStr` before conversion
2. Use `v.Str()` for string values (zero-copy)
3. Fall back to `v.AsString()` for int, bool, float types
4. Applied to all three Map.Range iterations in buildLogRow

**Results** (Optimization #1):
```
Micro-benchmark: +19.4% throughput (246M → 294M attrs/sec)
End-to-end:      +3.2% total (66,243 → 68,385 logs/sec/core)
```

**Analysis**:
- ✅ Micro-benchmark shows 19.4% improvement
- ❌ Only +0.9% incremental improvement on top of prefixAttributeRowKey optimization
- ❌ Combined optimizations: only +3.2% end-to-end (below 10% target)

**Root Cause**: Map.Range iteration itself dominates CPU time. The OTEL library's map iteration overhead is the real bottleneck, not what we do inside the callback. We're optimizing 15-20% of CPU within a 45% bottleneck, but the iteration framework itself is immovable.

**Verdict**: ⚠️ ACCEPT micro-optimization (19% isolated gain), but recognize we need architectural change to see significant end-to-end improvement. Can't optimize external library code (Map.Range).

---

### 4. io.ReadAll Buffer Growth

**Target**: 10%+ improvement in memory

**Baseline**: TBD (micro-benchmark needed)

**Changes**: TBD

**Results**: TBD

---

## Summary of Gains

**After both optimizations** (prefixAttributeRowKey + Value.Str() fast path):
- **End-to-end**: +3.2% throughput (66,243 → 68,385 logs/sec/core), -5.3% memory
- **Micro-benchmarks**: +43% and +19% in isolation for individual components

**Breakdown**:
1. prefixAttributeRowKey: +43% isolated, +2.3% end-to-end
2. Value.Str() fast path: +19.4% isolated, +0.9% end-to-end incremental

**Key learnings**:
1. ✅ Micro-optimizations work well in isolation (43%, 19% gains)
2. ❌ Small end-to-end impact (+3.2% total) - below 10% target
3. 🎯 **Root cause**: Map.Range iteration overhead in OTEL library (external, can't optimize)
4. 💡 **Insight**: Optimizing WITHIN expensive operations has limited impact when the operation itself is expensive

**Bottleneck Analysis**:
- buildLogRow was 45% CPU, we optimized components within it (20% + 15% = 35% of that 45%)
- But Map.Range iteration itself (external library) is the dominant cost
- Our optimizations made the callback cheaper, but iterations still expensive

**Path Forward**:
Since micro-optimizations hit diminishing returns, need **architectural changes**:
1. Reduce number of attribute iterations (batch processing, caching)
2. Avoid Map.Range by using different OTEL API patterns
3. Move to next bottleneck: io.ReadAll buffer growth (32.84% memory)
4. Re-profile to see if bottleneck has shifted after optimizations
