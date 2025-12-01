# Synthetic Benchmark Results: Arrow vs Go-Parquet

## Test Configuration

All tests run with production constraints:

- **GOGC=50** (GC when heap grows 50% instead of default 100%)
- **GOMAXPROCS=1** (single CPU core)
- **GOMEMLIMIT=2276 MB** (adjusted for pre-allocated data)
  - Base limit: 1538 MB
  - Pre-alloc overhead: 590 MB (measured) × 1.25 = 738 MB
  - Total: 2276 MB
- **Pre-allocated data**: 400k rows allocated once (590 MB RSS), each test uses first N rows
- **Metrics measure backend overhead only** (baseline subtracted)

## Performance Summary

| Rows   | Backend        | Throughput | Heap Δ (MB) | RSS Δ (MB) | Total Alloc | GC Runs | GC Pause | Output (KB) |
|--------|----------------|-----------|------------|-----------|-------------|---------|----------|-------------|
| 10k    | **go-parquet** | **97,359**| 371.4      | 56.3      | 841.8       | 4       | 0.17 ms  | 723         |
| 10k    | arrow          | 37,471    | 462.0      | 0         | 4,488       | 14      | 0.71 ms  | 698 (-3%)   |
| 25k    | **go-parquet** | **104,825**| 340.9     | 0.1       | 911.2       | 4       | 0.22 ms  | 1,801       |
| 25k    | arrow          | 32,360    | 486.6      | 0.3       | 6,628       | 21      | 0.99 ms  | 1,740 (-3%) |
| 50k    | **go-parquet** | **106,663**| 330.2     | 0.2       | 1,033       | 5       | 0.29 ms  | 3,596       |
| 50k    | arrow          | 38,608    | 472.4      | 0         | 5,594       | 17      | 0.97 ms  | 3,469 (-4%) |
| 100k   | **go-parquet** | **112,927**| 338.3     | 0.7       | 1,146       | 5       | 0.38 ms  | 7,192       |
| 100k   | arrow          | 45,652    | 1,138      | 668.7     | 11,184      | 24      | 1.39 ms  | 6,939 (-4%) |
| 200k   | **go-parquet** | **109,257**| 336.4     | 0.7       | 1,160       | 5       | 0.29 ms  | 14,385      |
| 200k   | arrow          | 62,472    | 1,785      | 720.7     | 22,366      | 34      | 2.18 ms  | 13,874 (-4%)|
| 400k   | **go-parquet** | **111,705**| 360.0     | 0.3       | 2,242       | 10      | 0.59 ms  | 28,764      |
| 400k   | arrow          | 68,240    | 1,928      | 44.1      | 44,729      | 59      | 3.94 ms  | 27,734 (-4%)|

## Key Findings

### 1. Go-Parquet Dominates Across All Batch Sizes

**Performance advantage**: Go-parquet is **39-224% faster** across all batch sizes:

- **10K rows**: go-parquet **160% faster** (97,359 vs 37,471 logs/sec)
- **25K rows**: go-parquet **224% faster** (104,825 vs 32,360 logs/sec)
- **50K rows**: go-parquet **176% faster** (106,663 vs 38,608 logs/sec)
- **100K rows**: go-parquet **147% faster** (112,927 vs 45,652 logs/sec)
- **200K rows**: go-parquet **75% faster** (109,257 vs 62,472 logs/sec)
- **400K rows**: go-parquet **64% faster** (111,705 vs 68,240 logs/sec)

**Conclusion**: After removing the CBOR intermediate codec, go-parquet consistently outperforms Arrow at all batch sizes. The direct-to-Parquet implementation achieves ~100K+ logs/sec throughput with minimal GC overhead.

### 2. Memory Overhead Comparison

#### Go-Parquet (Low Overhead, Consistent)

- **Heap Delta**: 330-371 MB across all sizes (very consistent)
- **RSS Delta**: 0.1-56.3 MB (minimal except initial 10K run)
- **Total Allocations**: 841-2,242 MB (2.1-5.6 MB per 1K rows)
- **GC Runs**: 4-10 (scales linearly)
- **GC Pause**: 0.17-0.59 ms (sub-millisecond)

#### Arrow (High Memory Churn)

- **Heap Delta**: 462-1,928 MB (1.2-5.4x more than go-parquet)
- **RSS Delta**: 0-720 MB (spikes at 100K-200K)
- **Total Allocations**: 4,488-44,729 MB (5.3-20.0x more than go-parquet!)
- **GC Runs**: 14-59 (3.5-5.9x more than go-parquet)
- **GC Pause**: 0.71-3.94 ms (4.2-6.7x longer than go-parquet)

**Why Go-Parquet Wins**:

1. **Minimal allocations**: 5-20x less memory churn than Arrow
2. **Predictable GC**: 4-10 GC runs vs Arrow's 14-59
3. **Sub-millisecond pauses**: 0.17-0.59ms vs Arrow's 0.71-3.94ms
4. **Consistent overhead**: Heap delta stays in 330-371 MB range regardless of batch size

### 3. Throughput Scaling

#### Go-Parquet (Stable, High Throughput)

- 10K → 25K: **+7.7%** throughput increase
- 25K → 50K: **+1.8%** (stable)
- 50K → 100K: **+5.9%** (continues scaling)
- 100K → 200K: **-3.2%** (slight dip)
- 200K → 400K: **+2.2%** (stable)

**Consistent ~97-113K logs/sec across all sizes** - excellent predictability. Peaks at 100K rows.

#### Arrow (Accelerates but Never Catches Up)

- 10K → 25K: **-13.6%** (slowdown, GC overhead dominates)
- 25K → 50K: **+19.3%** (recovery as batching benefits emerge)
- 50K → 100K: **+18.2%** (continues improving)
- 100K → 200K: **+36.8%** (columnar advantages emerge)
- 200K → 400K: **+9.2%** (continues scaling)

Arrow's performance **improves with batch size** - starts slow (37K logs/sec at 10K) but reaches **68K logs/sec at 400K** (+82% improvement). However, it never catches up to go-parquet's steady 100K+ logs/sec.

### 4. File Size Efficiency

Arrow produces **3-4% smaller output files** across all batch sizes:

- 10K: 698 KB vs 723 KB (-3.5%)
- 50K: 3,469 KB vs 3,596 KB (-3.5%)
- 400K: 27,734 KB vs 28,764 KB (-3.6%)

**However**: The 3-4% storage savings is vastly offset by:

- 64-224% slower processing time
- 5-20x more memory allocations
- 3.5-5.9x more GC runs
- 4.2-6.7x longer GC pauses

**Cost analysis**: At 400K rows, Arrow saves 1 MB (3.6%) but takes 85% longer to process (5.86s vs 3.58s). For S3 storage at $0.023/GB/month, 1 MB saves $0.000023/month while the extra CPU time costs far more.

### 5. 2x Speedup from Removing CBOR Codec

Comparing new direct-to-Parquet vs old CBOR-based implementation (go-parquet):

| Rows   | Old (CBOR) | New (Direct) | Speedup |
|--------|-----------|--------------|---------|
| 10k    | 49,030    | 97,359       | **+99%** |
| 25k    | 52,134    | 104,825      | **+101%** |
| 50k    | 52,636    | 106,663      | **+103%** |
| 100k   | 53,476    | 112,927      | **+111%** |
| 200k   | 54,988    | 109,257      | **+99%** |
| 400k   | 55,769    | 111,705      | **+100%** |

**Result**: Removing the CBOR intermediate codec doubled throughput across all batch sizes (~2x improvement).

## Production Recommendation

### Use Go-Parquet for All Workloads

Based on comprehensive benchmarking, **go-parquet is the clear winner** for all batch sizes:

✅ **Go-Parquet Benefits**:

- **64-224% faster** throughput across all batch sizes
- Consistent 97-113K logs/sec performance
- Minimal memory overhead (330-371 MB heap delta)
- Predictable, linear scaling
- 4-10 GC runs (vs Arrow's 14-59)
- Sub-millisecond GC pauses (0.17-0.59 ms)
- **Recommended for all production workloads**

❌ **Arrow Drawbacks**:

- 39-224% slower than go-parquet at all batch sizes
- High allocation overhead (5-20x more than go-parquet)
- 3.5-5.9x more GC runs
- 4.2-6.7x longer GC pauses
- Storage savings (3-4%) don't justify the performance cost
- **Not recommended for production use**

### Hardware Sizing

**For typical workloads (all batch sizes)**:

- **Backend**: go-parquet
- **RAM per worker**: 1.5-2 GB
- **Expected throughput**: ~100-113K logs/sec per core
- **Rationale**: Minimal overhead, predictable performance, 2x faster than old CBOR implementation

### Why Not Arrow?

Arrow's columnar approach creates more allocations than it saves in processing time. Even at 400K rows where Arrow performs best, it's still **64% slower** than go-parquet while using **5.4x more heap** and producing **20x more allocations**.

The 3-4% file size savings doesn't offset:

- 64-224% slower processing (higher CPU costs)
- 5-20x memory churn (higher GC overhead)
- Unpredictable RSS spikes (harder capacity planning)

## Benchmark Methodology

Data generated with synthetic OTEL log generator (`internal/perftest/synthetic_data.go`):

- **400k rows pre-allocated** at benchmark start
- Each test uses **first N batches** from pool (10 batches = 10K rows, etc.)
- **Baseline memory measured** before backend processing starts
- **Peak memory tracked** during backend processing
- **Metrics delta-calculated** to isolate backend overhead
- **40 columns** (realistic production schema):
  - Core fields: timestamp, message, level, fingerprint (always present)
  - Resource attrs: service, host, cloud, k8s metadata (always present)
  - Sparse attrs: HTTP (30%), DB (20%), errors (15%), user tracking (25%), metrics (40%), cache/queue (5%)
- **Realistic cardinality**: 5 services, 100K users, 10K hosts
- **Fixed seed (42)**: Reproducible results
- **Direct Parquet writes**: No intermediate CBOR codec (removed as of 2025)

This approach ensures:

1. Input data doesn't pollute memory measurements
2. Backend overhead is isolated and accurate
3. Tests compare apples-to-apples across all sizes
4. Results are reproducible
5. Performance baseline for future improvements

## Test Platform

- **Hardware**: Apple M2 Pro
- **OS**: macOS (Darwin 25.1.0)
- **Go Version**: 1.25.4
- **Date**: 2025-11-29
