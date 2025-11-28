# Baseline Performance Results

Date: 2025-11-27
System: Apple M2 Pro, 10 cores, Darwin 25.1.0
Go Version: go1.25

## Test Data

- **Source**: Real production data from test account (S3)
- **Location**: `s3://chq-saas-us-east-2-4d79e03f/db/65928f26-224b-4acb-8e57-9ee628164694/`
- **Format**: DuckDB-generated Parquet files
- **Test File**: `tbl_7337773173775.parquet` (1.05 MB, 50,478 logs)
- **Local Path**: `/tmp/lakerunner-perftest/cooked/`

## Baseline Results

### 1. Parquet Read Performance

**Command**: `go test -bench=BenchmarkRealDataParquetRead -benchtime=3s`

**Results**:
- **Throughput**: 61,533 logs/sec total
- **Per-core**: 6,153 logs/sec/core
- **Bandwidth**: 1.28 MB/sec
- **Duration**: 856ms per iteration
- **Memory**: 119 MB peak, 292 MB total allocs
- **GC**: 7 collections

**Analysis**:
- Using `NewParquetRawReader` to read DuckDB-format Parquet files
- High memory allocation suggests room for optimization
- 7 GC cycles in <1 second indicates GC pressure
- Per-core throughput is the key metric for scaling

**Bottlenecks**:
- Memory allocations (292 MB for 1 MB file = 292x overhead)
- Frequent GC (7 cycles in 856ms)
- CPU-bound (reading compressed Parquet)

### 2. Pure Ingestion Performance (Raw OTEL → pipeline.Row)

**Command**: `go test -bench=BenchmarkPureIngestion -benchtime=3s -cpuprofile=/tmp/ingest-cpu.prof -memprofile=/tmp/ingest-mem.prof`

**Test Data**:
- **Source**: Raw OTEL protobuf files from S3 (`otel-raw/`)
- **Test File**: `logs_360985939.binpb.gz` (176 KB compressed)
- **Contents**: 3,036 logs, 143 KB decompressed (0.8x compression)

**Results**:
- **Throughput**: 66,243 logs/sec/core (single-core, GOMAXPROCS=1)
- **Bandwidth**: 3.14 MB/sec
- **Duration**: 45.8ms per iteration
- **Memory**: 22 MB peak, 38 MB total allocs
- **GC**: 6 collections

**Analysis**:
This benchmark isolates ONLY the cost of reading raw OTEL files and converting to pipeline.Row format. No writing, no processing, no DB operations. Forces single-core operation (GOMAXPROCS=1) to match production pattern where "we hit 1 core at max cpu + GC."

### CPU Profile Analysis (15.47s total samples)

**Top CPU Consumers** (by cumulative time):

1. **buildLogRow** - 45.05% (6.97s) `internal/filereader/ingest_proto_logs.go:150-174`
   - Iterating over OTEL attributes and converting to pipeline.Row
   - Map.Range on Resource.Attributes: 24.24% (3.76s)
   - Map.Range on LogRecord.Attributes: 19.59% (3.03s)
   - **Bottleneck**: Iterating Map structures is expensive

2. **prefixAttributeRowKey** - 20.30% (3.14s) `internal/filereader/otel_attributes.go:23-36`
   - String operations to transform attribute keys
   - strings.ReplaceAll: 9.50% (1.47s)
   - wkk.NewRowKey: 6.66% (1.03s)
   - **Bottleneck**: Creating new strings for every attribute key

3. **runtime.mallocgc** - 23.92% (3.70s)
   - Memory allocation overhead throughout
   - **Bottleneck**: High allocation rate triggers frequent GC

4. **Value.AsString** - 15.45% (2.39s)
   - Converting OTEL attribute values to strings
   - **Bottleneck**: Type conversions and string allocations

5. **GC Operations** - ~11% (1.70s)
   - runtime.kevent: 11.18% (1.73s)
   - runtime.scanobject: 2.97% (0.46s)
   - runtime.wbBufFlush1: 1.75% (0.27s)
   - **Bottleneck**: High allocation rate forces frequent GC cycles

**Runtime Operations**: 16.42% (2.54s) spent in runtime.madvise (memory management)

### Memory Profile Analysis (13.44 GB allocated total)

**Top Memory Allocators** (by alloc_space):

1. **io.ReadAll** - 32.84% (4.41 GB) `io/io.go:723`
   - Reading compressed files with growing buffer
   - `b = append(b, 0)[:len(b)]` reallocates repeatedly
   - **Bottleneck**: Buffer growth strategy causes excessive allocations

2. **buildLogRow operations** - 25.94% (3.49 GB)
   - Attribute iteration and row construction
   - Map.Range allocations
   - **Bottleneck**: High allocation rate from attribute processing

3. **prefixAttributeRowKey** - 14.66% (1.97 GB)
   - String building: prefix + "_" + strings.ReplaceAll(...)
   - Creating RowKey wrappers
   - **Bottleneck**: Each attribute name creates 2-3 string allocations

4. **Protobuf Unmarshaling** - 13.56% (1.82 GB)
   - AnyValue.Unmarshal: 5.54% (745 MB)
   - LogRecord.Unmarshal operations
   - KeyValue.Unmarshal operations
   - **Bottleneck**: Parsing OTEL protobuf format

5. **Value.AsString** - 18.01% (2.42 GB)
   - Converting attribute values to strings
   - **Bottleneck**: Type conversions allocate new strings

**Key Metrics**:
- **17.7 KB allocated per log** (13.44 GB / 774K logs processed across benchmark)
- **Memory overhead**: 123x (17.7 KB allocated for ~143 bytes decompressed per log)
- **Allocation rate**: 293 GB/sec during active processing

### Performance Comparison

| Metric | Pure Ingestion | Parquet Read | Target | Gap |
|--------|----------------|--------------|--------|-----|
| Logs/sec/core | 66,243 | 6,153 | 100,000 | 1.5x |
| Memory overhead | 123x | 292x | ~5x | 25x |
| GC pressure | 6 cycles | 7 cycles | <3 | 2x |

**Key Insight**: Pure ingestion is 10.8x faster than Parquet read (66K vs 6K logs/sec/core), suggesting the bottleneck is in Parquet reading, not OTEL parsing.

### Single-Core Processing Characteristics

Benchmark forces `GOMAXPROCS=1` to match production pattern:
- Production: "we hit 1 core at max cpu with processing + GC"
- Scaling strategy: Horizontal (more pods), not vertical (more cores per pod)
- Single-core constraint isolates per-pod performance ceiling

**Why Single-Core**:
1. Simpler concurrency model (no lock contention)
2. Predictable GC behavior (stop-the-world pauses affect one core)
3. Easier capacity planning (linear scaling with pods)
4. Better resource isolation in Kubernetes

### Optimization Opportunities

**High Impact** (>5% CPU or >1 GB memory):

1. **Reduce Map.Range allocations** (45% CPU, 3.49 GB memory)
   - Consider batch attribute extraction
   - Reuse Row objects from pool
   - Pre-allocate capacity for known attribute counts

2. **Optimize prefixAttributeRowKey** (20% CPU, 1.97 GB memory)
   - Cache transformed keys (e.g., LRU with 1000 entries)
   - Use strings.Builder for concatenation
   - Avoid strings.ReplaceAll for simple cases

3. **Fix io.ReadAll buffer growth** (32.84% memory)
   - Pre-allocate buffer based on compressed size estimate
   - Use io.LimitReader to avoid unbounded growth
   - Consider streaming decompression

4. **Reduce Value.AsString allocations** (15% CPU, 2.42 GB memory)
   - Keep values in OTEL format longer
   - Convert to string only when writing to Parquet
   - Use []byte instead of string where possible

**Medium Impact** (2-5% CPU or 500MB-1GB memory):

5. **Reduce protobuf unmarshaling overhead** (13.56% memory)
   - Use zero-copy unmarshaling where possible
   - Reuse unmarshal buffers
   - Consider protobuf.Unmarshal options

6. **Tune GC** (11% CPU)
   - Test GOGC=100 (default) vs current GOGC=50
   - May reduce GC frequency at cost of higher memory

### Next Steps

1. **Parquet Write Benchmark**: Measure write performance
2. **Merge Sort Benchmark**: Test multi-file merge performance
3. **Implement Quick Wins**: Cache prefixAttributeRowKey results, fix io.ReadAll
4. **GC Tuning**: Experiment with GOGC values (50, 100, 200)
5. **Profile with Optimizations**: Re-run after each major change

## Benchmark Command Reference

```bash
# Pure ingestion benchmark (raw OTEL → pipeline.Row)
go test -bench=BenchmarkPureIngestion -benchtime=3s -benchmem ./internal/perftest/

# With profiling
go test -bench=BenchmarkPureIngestion -benchtime=3s \
  -cpuprofile=/tmp/ingest-cpu.prof \
  -memprofile=/tmp/ingest-mem.prof \
  ./internal/perftest/

# Analyze CPU profile
go tool pprof -top -cum /tmp/ingest-cpu.prof
go tool pprof -list=buildLogRow /tmp/ingest-cpu.prof

# Analyze memory profile
go tool pprof -top -alloc_space /tmp/ingest-mem.prof
go tool pprof -list=buildLogRow /tmp/ingest-mem.prof

# Interactive flame graph
go tool pprof -http=:8080 /tmp/ingest-cpu.prof

# Parquet read benchmark
go test -bench=BenchmarkRealDataParquetRead -benchtime=3s -benchmem ./internal/perftest/

# Write benchmark
go test -bench=BenchmarkRealDataParquetWrite -benchtime=3s -benchmem ./internal/perftest/

# Merge sort benchmark
go test -bench=BenchmarkRealDataMergeSort -benchtime=1x -benchmem ./internal/perftest/

# All benchmarks
go test -bench=. -benchtime=3s -benchmem ./internal/perftest/
```

## Test Data Management

```bash
# Download raw OTEL test data (for pure ingestion benchmarks)
./scripts/download-perf-testdata.sh raw 10

# Download cooked Parquet test data (for read/write benchmarks)
./scripts/download-perf-testdata.sh cooked 10

# Check what's available
ls -lh /tmp/lakerunner-perftest/raw/
ls -lh /tmp/lakerunner-perftest/cooked/

# Inspect a Parquet file
./bin/lakerunner debug parquet cat --file /tmp/lakerunner-perftest/cooked/tbl_7337773173775.parquet | head -5

# Clean up
rm -rf /tmp/lakerunner-perftest/
```

## Performance Targets

Based on the plan in `PERFORMANCE-TEST-PLAN.md`:

- **Ingestion**: Target >100K logs/sec/core
  - Pure ingestion (OTEL → Row): 66K → **need 1.5x improvement**
  - With Parquet read: 6K → **need 16x improvement**
- **Compaction**: Target >200K logs/sec/core
- **Memory**: Target <5GB for 100M logs (currently ~17.7 KB per log = **need 25x improvement**)
- **Scalability**: Target >80% efficiency up to 8 cores

## Key Findings

1. **Pure ingestion is 10.8x faster than Parquet read** (66K vs 6K logs/sec/core)
   - Bottleneck is in Parquet reading/writing, not OTEL parsing
   - Pure ingestion already close to 100K target (1.5x gap)

2. **Memory overhead is massive across the board**
   - Pure ingestion: 123x overhead (17.7 KB allocated per log)
   - Parquet read: 292x overhead
   - Target is ~5x overhead for production use

3. **CPU dominated by attribute processing** (65% combined)
   - buildLogRow (Map.Range): 45% CPU, 3.49 GB memory
   - prefixAttributeRowKey: 20% CPU, 1.97 GB memory
   - High-impact optimization target

4. **GC pressure is significant** (11% CPU)
   - 293 GB/sec allocation rate during active processing
   - Forces frequent GC cycles (6-7 per second)
   - Single-core constraint makes GC pauses more impactful

5. **io.ReadAll wastes memory** (32.84% of allocations)
   - Buffer growth strategy causes excessive allocations
   - Quick win: pre-allocate based on compressed size

6. **DuckDB format compatibility** - cooked files need raw reader

## Action Items

### Completed
- [x] Profile memory allocations to find hot spots
- [x] Profile CPU to identify computational bottlenecks
- [x] Document single-core processing characteristics

### Next Steps
- [ ] Test with different GOGC values (50, 100, 200)
- [ ] Measure write performance baseline
- [ ] Measure merge sort performance with 2, 4, 8 files
- [ ] Implement quick wins (prefixAttributeRowKey cache, io.ReadAll pre-allocation)
- [ ] Profile with optimizations and compare
- [ ] Test scaling behavior with different core counts (1, 2, 4, 8 cores)
