# Reader Schema Migration Plan

## Overview

This document tracks the migration of all readers to provide schema information upfront, eliminating mid-write schema changes in Parquet files. The burden of schema knowledge is placed on the source readers, which either provide it cheaply from metadata (Parquet) or by scanning the data once (OTEL formats).

## Design Principles

1. **Schema First**: Every reader must provide its complete schema before emitting any rows
2. **Type Consistency**: Once declared, a column's type cannot change
3. **Null Tracking**: Track "all null" vs "at least one non-null" for each column
4. **Type Promotion**: Aggregate readers (merge, serial) promote types when merging schemas (e.g., int64 + string → string)
5. **Transform Awareness**: Schema accounts for all transformations (fingerprint addition, column drops, etc.)
6. **Validation**: Optional runtime check comparing declared schema to emitted rows

## Schema Structure

```go
type ColumnSchema struct {
    Name        wkk.RowKey  // Interned string
    DataType    DataType    // Enum: String, Int64, Float64, Bool, Bytes, Any
    HasNonNull  bool        // true = at least one non-null value seen/expected
}

type ReaderSchema struct {
    Columns []ColumnSchema
}

// DataType values:
// - String: UTF-8 strings
// - Int64: 64-bit integers
// - Float64: 64-bit floating point
// - Bool: boolean values
// - Bytes: byte arrays
// - Any: complex types (lists, structs, maps) passed through as-is
```

## Reader interface changes

type SchemafiedReader interface {
  Reader
  SchemaReader
}

type SchemaReader interface {
  // whatever function(s) we need here to return the content's schema
}

As we begin to convert the individual readers, we will change them to return a SchemafiedReader.

**Important Invariants:**
- OK: Declare `HasNonNull=true` but emit no non-null values
- **NOT OK**: Declare `HasNonNull=false` then emit non-null values
- **NOT OK**: Declare type as Int64 then emit String values

---

## Reader Inventory

### 1. Base/Raw Readers (Read from Files)

#### 1.1 ParquetRawReader
- **File**: `internal/filereader/parquet_raw_reader.go`
- **Transformations**:
  - Converts dotted field names to underscores
  - Applies schema normalization for type consistency
- **Schema Source**: ✅ **Metadata** - Parquet file header contains complete schema
- **Schema Extraction**: Walks parquet schema tree, handles Groups and leaf nodes, maps parquet types to DataType
- **Used In**:
  - ❌ Ingestion: No (uses ArrowRawReader or Proto readers)
  - ✅ Compaction: Metrics, Traces (reads cooked Parquet)
  - ✅ Rollup: Metrics
- **Status**: ✅ Schema Interface | ✅ Tested

---

#### 1.2 ArrowRawReader
- **File**: `internal/filereader/arrow_raw_reader.go`
- **Transformations**:
  - Converts Arrow types to Go types
  - Handles NULL-type columns (skips them)
  - Preserves dotted field names (unlike ParquetRawReader)
  - Applies schema normalization for type consistency
- **Schema Source**: ✅ **Metadata** - Arrow schema from Parquet metadata
- **Schema Extraction**: Extracts from Arrow schema fields, maps Arrow types to DataType, uses DataTypeAny for complex types
- **Used In**:
  - ✅ Ingestion: Logs (handles NULL columns from OTel collector)
  - ✅ Compaction: Logs
  - ❌ Rollup: No
- **Notes**: Preferred for logs due to NULL-type column handling. Complex types (lists, structs, maps) use DataTypeAny and pass through as-is.
- **Status**: ✅ Schema Interface | ✅ Tested

---

#### 1.3 CSVReader
- **File**: `internal/filereader/csv_reader.go`
- **Transformations**:
  - Parses CSV header to determine column names
  - All values initially treated as strings
  - Attempts type inference per column (int64, float64, bool, or string)
- **Schema Source**: ⚠️ **Scan Required** - Must read header + sample rows to infer types
- **Used In**:
  - ✅ Ingestion: Logs (with CSVLogTranslator wrapper)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Notes**: Could optimize by reading only header + first N rows for schema
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 1.4 JSONLinesReader
- **File**: `internal/filereader/reader.go` (inline implementation)
- **Transformations**:
  - Parses JSON objects
  - Flattens nested structures
  - Type inference per value
- **Schema Source**: ⚠️ **Full Scan Required** - Must read all records to discover all columns
- **Used In**:
  - ✅ Ingestion: Logs, Metrics, Traces (via ReaderForFileWithOptions)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Notes**: Schema can vary per row; need to scan all rows to find all possible columns
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 1.5 IngestProtoLogsReader
- **File**: `internal/filereader/ingest_proto_logs.go`
- **Transformations**:
  - Converts OTEL LogRecord protobuf to Row
  - Extracts resource attributes, log attributes, body
  - Flattens hierarchical structure
  - Adds `_timestamp`, `_tsns`, `_message`, `_level` fields
  - Attribute names prefixed with `resource_` or `log_`
- **Schema Source**: ⚠️ **Scan Required** - Must read all records to discover all attributes
- **Used In**:
  - ✅ Ingestion: Logs (for .binpb files)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Notes**: Already sorts data during read; can extract schema during sort pass
- **Status**: ✅ Schema Interface | ✅ Tested

---

#### 1.6 IngestProtoMetricsReader
- **File**: `internal/filereader/ingest_proto_metrics.go`
- **Transformations**:
  - Converts OTEL Metric protobuf to multiple Rows (one per datapoint)
  - Extracts resource attributes, metric labels
  - Flattens Sum, Gauge, Histogram, Summary metrics
  - Adds `metric_name`, `metric_type`, `_value`, `_timestamp` fields
  - Attribute/label names prefixed with `resource_` or `metric_`
- **Schema Source**: ⚠️ **Scan Required** - Must read all metrics to discover all attributes/labels
- **Used In**:
  - ✅ Ingestion: Metrics (for .binpb files)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Notes**: Eager loads entire file into memory already
- **Status**: ✅ Schema Interface | ✅ Tested

---

#### 1.7 IngestProtoTracesReader
- **File**: `internal/filereader/ingest_proto_traces.go`
- **Transformations**:
  - Converts OTEL Span protobuf to Row
  - Extracts resource attributes, span attributes, events
  - Adds `trace_id`, `span_id`, `parent_span_id`, `_timestamp` fields
  - Attribute names prefixed with `resource_` or `span_`
  - Computes `chq_fingerprint` from span name
- **Schema Source**: ⚠️ **Scan Required** - Must read all spans to discover all attributes
- **Used In**:
  - ✅ Ingestion: Traces (for .binpb files)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Notes**: Already sorts data during read; can extract schema during sort pass
- **Status**: ✅ Schema Interface | ✅ Tested

---

### 2. Translator/Wrapper Readers

#### 2.1 TranslatingReader
- **File**: `internal/filereader/translating.go`
- **Transformations**: Applies RowTranslator to each row (caller-defined transformations)
- **Schema Source**: 🔄 **Depends on Translator** - Must merge:
  1. Wrapped reader's schema
  2. Columns added by translator
  3. Columns removed by translator
- **Used In**:
  - ✅ Ingestion: All types (wraps base readers with signal-specific translators)
  - ❌ Compaction: No
  - ❌ Rollup: No
- **Translator Types**:
  - `LogTranslator`: Adds `resource_bucket_name`, `resource_file_name`, etc.
  - `ParquetLogTranslator`: Minimal transforms for pre-cooked logs
  - `CSVLogTranslator`: Adds log-specific fields to CSV data
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 2.2 CookedLogTranslatingReader
- **File**: `internal/filereader/cooked_log_translating_reader.go`
- **Transformations**:
  - Drops rows missing `_timestamp`
  - Ensures `chq_fingerprint` is int64 (converts from string/bytes if needed)
  - Converts `log_message` bytes to string
  - Adds `_tsns` if missing (derived from `_timestamp`)
- **Schema Source**: 🔄 **From Wrapped Reader** + Transform tracking
- **Used In**:
  - ✅ Ingestion: No
  - ✅ Compaction: Logs (wraps ParquetRawReader/ArrowRawReader)
  - ❌ Rollup: No
- **Schema Changes**: May add `_tsns` column
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 2.3 CookedMetricTranslatingReader
- **File**: `internal/filereader/cooked_metric_translating_reader.go`
- **Transformations**:
  - Drops rows missing `_timestamp` or `metric_name`
  - Ensures `_value` is float64
  - Adds `_tsns` if missing
  - Drops rows with invalid timestamps
- **Schema Source**: 🔄 **From Wrapped Reader** + Transform tracking
- **Used In**:
  - ✅ Ingestion: No
  - ✅ Compaction: Metrics
  - ✅ Rollup: Metrics
- **Schema Changes**: May add `_tsns` column
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 2.4 CookedTraceTranslatingReader
- **File**: `internal/filereader/cooked_trace_translating_reader.go`
- **Transformations**:
  - Drops rows missing `_timestamp` or `trace_id`
  - Ensures `chq_fingerprint` is int64
  - Adds `_tsns` if missing
- **Schema Source**: 🔄 **From Wrapped Reader** + Transform tracking
- **Used In**:
  - ✅ Ingestion: No
  - ✅ Compaction: Traces
  - ❌ Rollup: No
- **Schema Changes**: May add `_tsns` column
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

### 3. Aggregate Readers

#### 3.1 MergesortReader
- **File**: `internal/filereader/mergesort_reader.go`
- **Transformations**:
  - Merges N sorted readers into single sorted stream
  - **No row transformations** (pass-through)
- **Schema Source**: 🔄 **Merge Child Schemas**
  - Union all column names from child readers
  - **Type Promotion Rules**:
    - If all children have same type for column → use that type
    - If types differ → promote to most general (int64 + string → string)
    - Track `HasNonNull = any child has non-null`
- **Used In**:
  - ✅ Ingestion: Logs, Metrics, Traces (multi-file ingestion)
  - ✅ Compaction: Logs, Metrics, Traces (multi-segment reads)
  - ✅ Rollup: Metrics
- **Critical for Schema**: This is where type promotion happens!
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 3.2 SequentialReader
- **File**: `internal/filereader/sequential_reader.go`
- **Transformations**: Concatenates N readers sequentially (exhaust R1, then R2, ...)
- **Schema Source**: 🔄 **Merge Child Schemas** (same rules as MergesortReader)
- **Used In**:
  - ❌ Ingestion: No
  - ❌ Compaction: No
  - ❌ Rollup: No
  - (Experimental/testing only)
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 3.3 DiskSortingReader
- **File**: `internal/filereader/disk_sorting_reader.go`
- **Transformations**:
  - Sorts wrapped reader by provided key
  - Spills to disk if needed
  - **No schema changes** (pass-through wrapped reader's schema)
- **Schema Source**: 🔄 **From Wrapped Reader**
- **Used In**:
  - ✅ Ingestion: No
  - ✅ Compaction: Metrics (if `sort_version` mismatch)
  - ❌ Rollup: No
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

### 4. Processing Readers

#### 4.1 AggregatingMetricsReader
- **File**: `internal/filereader/aggregating_metrics_reader.go`
- **Transformations**:
  - Aggregates metric datapoints by (name, labels, time_bucket)
  - Sums values within aggregation window
  - **Reduces row count** (many → few)
  - Drops some attribute columns (those not part of aggregation key)
- **Schema Source**: 🔄 **Filtered Schema**
  - Start with wrapped reader's schema
  - Remove columns not in aggregation key
  - Mark output `_value` as always non-null
- **Used In**:
  - ✅ Ingestion: Metrics (optional, via `EnableAggregation`)
  - ✅ Compaction: Metrics
  - ✅ Rollup: Metrics
- **Schema Impact**: Significant - removes columns not in aggregation key
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

#### 4.2 MetricFilteringReader
- **File**: `internal/filereader/metric_filtering_reader.go`
- **Transformations**:
  - Filters metrics by metric name (allowlist/denylist)
  - Drops entire rows that don't match filters
  - **No schema changes** (columns unchanged, just fewer rows)
- **Schema Source**: 🔄 **From Wrapped Reader** (pass-through)
- **Used In**:
  - ✅ Ingestion: Metrics (optional)
  - ✅ Compaction: Metrics (optional)
  - ❌ Rollup: No
- **Status**: ⬜ Schema Interface | ⬜ Tested

---

### 5. Experimental Readers (Not in Production)

#### 5.1 DuckDBParquetReader
- **File**: `internal/filereader/experimental/duckdb_parquet_reader.go`
- **Status**: Experimental - Not used in production

#### 5.2 ArrowCookedReader
- **File**: `internal/filereader/experimental/arrow_cooked_reader.go`
- **Status**: Experimental - Not used in production

---

## Reader Stacks by Pipeline

### Log Ingestion (Raw → Cooked)
```
[.binpb file]
  → IngestProtoLogsReader (OTEL → Row, adds resource_*, log_*, _timestamp, _message, _level)
[.json.gz / .json file]
  → JSONLinesReader (JSON → Row, type inference)
[.csv file]
  → CSVReader (CSV → Row, type inference)
    → TranslatingReader(CSVLogTranslator) (adds log-specific fields)
[.parquet file]
  → ArrowRawReader (Parquet → Row, handles NULL columns)

[Multiple files]
  → MergesortReader (N readers → 1 sorted stream) [TYPE PROMOTION HERE]

[During row processing]
  → LogTranslator applied (adds resource_bucket_name, resource_file_name, chq_fingerprint, etc.)

Output: Cooked Parquet (logs)
```

### Log Compaction (Cooked → Compacted Cooked)
```
[Multiple cooked parquet segments]
  → ArrowRawReader (per segment)
    → CookedLogTranslatingReader (validation, type coercion, drop invalid rows)
  → MergesortReader (merge N segments by timestamp) [TYPE PROMOTION HERE]

Output: Compacted Parquet (logs)
```

### Metric Ingestion (Raw → Cooked)
```
[.binpb file]
  → IngestProtoMetricsReader (OTEL → Rows, one per datapoint)
[.json / .parquet file]
  → JSONLinesReader or ParquetRawReader

[Optional: Multiple files]
  → MergesortReader [TYPE PROMOTION HERE]

[Optional: Aggregation]
  → AggregatingMetricsReader (reduce by time bucket + labels)

Output: Cooked Parquet (metrics)
```

### Metric Compaction (Cooked → Compacted Cooked)
```
[Multiple cooked parquet segments]
  → ParquetRawReader (per segment)
    → CookedMetricTranslatingReader (validation, type coercion)
    → [Optional] DiskSortingReader (if sort_version mismatch)
  → MergesortReader (merge N segments by sort key) [TYPE PROMOTION HERE]
  → AggregatingMetricsReader (aggregate by time window + labels)

Output: Compacted Parquet (metrics)
```

### Metric Rollup (Cooked → Rolled-up Cooked)
```
[Multiple cooked parquet segments at fine granularity]
  → ParquetRawReader (per segment)
    → CookedMetricTranslatingReader
  → MergesortReader [TYPE PROMOTION HERE]
  → AggregatingMetricsReader (aggregate to coarser time bucket)

Output: Rolled-up Parquet (metrics at coarser granularity)
```

### Trace Ingestion (Raw → Cooked)
```
[.binpb file]
  → IngestProtoTracesReader (OTEL Span → Row, computes chq_fingerprint)
[.json / .parquet file]
  → JSONLinesReader or ParquetRawReader

[Multiple files]
  → MergesortReader [TYPE PROMOTION HERE]

Output: Cooked Parquet (traces)
```

### Trace Compaction (Cooked → Compacted Cooked)
```
[Multiple cooked parquet segments]
  → ParquetRawReader (per segment)
    → CookedTraceTranslatingReader (validation, type coercion)
  → MergesortReader (merge by trace_id, timestamp) [TYPE PROMOTION HERE]

Output: Compacted Parquet (traces)
```

---

## Schema Sources Summary

| Reader | Schema Source | Needs Scan? | Used in Production? |
|--------|--------------|-------------|---------------------|
| ParquetRawReader | Metadata | No | ✅ Yes |
| ArrowRawReader | Metadata | No | ✅ Yes |
| CSVReader | Header + Inference | Partial (header + samples) | ✅ Yes |
| JSONLinesReader | Full Data Scan | Yes | ✅ Yes |
| IngestProtoLogsReader | Full Data Scan | Yes | ✅ Yes |
| IngestProtoMetricsReader | Full Data Scan | Yes | ✅ Yes |
| IngestProtoTracesReader | Full Data Scan | Yes | ✅ Yes |
| TranslatingReader | Wrapped + Translator | Depends on wrapped | ✅ Yes |
| CookedLogTranslatingReader | Wrapped + Transforms | Depends on wrapped | ✅ Yes |
| CookedMetricTranslatingReader | Wrapped + Transforms | Depends on wrapped | ✅ Yes |
| CookedTraceTranslatingReader | Wrapped + Transforms | Depends on wrapped | ✅ Yes |
| MergesortReader | Merge Children | Depends on children | ✅ Yes |
| SequentialReader | Merge Children | Depends on children | ❌ No |
| DiskSortingReader | Pass-through Wrapped | Depends on wrapped | ✅ Yes |
| AggregatingMetricsReader | Filtered Wrapped | Depends on wrapped | ✅ Yes |
| MetricFilteringReader | Pass-through Wrapped | Depends on wrapped | ✅ Yes |

---

## Implementation Phases

### Phase 1: Schema Interface & Base Readers
- [ ] Define `ColumnSchema`, `ReaderSchema` types in `pipeline/` package
- [ ] Add `GetSchema() (ReaderSchema, error)` method to `Reader` interface
- [ ] Implement for **Parquet/Arrow** readers (easy - metadata)
- [ ] Implement for **CSV** reader (header + sample scan)
- [ ] Implement for **JSONLines** reader (full scan or schema hint file)

### Phase 2: Proto Readers (Scan Required)
- [ ] **IngestProtoLogsReader**: Extract schema during existing sort pass
- [ ] **IngestProtoMetricsReader**: Extract schema during eager load
- [ ] **IngestProtoTracesReader**: Extract schema during existing sort pass

### Phase 3: Translating/Wrapper Readers
- [ ] **TranslatingReader**: Combine wrapped schema + translator schema delta
- [ ] **CookedLog/Metric/TraceTranslatingReader**: Track transform effects on schema
- [ ] **DiskSortingReader**: Pass-through wrapper (simple)

### Phase 4: Aggregate Readers (Critical)
- [ ] **MergesortReader**: Implement type promotion logic
- [ ] **SequentialReader**: Implement schema merge
- [ ] Define type promotion rules (int64 + string → string, etc.)

### Phase 5: Processing Readers
- [ ] **AggregatingMetricsReader**: Output filtered schema
- [ ] **MetricFilteringReader**: Pass-through schema

### Phase 6: Schema Validation
- [ ] Add optional runtime validator: `SchemaValidatingReader`
- [ ] Track violations (wrong type emitted, unexpected non-null, etc.)
- [ ] Expose violations via `ReaderStats` object

### Phase 7: Writer Integration
- [ ] Update `ParquetWriter` to accept `ReaderSchema` upfront
- [ ] Filter out all-null columns before writing
- [ ] Ensure no mid-write schema changes

### Phase 8: Testing & Rollout
- [ ] Add schema tests for each reader
- [ ] Integration tests for full reader stacks
- [ ] Add validation in production with monitoring
- [ ] Disable validation after stability proven

---

## Type Promotion Rules (for MergesortReader)

When merging schemas from multiple readers:

| Type A | Type B | Promoted Type |
|--------|--------|---------------|
| int64 | int64 | int64 |
| int64 | float64 | float64 |
| int64 | string | string |
| int64 | bool | string |
| float64 | float64 | float64 |
| float64 | string | string |
| float64 | bool | string |
| string | string | string |
| string | bool | string |
| bool | bool | bool |
| bytes | bytes | bytes |
| bytes | * | string |
| any | * | any |

**General Rules**:

- When in doubt, promote to `string` (most permissive type for simple types)
- `any` mixed with anything stays `any` (preserves passthrough behavior for complex types)

---

## Open Questions

1. **Schema caching**: Should we cache schemas from slow-to-scan readers (proto, JSON)?
2. **Schema hints**: Should we support optional `.schema.json` files alongside data files?
3. **Partial scans**: For huge JSON/proto files, can we limit scan to first N records and risk missing columns?
4. **Schema evolution**: How do we handle cooked Parquet files where schema has changed over time?
5. **Validation performance**: What's the overhead of runtime validation? Acceptable in production?

---

## Success Criteria

- ✅ No mid-write schema changes in Parquet output
- ✅ All readers provide schema before first `Next()` call
- ✅ Type consistency enforced across reader stacks
- ✅ Null-only columns filtered out before write
- ✅ Schema validation catches violations in development
- ✅ Production rollout with monitoring, no regressions

---

## Notes

- **Sorting side-effect**: IngestProto readers already scan data (for sorting), so schema extraction is "free"
- **Aggregation impact**: AggregatingMetricsReader significantly changes schema - must be explicit
- **MergesortReader is critical**: This is where most type promotion happens - needs careful implementation
- **Validation cost**: Consider making validation opt-in via env var or config flag
