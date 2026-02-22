# parquetwriter

High-performance Parquet writer with schema-driven conversion, size-based splitting, and telemetry signal factories.

## Features

- **Schema-Driven Conversion**: Requires upfront schema from reader; validates all columns against schema
- **Multiple Ordering Strategies**: None, in-memory, merge sort, spillable, presumed
- **Size-Based File Splitting**: Automatic splitting with group boundary respect
- **Signal-Specific Factories**: Optimized writers for metrics (TID-grouped), logs (timestamp-ordered), traces (slot-grouped)
- **Per-File Metadata Collection**: Returns actual TIDs, fingerprints, span/trace IDs for database operations
- **Direct Parquet Writes**: Uses go-parquet library for 2x faster throughput (100K+ logs/sec)

The schema will be checked as rows are written, and missing columns in a row will be filled with NULL.

## Architecture

- **UnifiedWriter**: Main implementation coordinating ordering, sizing, and splitting
- **OrderingEngines**: Handle different sort strategies with memory/disk trade-offs
- **FileSplitter**: Manages file boundaries and direct parquet writes using go-parquet
- **SchemaBuilder**: Converts reader schemas to Parquet schemas with string conversion
- **Signal Factories**: Pre-configured writers for specific telemetry types

## Quick Start

```go
// Get schema from reader (required)
reader, _ := filereader.ReaderForFileWithOptions(inputFile, options)
schema := reader.GetSchema()

// Metrics (TID-grouped, no splits within TIDs)
writer, err := factories.NewMetricsWriter("/tmp", schema, 200)

// Logs (timestamp-ordered, spillable sorting)
writer, err := factories.NewLogsWriter("/tmp", schema, 150, parquetwriter.DefaultBackend, "", true)

// Traces (slot-grouped, start time ordered)
writer, err := factories.NewTracesWriter("/tmp", schema, 300)

// Unlimited file size (single file, no splitting)
writer, err := factories.NewMetricsWriter("/tmp", schema, parquetwriter.NoRecordLimitPerFile)

// Write data
err = writer.Write(map[string]any{"field": "value", "count": int64(123)})

// Finalize
results, err := writer.Close(ctx)
for _, result := range results {
    fmt.Printf("File: %s, Records: %d, Metadata: %+v\n",
        result.FileName, result.RecordCount, result.Metadata)
}
```

## Configuration

| Field | Description |
|-------|-------------|
| `Schema` | **Required**: Reader schema from filereader (must be complete upfront) |
| `TmpDir` | **Required**: Directory for temporary parquet files |
| `GroupKeyFunc` | Extract grouping key (for `NoSplitGroups`) |
| `NoSplitGroups` | Prevent splitting groups across files |
| `RecordsPerFile` | Record limit per file (use `NoRecordLimitPerFile` for unlimited) |
| `StatsProvider` | Collect per-file metadata |

## Custom Writer

```go
// Get schema from reader
reader, _ := filereader.ReaderForFileWithOptions(inputFile, options)
schema := reader.GetSchema()

config := parquetwriter.WriterConfig{
    TmpDir:         tmpdir,
    Schema:         schema,
    RecordsPerFile: 10000,  // Or use parquetwriter.NoRecordLimitPerFile for unlimited
    GroupKeyFunc:   func(row pipeline.Row) any { return row["group"] },
    NoSplitGroups:  true,
}
writer, err := parquetwriter.NewUnifiedWriter(config)
```

## Signal-Specific Metadata

- **Metrics**: `MetricsFileStats{TIDCount, MinTID, MaxTID, TIDs[]}`
- **Logs**: `LogsFileStats{FingerprintCount, FirstTS, LastTS, Fingerprints[]}`
- **Traces**: `TracesFileStats{SpanCount, TraceCount, FirstTS, LastTS, SpanIDs[], TraceIDs[]}`
