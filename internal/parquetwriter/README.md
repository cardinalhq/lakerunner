# parquetwriter

Schema-less Parquet writer with dynamic ordering, size-based splitting, and telemetry signal factories.

## Features

- **Dynamic Schema Discovery**: Accepts `map[string]any` and builds per-file schemas from observed keys
- **Multiple Ordering Strategies**: None, in-memory, merge sort, spillable, presumed
- **Size-Based File Splitting**: Automatic splitting with group boundary respect
- **Signal-Specific Factories**: Optimized writers for metrics (TID-grouped), logs (timestamp-ordered), traces (slot-grouped)
- **Per-File Metadata Collection**: Returns actual TIDs, fingerprints, span/trace IDs for database operations

## Architecture

- **UnifiedWriter**: Main implementation coordinating ordering, sizing, and splitting
- **OrderingEngines**: Handle different sort strategies with memory/disk trade-offs
- **FileSplitter**: Manages file boundaries and schema discovery
- **SchemaBuilder**: Builds Parquet schemas dynamically from data
- **Signal Factories**: Pre-configured writers for specific telemetry types

## Quick Start

```go
// Metrics (TID-grouped, no splits within TIDs)
writer, err := factories.NewMetricsWriter("/tmp", 200)

// Logs (timestamp-ordered, spillable sorting)  
writer, err := factories.NewLogsWriter("/tmp", 150)

// Traces (slot-grouped, start time ordered)
writer, err := factories.NewTracesWriter("/tmp", slotID, 300)

// Unlimited file size (single file, no splitting)
writer, err := factories.NewMetricsWriter("/tmp", parquetwriter.NoRecordLimitPerFile)

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
| `GroupKeyFunc` | Extract grouping key (for `NoSplitGroups`) |
| `NoSplitGroups` | Prevent splitting groups across files |
| `RecordsPerFile` | Record limit per file (use `NoRecordLimitPerFile` for unlimited) |
| `StatsProvider` | Collect per-file metadata |

## Custom Writer

```go
config := parquetwriter.WriterConfig{
    TmpDir:         tmpdir,
    RecordsPerFile: 10000, // Or use parquetwriter.NoRecordLimitPerFile for unlimited
    GroupKeyFunc:   func(row map[string]any) any { return row["group"] },
    NoSplitGroups:  true,
}
writer, err := parquetwriter.NewUnifiedWriter(config)
```

## Signal-Specific Metadata

- **Metrics**: `MetricsFileStats{TIDCount, MinTID, MaxTID, TIDs[]}`
- **Logs**: `LogsFileStats{FingerprintCount, FirstTS, LastTS, Fingerprints[]}`
- **Traces**: `TracesFileStats{SlotID, SpanCount, TraceCount, FirstTS, LastTS, SpanIDs[], TraceIDs[]}`
