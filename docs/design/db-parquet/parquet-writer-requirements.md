# Parquet Writer Requirements by Signal Type

This document defines the specific requirements and constraints for writing Parquet files for each signal type (logs, metrics, traces) in Lakerunner.

## Overview

Lakerunner processes three types of telemetry signals, each with different characteristics that affect how they should be written to Parquet files:

- **Metrics**: Time-series data with TID ordering requirements
- **Logs**: Event data with timestamp ordering requirements  
- **Traces**: Distributed tracing data with slot-based grouping

## Common Requirements

All signal types share these base requirements:

1. **Target File Size**: ~1.1MB per file (defined in `cmd/root.go:targetFileSize`)
2. **File Format**: Apache Parquet with optimal compression and encoding
3. **Schema Management**: Dynamic schema based on seen columns
4. **Temporary File Cleanup**: All temporary files must be cleaned up after processing
5. **Error Handling**: Graceful handling of write errors and context cancellation
6. **Memory Management**: Efficient memory usage for large datasets

## Metrics Requirements

### Ordering Requirements
- **CRITICAL**: Data MUST be ordered by TID (Timeseries ID)
- Input data from TIDMerger is already TID-ordered
- Use `OrderPresumed` strategy to avoid unnecessary re-sorting

### Grouping Requirements  
- **CRITICAL**: Never split TID groups across files
- All data points for a single TID must be in the same output file
- Use `NoSplitGroups: true` with TID-based grouping function
- This may result in files exceeding target size to maintain TID integrity

### File Splitting Strategy
- Split only when switching to a new TID
- Prefer slightly larger files over split TIDs
- Use TID count and TID range in file metadata

### Performance Considerations
- TID-ordered input allows streaming writes
- No external merge sort needed
- Memory usage scales with largest TID group size

### Key Schema Fields
- `_cardinalhq.tid` (int64, required): Timeseries identifier
- Various metric fields based on metric type

## Logs Requirements

### Ordering Requirements
- **REQUIRED**: Data MUST be ordered by timestamp  
- Input data is typically unordered from ingestion
- Use `OrderMergeSort` for large datasets that don't fit in memory
- Use `OrderInMemory` for smaller datasets

### Grouping Requirements
- **NONE**: No grouping constraints
- Can split anywhere for optimal file sizes
- Splitting by timestamp boundaries is acceptable

### File Splitting Strategy
- Split freely to maintain target file size
- Hour-based splitting in some cases for query optimization
- Use timestamp range in file metadata

### Performance Considerations
- External merge sort needed for large datasets
- Memory usage controlled by sort buffer size
- Temporary chunk files created during sorting

### Key Schema Fields
- `_cardinalhq.timestamp` (int64, required): Event timestamp in milliseconds
- `_cardinalhq.fingerprint` (int64, optional): For deduplication
- Log content fields vary by source

## Traces Requirements

### Ordering Requirements
- **OPTIONAL**: Typically ordered by start time for better query performance
- Input data may be unordered
- Use `OrderInMemory` for moderate datasets
- Use `OrderMergeSort` for very large datasets

### Grouping Requirements
- **SOFT**: Group by slot ID for query locality
- Can split within slots if needed for size management
- Slot-based organization helps with query performance

### File Splitting Strategy
- Split within slots when necessary for file size
- Maintain slot locality where possible
- Include slot ID and span/trace counts in metadata

### Performance Considerations
- Moderate memory requirements for typical trace volumes
- Slot-based partitioning aids query performance
- Consider trace locality during splitting

### Key Schema Fields
- `_cardinalhq.trace_id` (string, required): Trace identifier
- `_cardinalhq.span_id` (string, required): Span identifier  
- `_cardinalhq.start_time_unix_ns` (int64, required): Span start time
- Various span attributes and resource attributes

## Implementation Guidelines

### Writer Configuration by Signal Type

#### Metrics Writer
```go
config := WriterConfig{
    OrderBy:        OrderPresumed,     // Input already ordered
    GroupKeyFunc:   tidGrouper,        // Group by TID
    NoSplitGroups:  true,              // Never split TIDs
    StatsProvider:  MetricsStats,      // TID count/range stats
}
```

#### Logs Writer
```go
config := WriterConfig{
    OrderBy:        OrderMergeSort,    // External sort by timestamp
    OrderKeyFunc:   timestampExtractor, // Sort key
    NoSplitGroups:  false,             // Split freely
    StatsProvider:  LogsStats,         // Timestamp range/fingerprints
}
```

#### Traces Writer
```go
config := WriterConfig{
    OrderBy:        OrderInMemory,     // In-memory sort by start time
    OrderKeyFunc:   startTimeExtractor, // Sort key
    GroupKeyFunc:   slotGrouper,       // Group by slot (soft)
    NoSplitGroups:  false,             // Split within slots OK
    StatsProvider:  TracesStats,       // Span/trace counts per slot
}
```

### Error Conditions to Handle

1. **Schema Violations**: Row contains columns not in schema
2. **Missing Required Fields**: TID for metrics, timestamp for logs, etc.
3. **Memory Exhaustion**: Large TID groups or sort buffers
4. **Disk Space**: Temporary file creation failures
5. **Context Cancellation**: Graceful shutdown during processing

### Monitoring and Observability

Each writer should emit metrics for:
- Rows processed per signal type
- Files created per signal type  
- Average file sizes by signal type
- Processing time per batch
- Memory usage during processing
- Error counts by type

## Migration Strategy

1. **Phase 1**: Implement new writer alongside existing buffet writer
2. **Phase 2**: Use new writer in non-critical paths for validation
3. **Phase 3**: Migrate logs compaction (most flexible requirements)
4. **Phase 4**: Migrate metrics compaction (most critical requirements)
5. **Phase 5**: Migrate traces compaction
6. **Phase 6**: Remove old buffet writer implementation

## Testing Requirements

### Unit Tests
- Individual component testing (orderers, splitters, estimators)
- Error condition handling
- Configuration validation
- Memory usage patterns

### Integration Tests  
- Full pipeline testing with real data patterns
- File size distribution validation
- Ordering verification
- Performance benchmarking

### Signal-Specific Tests
- **Metrics**: TID grouping integrity, no-split verification
- **Logs**: Timestamp ordering accuracy, memory handling
- **Traces**: Slot locality, moderate dataset handling

## Future Considerations

### Performance Optimizations
- Parallel writing for independent files
- Streaming compression
- Better size estimation algorithms
- Adaptive buffer sizing

### Feature Enhancements
- Column-level compression hints
- Custom partitioning strategies  
- Query-aware file organization
- Real-time file size monitoring

### Operational Features
- Metrics and alerting integration
- Configuration hot-reloading
- Writer pool management
- Resource usage reporting