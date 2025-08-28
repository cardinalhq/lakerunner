# TODO

## ExponentialHistogram Processing

- [ ] Implement proper `addExponentialHistogramDatapointFields` function in `internal/filereader/proto_metrics.go:252`
  - Currently only extracts basic fields (count, sum, timestamp)
  - Needs to process exponential histogram buckets and convert to sketches/rollups like regular histograms
  - Should follow the same pattern as `addHistogramDatapointFields` but handle exponential bucket structure
  - Reference: OpenTelemetry exponential histogram specification

## Summary Processing

- [ ] Implement proper `addSummaryDatapointFields` function in `internal/filereader/proto_metrics.go`
  - Currently only extracts basic fields (count, sum, quantiles, timestamp)
  - Needs to process summary quantiles and convert to sketches/rollups like histograms
  - Should follow the same pattern as `addHistogramDatapointFields` but handle summary quantile structure
  - Reference: OpenTelemetry summary specification

## Log Parquet Reader Translation

- [ ] Fix log parquet filereader to apply translation layer
  - Currently the parquet reader for logs returns raw parquet data without CardinalHQ field translation
  - Need to add a translation layer to convert parquet field names to CardinalHQ format
  - Should ensure returned reader produces rows with `_cardinalhq.*` fields consistently
  - Related files: `internal/filereader/parquet.go` or similar parquet reading logic
  - Goal: Make parquet-sourced log data consistent with protobuf-sourced log data field naming

## Parquet File Format Specifications

- [ ] Create spec for standard cooked log parquet format
  - Define the canonical schema and field types for processed log files
  - Document required `_cardinalhq.*` fields and their purposes
  - Include indexing and compression requirements
  - Specify file naming conventions and partitioning strategy

- [ ] Create spec for standard cooked metrics parquet format
  - Define the canonical schema and field types for processed metric files
  - Document metric-specific fields like TID, rollup levels, and aggregation data
  - Include guidance for different metric types (gauge, counter, histogram, summary)
  - Specify file organization and time-based partitioning

- [ ] Create spec for standard cooked traces parquet format
  - Define the canonical schema and field types for processed trace files
  - Document trace-specific fields like span relationships, slot IDs, and trace context
  - Include span duration indexing and trace assembly requirements
  - Specify file organization optimized for trace queries
