# TODO

## OpenTelemetry Proto Metrics Reader

### ExponentialHistogram Processing
- [ ] Implement proper `addExponentialHistogramDatapointFields` function in `internal/filereader/proto_metrics.go:252`
  - Currently only extracts basic fields (count, sum, timestamp)
  - Needs to process exponential histogram buckets and convert to sketches/rollups like regular histograms
  - Should follow the same pattern as `addHistogramDatapointFields` but handle exponential bucket structure
  - Reference: OpenTelemetry exponential histogram specification

### Summary Processing
- [ ] Implement proper `addSummaryDatapointFields` function in `internal/filereader/proto_metrics.go`
  - Currently only extracts basic fields (count, sum, quantiles, timestamp)
  - Needs to process summary quantiles and convert to sketches/rollups like histograms
  - Should follow the same pattern as `addHistogramDatapointFields` but handle summary quantile structure
  - Reference: OpenTelemetry summary specification

## Metric Ingest Query Issue
- [ ] **CRITICAL**: Handle `_cardinalhq.name` field properly in metric ingest
  - The new query system is treating `_cardinalhq.name` as a full value dimension for all datasets
  - This change is on `lakerunner/main` but needs to be verified on deployment branches
  - Need to ensure metric ingest processes handle this field correctly
  - Related to logs ingest - both need consistent `_cardinalhq.*` field handling
  - Contact: @Michael for context on query system changes

## Log Parquet Reader Translation
- [ ] Fix log parquet filereader to apply translation layer
  - Currently the parquet reader for logs returns raw parquet data without CardinalHQ field translation
  - Need to add a translation layer to convert parquet field names to CardinalHQ format
  - Should ensure returned reader produces rows with `_cardinalhq.*` fields consistently
  - Related files: `internal/filereader/parquet.go` or similar parquet reading logic
  - Goal: Make parquet-sourced log data consistent with protobuf-sourced log data field naming