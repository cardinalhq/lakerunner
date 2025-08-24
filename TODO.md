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