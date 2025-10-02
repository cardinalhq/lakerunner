# Allowed field names

When we translate from one format to another, we do not want to add extra fields that are
not properly prefixed, and then only add those fields when they are not high cardinality
generally.

## signal Metric

### TID calculation

TIDs (Timeseries IDs) are used to uniquely identify a specific time series based on its
dimensional attributes. The TID is a 64-bit hash computed from specific fields in the metric data.

#### Fields included in TID calculation

- `metric_name` - The metric name (always included)
- `chq_metric_type` - The metric type: gauge, counter, histogram (always included if present)
- `resource_*` - Resource attributes (string values only)
- `attr_*` - Metric-specific attributes/labels (string values only)
- `metric_*` - Metric-specific fields (string values only)

#### Fields explicitly excluded from TID calculation

- `scope_*` - Scope attributes are NOT included in the TID
- All other `chq_*` fields (except name and metric_type)
- Any arbitrary fields that don't match the above prefixes
- Empty string values (filtered out regardless of field name)
- Non-string values in `resource_*` and `attr_*` fields

The TID calculation is deterministic - the same set of attributes will always produce the same TID.
Fields are sorted alphabetically before hashing to ensure consistent results regardless of field order.

### Standard Metric Fields

#### Core identification fields

- `metric_name` - The metric name (required)
- `chq_metric_type` - The metric type: gauge, counter, histogram
- `chq_tid` - The computed timeseries ID (added during processing)
- `chq_timestamp` - The metric datapoint's timestamp in Unix milliseconds

#### Metadata fields

- `chq_description` - Human-readable description of the metric
- `chq_unit` - The unit of measurement as per Otel spec
- `chq_scope_url` - Instrumentation scope URL/version
- `chq_scope_name` - Instrumentation scope name
- `chq_customer_id` - Customer/organization identifier
- `chq_telemetry_type` - Type of telemetry data (set to "metrics")

#### Statistical aggregation fields (chq_rollup_*)

- `chq_rollup_sum` - Sum of all values in the aggregation window
- `chq_rollup_count` - Number of values in the aggregation window
- `chq_rollup_avg` - Average value
- `chq_rollup_min` - Minimum value
- `chq_rollup_max` - Maximum value
- `chq_rollup_p25` - 25th percentile
- `chq_rollup_p50` - 50th percentile (median)
- `chq_rollup_p75` - 75th percentile
- `chq_rollup_p90` - 90th percentile
- `chq_rollup_p95` - 95th percentile
- `chq_rollup_p99` - 99th percentile

#### Distribution representation

- `chq_sketch` - Binary encoded DDSketch for histogram/distribution data

#### Dimensional attributes

- `resource_*` - Resource attributes (e.g., `resource_host`, `resource_service_name`)
- `attr_*` - Signal-specific labels/attributes (e.g., `attr_http_status_code`)
- `scope_*` - Instrumentation scope attributes
