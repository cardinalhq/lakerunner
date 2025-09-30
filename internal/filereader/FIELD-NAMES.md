# Allowed field names

When we translate from one format to another, we do not want to add extra fields that are
not properly prefixed, and then only add those fields when they are not high cardinality
generally.

## signal Metric

### TID calculation

TIDs (Timeseries IDs) are used to uniquely identify a specific time series based on its
dimensional attributes. The TID is a 64-bit hash computed from specific fields in the metric data.

#### Fields included in TID calculation

- `_cardinalhq_name` - The metric name (always included)
- `_cardinalhq_metric_type` - The metric type: gauge, counter, histogram (always included if present)
- `resource_*` - Resource attributes (string values only)
- `metric_*` - Metric-specific attributes/labels (string values only)

#### Fields explicitly excluded from TID calculation

- `scope.*` - Scope attributes are NOT included in the TID
- All other `_cardinalhq_*` fields (except name and metric_type)
- All underscore-prefixed fields (except the two special _cardinalhq fields above)
- Any arbitrary fields that don't match the above prefixes
- Empty string values (filtered out regardless of field name)
- Non-string values in `resource_*` and `metric_*` fields

The TID calculation is deterministic - the same set of attributes will always produce the same TID.
Fields are sorted alphabetically before hashing to ensure consistent results regardless of field order.

### Standard Metric Fields

#### Core identification fields

- `_cardinalhq_name` - The metric name (required)
- `_cardinalhq_metric_type` - The metric type: gauge, counter, histogram
- `_cardinalhq_tid` - The computed timeseries ID (added during processing)
- `_cardinalhq_timestamp` - The metric datapoint's timestamp in Unix milliseconds

#### Metadata fields

- `_cardinalhq_description` - Human-readable description of the metric
- `_cardinalhq_unit` - The unit of measurement as per Otel spec
- `_cardinalhq_scope_url` - Instrumentation scope URL/version
- `_cardinalhq_scope_name` - Instrumentation scope name
- `_cardinalhq_customer_id` - Customer/organization identifier
- `_cardinalhq_telemetry_type` - Type of telemetry data (set to "metrics")

#### Statistical aggregation fields (rollup_*)

- `rollup_sum` - Sum of all values in the aggregation window
- `rollup_count` - Number of values in the aggregation window
- `rollup_avg` - Average value
- `rollup_min` - Minimum value
- `rollup_max` - Maximum value
- `rollup_p25` - 25th percentile
- `rollup_p50` - 50th percentile (median)
- `rollup_p75` - 75th percentile
- `rollup_p90` - 90th percentile
- `rollup_p95` - 95th percentile
- `rollup_p99` - 99th percentile

#### Distribution representation

- `sketch` - Binary encoded DDSketch for histogram/distribution data

#### Dimensional attributes

- `resource_*` - Resource attributes (e.g., `resource_host`, `resource_service.name`)
- `metric_*` - Metric-specific labels/attributes (e.g., `metric_http.status_code`)
- `scope_*` - Instrumentation scope attributes
