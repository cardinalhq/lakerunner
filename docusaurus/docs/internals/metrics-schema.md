---
sidebar_position: 3
---

# Metrics Parquet Schema

Metrics Parquet files store flattened and aggregated OpenTelemetry metrics with DDSketch encoding for accurate percentile calculations.

## System Fields

### chq_customer_id

**Type:** string (required)

Organization ID that owns this metric data. Set during ingestion from the `OrgID` in reader options. Used for data isolation and access control.

### chq_tid

**Type:** int64 (required)

Time-series ID. A 64-bit FNV-1a hash that uniquely identifies a metric time series. Computed from:

1. `metric_name` (normalized)
2. `chq_metric_type`
3. Selected `resource_*` attributes
4. All `attr_*` attributes

Enables efficient grouping for aggregation, rollup, and query optimization.

### chq_timestamp

**Type:** int64 (required)

Milliseconds since Unix epoch. Truncated to 10-second intervals for rollup alignment: `(ts / 10000) * 10000`.

### chq_tsns

**Type:** int64 (required)

Original timestamp in nanoseconds for full precision.

### chq_metric_type

**Type:** string (required)

Semantic type of the metric:

| Value | OTEL Source |
| ----- | ----------- |
| `"gauge"` | Gauge metrics (point-in-time values) |
| `"count"` | Sum metrics (cumulative or delta counters) |
| `"histogram"` | Histogram, ExponentialHistogram, Summary metrics |

### chq_sketch

**Type:** bytes (required)

Serialized DDSketch (DataDog Sketch) for this datapoint. Core data structure enabling accurate percentile calculations across aggregated data.

- Single-value metrics (gauge/sum): Contains exactly one value
- Histogram metrics: Reconstructed distribution from bucket counts

### chq_telemetry_type

**Type:** string (required)

Always `"metrics"` for metric records.

### chq_description

**Type:** string (nullable)

Metric description from OTEL metadata.

### chq_unit

**Type:** string (nullable)

Metric unit from OTEL metadata (e.g., "ms", "By", "\{requests\}").

### chq_scope_name

**Type:** string (nullable)

Instrumentation scope name from OTEL.

### chq_scope_url

**Type:** string (nullable)

Instrumentation scope **version** (not URL, despite the field name).

## Rollup Fields

Pre-computed statistics extracted from the DDSketch:

| Field | Type | Description |
| ----- | ---- | ----------- |
| `chq_rollup_avg` | float64 | Average value (sum/count) |
| `chq_rollup_count` | float64 | Number of observations |
| `chq_rollup_min` | float64 | Minimum value |
| `chq_rollup_max` | float64 | Maximum value |
| `chq_rollup_sum` | float64 | Sum of all values |
| `chq_rollup_p25` | float64 | 25th percentile |
| `chq_rollup_p50` | float64 | 50th percentile (median) |
| `chq_rollup_p75` | float64 | 75th percentile |
| `chq_rollup_p90` | float64 | 90th percentile |
| `chq_rollup_p95` | float64 | 95th percentile |
| `chq_rollup_p99` | float64 | 99th percentile |

For single-value metrics, all percentile fields contain the same value.

## Metric Name

### metric_name

**Type:** string (required)

Normalized metric name:

- Lowercase conversion
- Dots and non-alphanumeric characters replaced with underscores

Example: `http.server.request.duration` → `http_server_request_duration`

## Resource Attribute Filtering

Unlike logs, metrics apply strict filtering to reduce cardinality. Only these resource attributes are retained:

- `resource_app`
- `resource_container_image_name`
- `resource_container_image_tag`
- `resource_k8s_cluster_name`
- `resource_k8s_daemonset_name`
- `resource_k8s_deployment_name`
- `resource_k8s_namespace_name`
- `resource_k8s_pod_ip`
- `resource_k8s_pod_name`
- `resource_k8s_statefulset_name`
- `resource_service_name`
- `resource_service_version`

Other resource attributes are discarded during ingestion.

## Datapoint Attributes

All datapoint attributes retained with `attr_*` prefix, except:

- Attributes starting with underscore (`_`) are skipped
- Empty string values are skipped

## Type Handling

| Source Type | Storage |
| ----------- | ------- |
| String | String |
| Integer | Int64 |
| Float | Float64 |
| Boolean | String (`"true"`, `"false"`) |
| Maps/Slices | JSON string |

## OTEL Metric Type Processing

### Gauge Metrics

Single numeric value at a point in time.

- Value from `datapoint.DoubleValue()` or `datapoint.IntValue()`
- Single-value sketch created
- All rollup percentile fields set to same value
- `chq_metric_type` = `"gauge"`

### Sum Metrics (Counters)

Cumulative or delta counters.

- Processed identically to gauges
- `chq_metric_type` = `"count"`

### Histogram Metrics

Explicit bucket histograms with counts and bounds.

- Bucket counts and explicit bounds extracted
- Values reconstructed using `ConvertHistogramToValues()`
- Handles underflow and overflow buckets
- `chq_metric_type` = `"histogram"`

### Exponential Histogram Metrics

Logarithmic bucket histograms.

- Scale, zero count, positive/negative buckets extracted
- Converted to values via `ConvertExponentialHistogramToValues()`
- `chq_metric_type` = `"histogram"`

### Summary Metrics

Pre-computed quantile summaries.

- Quantile values and counts extracted
- Converted to DDSketch
- `chq_metric_type` = `"histogram"`

## Example Transformation

### OTEL Input

```text
name: "http.server.request.count"
description: "Number of HTTP requests"
unit: "\{requests\}"
type: Sum
datapoints: [
  {
    timestamp: 1640995200000000000
    value: 42
    attributes: [
      {key: "http.method", value: "GET"},
      {key: "http.status_code", value: 200}
    ]
  }
]
resource: {
  attributes: [
    {key: "service.name", value: "api-gateway"},
    {key: "k8s.namespace.name", value: "production"}
  ]
}
scope: {
  name: "opentelemetry-go"
  version: "1.21.0"
}
```

### Parquet Output

```text
metric_name: "http_server_request_count"
chq_customer_id: "org-123"
chq_description: "Number of HTTP requests"
chq_metric_type: "count"
chq_scope_name: "opentelemetry-go"
chq_scope_url: "1.21.0"
chq_sketch: <encoded DDSketch bytes>
chq_telemetry_type: "metrics"
chq_tid: -1234567890123456789
chq_timestamp: 1640995200000
chq_tsns: 1640995200000000000
chq_unit: "\{requests\}"
chq_rollup_avg: 42.0
chq_rollup_count: 1.0
chq_rollup_max: 42.0
chq_rollup_min: 42.0
chq_rollup_sum: 42.0
chq_rollup_p25: 42.0
chq_rollup_p50: 42.0
chq_rollup_p75: 42.0
chq_rollup_p90: 42.0
chq_rollup_p95: 42.0
chq_rollup_p99: 42.0

resource_service_name: "api-gateway"
resource_k8s_namespace_name: "production"

attr_http_method: "GET"
attr_http_status_code: 200
```

## Dropped Datapoints

Datapoints are not written in these cases:

- NaN or Infinity values for gauge/sum
- Empty metric name after normalization
- Histogram with no bucket counts
- Exponential histogram with no data
- Summary with zero count or no quantile values

## File Sorting

Sorted by `[metric_name, chq_tid, chq_timestamp]`:

- Groups datapoints by metric for efficient queries
- Keeps time series together for range scans
- Chronological ordering within each series

## Required Fields

- `metric_name`
- `chq_tid`
- `chq_timestamp`
- `chq_tsns`
- `chq_metric_type`
- `chq_customer_id`
- `chq_telemetry_type`
- `chq_sketch`
- All `chq_rollup_*` fields

## Optional Fields

- `chq_description`
- `chq_unit`
- `chq_scope_name`
- `chq_scope_url`
- All `resource_*` fields (filtered)
- All `attr_*` fields

## Legacy Fields

These may appear in older files:

| Legacy Field | Modern Equivalent |
| ------------ | ----------------- |
| `_cardinalhq_customer_id` | `chq_customer_id` |
| `_cardinalhq_bucket_prefix` | (removed) |
| `_cardinalhq_collector_id` | (removed) |
