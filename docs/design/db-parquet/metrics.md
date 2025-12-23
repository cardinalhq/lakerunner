# Fields of the "cooked" db/ Parquet for Metrics

Metrics parquet files are a flattened and aggregated version of the OpenTelemetry
wire format built for efficient time-series queries and statistical analysis.

## chq prefix

Fields beginning with `chq_` are system fields used by the CardinalHQ Lakerunner data lake system.

### Field Specifications and Data Types

`chq_customer_id` (string, required) is the organization ID that owns this metric data. Set
during ingestion from the `OrgID` in the reader options. Used for data isolation and access control.

`chq_description` (string, nullable) contains the metric description from the OTEL metric metadata.
Extracted from `metric.Description()` during ingestion. May be empty if not provided by the source.

`chq_metric_type` (string, required) indicates the semantic type of the metric. Derived from
the OTEL metric type during ingestion:

- `"gauge"` - For OTEL Gauge metrics (point-in-time values)
- `"count"` - For OTEL Sum metrics (cumulative or delta counters)
- `"histogram"` - For OTEL Histogram, ExponentialHistogram, and Summary metrics

Source: [ingest_proto_metrics_sorting.go:376-385](../../../internal/filereader/ingest_proto_metrics_sorting.go#L376-L385)

`chq_scope_name` (string, nullable) contains the instrumentation scope name from OTEL.
Extracted from `scope.Name()` - typically the name of the instrumentation library or SDK
that generated this metric.

`chq_scope_url` (string, nullable) contains the instrumentation scope version from OTEL.
Extracted from `scope.Version()` - the version of the instrumentation library.
Note: Despite the name, this is the scope **version**, not a URL.

Source: [ingest_proto_metrics_sorting.go:363-364](../../../internal/filereader/ingest_proto_metrics_sorting.go#L363-L364)

`chq_sketch` (bytes, required) contains the serialized DDSketch (DataDog Sketch) for this
datapoint. This is the core statistical data structure that enables accurate percentile
calculations across aggregated data. The sketch is encoded using `helpers.EncodeAndReturnSketch()`.

For single-value metrics (gauge/sum), the sketch contains exactly one value.
For histogram metrics, the sketch reconstructs the distribution from bucket counts.

Source: [ingest_proto_metrics_sorting.go:501](../../../internal/filereader/ingest_proto_metrics_sorting.go#L501)

`chq_telemetry_type` (string, required) is always set to the constant `"metrics"` for all
metric records. Used to distinguish telemetry types in unified queries.

`chq_tid` (int64, required) is the Time-series ID, a 64-bit FNV-1a hash that uniquely
identifies a metric time series. Computed from:

1. `metric_name` (normalized)
2. `chq_metric_type`
3. Selected `resource_*` attributes (see KeepResourceKeys below)
4. All `attr_*` attributes (datapoint attributes, excluding underscore-prefixed)

The TID enables efficient grouping of datapoints belonging to the same time series
for aggregation, rollup, and query optimization.

Source: [fingerprinter/tid.go:153-229](../../../internal/oteltools/pkg/fingerprinter/tid.go#L153-L229)

`chq_timestamp` (int64, required) represents milliseconds since the Unix epoch. Derived from
the OTEL datapoint timestamp using the following priority:

1. `datapoint.Timestamp()` (converted to milliseconds)
2. `datapoint.StartTimestamp()` if Timestamp is missing
3. Current system time at processing (fallback)

During processing, timestamps are truncated to 10-second intervals for rollup alignment:
`(ts / 10000) * 10000`

Source: [ingest_proto_metrics_sorting.go:523-544](../../../internal/filereader/ingest_proto_metrics_sorting.go#L523-L544)

`chq_tsns` (int64, required) represents the original timestamp in nanoseconds. Preserves
the full precision of the OTEL timestamp for cases where sub-millisecond accuracy is needed.

`chq_unit` (string, nullable) contains the metric unit from OTEL metadata. Extracted from
`metric.Unit()` - follows the OTEL unit format (e.g., "ms", "By", "{requests}").

### Rollup Fields

These fields contain pre-computed statistical aggregations extracted from the DDSketch.
For single-value metrics (gauge/sum), all percentile fields contain the same value.

`chq_rollup_avg` (float64, required) is the average value. Computed as `sum / count` from
the sketch.

`chq_rollup_count` (float64, required) is the number of observations. For single datapoints,
this is always 1.0. For aggregated data, this reflects the total observation count.

`chq_rollup_max` (float64, required) is the maximum observed value from the sketch.

`chq_rollup_min` (float64, required) is the minimum observed value from the sketch.

`chq_rollup_sum` (float64, required) is the sum of all observations from the sketch.

`chq_rollup_p25` (float64, required) is the 25th percentile value from the sketch.

`chq_rollup_p50` (float64, required) is the 50th percentile (median) value from the sketch.

`chq_rollup_p75` (float64, required) is the 75th percentile value from the sketch.

`chq_rollup_p90` (float64, required) is the 90th percentile value from the sketch.

`chq_rollup_p95` (float64, required) is the 95th percentile value from the sketch.

`chq_rollup_p99` (float64, required) is the 99th percentile value from the sketch.

Source: [ingest_proto_metrics_sorting.go:778-818](../../../internal/filereader/ingest_proto_metrics_sorting.go#L778-L818)

## metric_name Field

`metric_name` (string, required) is the normalized metric name. The original OTEL metric
name is normalized using `wkk.NormalizeName()`:

- Lowercase conversion
- Dots (`.`) and other non-alphanumeric characters replaced with underscores (`_`)

Example: `http.server.request.duration` becomes `http_server_request_duration`

Source: [wkk/intern.go:32-59](../../../pipeline/wkk/intern.go#L32-L59)

## Legacy Fields (deprecated)

These fields appear in some existing parquet files but are not generated by current
ingestion code. They may be present in older data or data from external sources.

`_cardinalhq_bucket_prefix` (string, nullable) - Legacy field containing the S3 object path
prefix. Used for data lineage in older files.

`_cardinalhq_collector_id` (string, nullable) - Legacy field containing the collector identifier.
Maps to the modern `chq_collector_id` concept but with different naming.

`_cardinalhq_customer_id` (string, nullable) - Legacy field equivalent to `chq_customer_id`.
Present for backward compatibility with older query APIs.

Source: [label_helpers.go:31](../../../internal/parquetwriter/factories/label_helpers.go#L31)

## Attribute Mapping Rules

Resource, scope, and datapoint attributes from OTEL are flattened and stored with prefixes.

### Attribute Prefixes and Sources

- `resource_*` - Attributes from the OTEL Resource object, containing deployment/infrastructure metadata
- `scope_*` - Attributes from the OTEL Scope/InstrumentationScope
- `attr_*` - Attributes from metric datapoints (gauge values, sum values, histogram datapoints)

### Resource Attribute Filtering

Unlike logs, metrics apply strict filtering to resource attributes. Only the following
keys are retained (after normalization):

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

Other resource attributes are discarded during ingestion to reduce cardinality and storage.

Source: [fingerprinter/tid.go:118-133](../../../internal/oteltools/pkg/fingerprinter/tid.go#L118-L133)

### Datapoint Attributes

All datapoint attributes are retained with the `attr_` prefix, except:

- Attributes with names starting with underscore (`_`) are skipped (internal/special attributes)
- Empty string values are skipped

### Attribute Name Normalization

OTEL attribute keys are normalized using `wkk.NormalizeName()`:

- Dots in attribute names are converted to underscores (e.g., `http.method` becomes `attr_http_method`)
- All characters are lowercased
- Non-alphanumeric characters become underscores

### Type Handling

Attribute values maintain their original types where possible:

- String values: Stored as strings
- Integer values: Stored as int64
- Float values: Stored as float64
- Boolean values: Converted to strings (`"true"`, `"false"`) to avoid type mismatches
- Maps/Slices: Converted to JSON string representation
- Empty values: Converted to strings

Source: [otel_schema.go:261-289](../../../internal/filereader/otel_schema.go#L261-L289)

## OTEL Metric Type Processing

Different OTEL metric types undergo different processing to generate the sketch and rollup fields.

### Gauge Metrics

Single numeric value at a point in time.

- Value extracted from `datapoint.DoubleValue()` or `datapoint.IntValue()`
- Single-value sketch created using `ddcache.Get().GetBytesForValue()`
- All rollup percentile fields set to the same value
- `chq_metric_type` = `"gauge"`

### Sum Metrics (Counters)

Cumulative or delta counters.

- Value extracted from `datapoint.DoubleValue()` or `datapoint.IntValue()`
- Processed identically to gauges for sketch creation
- `chq_metric_type` = `"count"`

### Histogram Metrics

Explicit bucket histograms with counts and bounds.

- Bucket counts and explicit bounds extracted from datapoint
- Values reconstructed from buckets using `metricmath.ConvertHistogramToValues()`
- Handles underflow bucket (below first bound) and overflow bucket (above last bound)
- `chq_metric_type` = `"histogram"`

Source: [ingest_proto_metrics_sorting.go:546-679](../../../internal/filereader/ingest_proto_metrics_sorting.go#L546-L679)

### Exponential Histogram Metrics

Logarithmic bucket histograms with scale and offset.

- Scale, zero count, positive/negative buckets extracted
- Converted to values using `metricmath.ConvertExponentialHistogramToValues()`
- Supports both positive and negative value ranges
- `chq_metric_type` = `"histogram"`

Source: [ingest_proto_metrics_sorting.go:681-744](../../../internal/filereader/ingest_proto_metrics_sorting.go#L681-L744)

### Summary Metrics

Pre-computed quantile summaries.

- Quantile values and counts extracted from datapoint
- Converted to DDSketch using `summaryToDDSketch()`
- `chq_metric_type` = `"histogram"`

Source: [ingest_proto_metrics_sorting.go:746-776](../../../internal/filereader/ingest_proto_metrics_sorting.go#L746-L776)

## Null Handling

If a particular row has a `null` value for an attribute, this typically means:

1. That specific metric datapoint did not have that attribute present in the original OTEL data
2. Other rows in the same Parquet file do have this attribute (creating the column in the schema)
3. The resource attribute was filtered out during ingestion

## File Sorting and Layout

Metrics parquet files are sorted by `[metric_name, chq_tid, chq_timestamp]` for optimal query performance:

- Grouping by `metric_name` enables efficient metric-specific queries
- Grouping by `chq_tid` keeps all datapoints for a time series together
- Sorting by `chq_timestamp` within a time series enables efficient range scans

Source: [factories/metrics.go:31-53](../../../internal/parquetwriter/factories/metrics.go#L31-L53)

## Complete OTEL → Parquet Examples

### Example 1: Simple Counter Metric

**OTEL Input:**

```protobuf
name: "http.server.request.count"
description: "Number of HTTP requests"
unit: "{requests}"
type: Sum
datapoints: [
  {
    timestamp: 1640995200000000000  # nanoseconds
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

**Parquet Output:**

```text
metric_name: "http_server_request_count"
chq_customer_id: "org-123" (from ingestion context)
chq_description: "Number of HTTP requests"
chq_metric_type: "count"
chq_scope_name: "opentelemetry-go"
chq_scope_url: "1.21.0"
chq_sketch: <encoded DDSketch bytes>
chq_telemetry_type: "metrics"
chq_tid: -1234567890123456789 (computed FNV-1a hash)
chq_timestamp: 1640995200000 (truncated to 10s interval)
chq_tsns: 1640995200000000000
chq_unit: "{requests}"
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

### Example 2: Histogram Metric

**OTEL Input:**

```protobuf
name: "http.server.request.duration"
description: "HTTP request latency"
unit: "ms"
type: Histogram
datapoints: [
  {
    timestamp: 1640995200000000000
    count: 100
    sum: 5000.0
    explicit_bounds: [10, 50, 100, 500, 1000]
    bucket_counts: [5, 30, 40, 20, 4, 1]
    attributes: [
      {key: "http.route", value: "/api/users"}
    ]
  }
]
resource: {
  attributes: [
    {key: "service.name", value: "api-gateway"}
  ]
}
```

**Parquet Output:**

```text
metric_name: "http_server_request_duration"
chq_metric_type: "histogram"
chq_sketch: <encoded DDSketch with distribution>
chq_rollup_avg: 50.0 (5000/100)
chq_rollup_count: 100.0
chq_rollup_max: ~1000.0 (estimated from overflow bucket)
chq_rollup_min: ~5.0 (estimated from first bucket)
chq_rollup_sum: 5000.0
chq_rollup_p25: ~25.0 (from sketch)
chq_rollup_p50: ~45.0 (from sketch)
chq_rollup_p75: ~80.0 (from sketch)
chq_rollup_p90: ~200.0 (from sketch)
chq_rollup_p95: ~400.0 (from sketch)
chq_rollup_p99: ~800.0 (from sketch)

resource_service_name: "api-gateway"
attr_http_route: "/api/users"
```

## Edge Cases and Error Handling

### Dropped Datapoints

Datapoints are dropped (not written to parquet) in the following cases:

- NaN or Infinity values for gauge/sum metrics
- Empty metric name (after normalization)
- Histogram with no bucket counts
- Exponential histogram with no data
- Summary with zero count or no quantile values

### Timestamp Handling

- Zero or missing timestamp: Falls back to start_timestamp, then current time
- Timestamps are truncated to 10-second boundaries for rollup alignment
- Nanosecond precision preserved in `chq_tsns`

### TID Computation Consistency

The TID must be computed identically during sorting and row building. The implementation
uses `ComputeTIDFromOTEL()` during the sort phase and passes the result to row building
to ensure consistency. Tests validate that this produces identical values to computing
TID from the materialized row.

## Schema Evolution and Compatibility

### Required Fields (Never Remove)

These fields must always be present in metrics Parquet files:

- `metric_name` - Required for metric identification
- `chq_tid` - Required for time series grouping
- `chq_timestamp` - Required for time-based queries
- `chq_tsns` - Required for full timestamp precision
- `chq_metric_type` - Required for query semantics
- `chq_customer_id` - Required for data isolation
- `chq_telemetry_type` - Always "metrics"
- `chq_sketch` - Required for percentile calculations
- All `chq_rollup_*` fields - Required for aggregation queries

### Optional Fields (Nullable)

These fields may be null/missing depending on source data:

- `chq_description` - Null if no description in OTEL
- `chq_unit` - Null if no unit in OTEL
- `chq_scope_name` - Null if no scope name
- `chq_scope_url` - Null if no scope version
- All `resource_*` fields - Depend on filtered source OTEL data
- All `attr_*` fields - Depend on source OTEL data

### Legacy Field Compatibility

When reading older parquet files:

- `_cardinalhq_*` fields should be mapped to their `chq_*` equivalents
- Query APIs should handle both naming conventions
