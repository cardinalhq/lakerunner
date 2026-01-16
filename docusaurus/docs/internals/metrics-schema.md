---
sidebar_position: 3
---

# Metrics Parquet Schema

See [Schema Conventions](./overview.md#schema-conventions) for field prefixes, name normalization, and [type coercion](./overview.md#type-coercion).

## Quick Reference

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `metric_name` | string | yes | Normalized metric name |
| `chq_tid` | int64 | yes | Time-series ID (FNV-1a hash) |
| `chq_timestamp` | int64 | yes | Milliseconds since epoch (10s aligned) |
| `chq_tsns` | int64 | yes | Nanoseconds since epoch |
| `chq_metric_type` | string | yes | `gauge` / `count` / `histogram` |
| `chq_sketch` | bytes | yes | Serialized DDSketch |
| `chq_customer_id` | string | yes | Organization ID |
| `chq_telemetry_type` | string | yes | Always `"metrics"` |
| `chq_description` | string | no | Metric description |
| `chq_unit` | string | no | Unit (e.g., "ms", "By") |
| `chq_scope_name` | string | no | Instrumentation scope name |
| `chq_scope_url` | string | no | Instrumentation scope version |

## Rollup Fields

| Field | Description |
| ----- | ----------- |
| `chq_rollup_count` | Observation count |
| `chq_rollup_sum` | Sum of values |
| `chq_rollup_min` | Minimum value |
| `chq_rollup_max` | Maximum value |
| `chq_rollup_avg` | Average (sum/count) |
| `chq_rollup_p25` | 25th percentile |
| `chq_rollup_p50` | Median |
| `chq_rollup_p75` | 75th percentile |
| `chq_rollup_p90` | 90th percentile |
| `chq_rollup_p95` | 95th percentile |
| `chq_rollup_p99` | 99th percentile |

All rollup fields are `float64`, required.

## Metric Type Mapping

| OTEL Type | `chq_metric_type` |
| --------- | ----------------- |
| Gauge | `gauge` |
| Sum | `count` |
| Histogram | `histogram` |
| ExponentialHistogram | `histogram` |
| Summary | `histogram` |

## Attribute Prefixes

| Prefix | Source |
| ------ | ------ |
| `resource_*` | OTEL Resource (filtered) |
| `attr_*` | Datapoint attributes |

### Retained Resource Attributes

Only these resource attributes are kept (others discarded):

- `resource_service_name`, `resource_service_version`
- `resource_k8s_cluster_name`, `resource_k8s_namespace_name`
- `resource_k8s_pod_name`, `resource_k8s_pod_ip`
- `resource_k8s_deployment_name`, `resource_k8s_daemonset_name`, `resource_k8s_statefulset_name`
- `resource_container_image_name`, `resource_container_image_tag`
- `resource_app`

## Time-Series ID (TID)

FNV-1a hash computed from:
1. `metric_name`
2. `chq_metric_type`
3. Retained resource attributes
4. All `attr_*` attributes

Same TID = same time series.

## Sorting

Files sorted by: `[metric_name, chq_tid, chq_timestamp]`

## Name Normalization

`http.server.request.duration` → `http_server_request_duration`

## Example

**Input (OTEL Sum):**
```
name: "http.request.count"
unit: "1"
type: Sum
value: 42
attributes: {http.method: "GET", status: 200}
resource: {service.name: "api"}
```

**Output (Parquet):**
```
metric_name: "http_request_count"
chq_tid: -1234567890123456789
chq_timestamp: 1640995200000
chq_metric_type: "count"
chq_sketch: <DDSketch bytes>
chq_rollup_count: 1.0
chq_rollup_sum: 42.0
chq_rollup_min: 42.0
chq_rollup_max: 42.0
chq_rollup_avg: 42.0
chq_rollup_p50: 42.0
chq_rollup_p95: 42.0
chq_rollup_p99: 42.0
resource_service_name: "api"
attr_http_method: "GET"
attr_status: 200
```
