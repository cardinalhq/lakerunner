---
sidebar_position: 4
---

# Traces Parquet Schema

Traces Parquet files contain flattened OpenTelemetry span data optimized for distributed tracing queries.

## Span Fields

Core span data from the OpenTelemetry Span object:

### span_trace_id

**Type:** string (required)

32-character hex-encoded trace ID that groups all spans in the same trace.

### span_id

**Type:** string (required)

16-character hex-encoded span ID uniquely identifying this span within a trace.

### span_parent_span_id

**Type:** string (nullable)

16-character hex-encoded parent span ID. Empty string for root spans.

### span_name

**Type:** string (required)

Operation name. Primary human-readable identifier for the operation being traced.

### span_kind

**Type:** string (required)

Relationship between the span and its parent:

| Value | Description |
| ----- | ----------- |
| `SPAN_KIND_UNSPECIFIED` | Default, relationship unknown |
| `SPAN_KIND_INTERNAL` | Internal operation within an application |
| `SPAN_KIND_SERVER` | Server side of synchronous RPC |
| `SPAN_KIND_CLIENT` | Client side of synchronous RPC |
| `SPAN_KIND_PRODUCER` | Initiator of async request |
| `SPAN_KIND_CONSUMER` | Receiver of async request |

### span_status_code

**Type:** string (required)

Operation status:

| Value | Description |
| ----- | ----------- |
| `STATUS_CODE_UNSET` | Default status |
| `STATUS_CODE_OK` | Completed successfully |
| `STATUS_CODE_ERROR` | Operation failed |

### span_status_message

**Type:** string (nullable)

Human-readable status message, typically populated when status is ERROR.

### span_end_timestamp

**Type:** int64 (required)

Span end time in milliseconds since Unix epoch.

### span_duration

**Type:** int64 (required)

Span duration in milliseconds (`span_end_timestamp - chq_timestamp`). Set to 0 if either timestamp required a fallback.

## System Fields

### chq_id

**Type:** string (required)

Unique identifier for this span row. Generated using `idgen.NextBase32ID()`.

### chq_timestamp

**Type:** int64 (required)

Span start time in milliseconds since Unix epoch. Falls back to current time if start timestamp is zero.

### chq_tsns

**Type:** int64 (required)

Span start time in nanoseconds for full precision.

### chq_fingerprint

**Type:** int64 (required)

Semantic fingerprint that groups similar spans. Computed using xxhash from a combination of resource and span attributes based on span type.

### chq_telemetry_type

**Type:** string (required)

Always `"traces"` for span records.

### chq_customer_id

**Type:** string (nullable)

Organization ID that owns this trace data.

## Fingerprint Calculation

The fingerprint groups semantically similar spans for analysis.

### Base Components

All fingerprints include:

1. `resource_k8s_cluster_name` (or `"unknown"`)
2. `resource_k8s_namespace_name` (or `"unknown"`)
3. `resource_service_name` (or `"unknown"`)
4. `span_kind`

### Pattern-Specific Components

Checked in order; first match determines pattern:

**Messaging Pattern** (when `attr_messaging_system` present):

| Component |
| --------- |
| `attr_messaging_system` |
| `attr_messaging_operation_type` |
| `attr_messaging_destination_name` |

**Database Pattern** (when `attr_db_system_name` present):

| Component |
| --------- |
| `span_name` |
| `attr_db_system_name` |
| `attr_db_namespace` |
| `attr_db_operation_name` |
| `attr_server_address` |
| `attr_db_collection_name` |

**HTTP Pattern** (when `attr_http_request_method` present):

| Component |
| --------- |
| `attr_http_request_method` |
| `attr_url_template` |

**Default Pattern** (no specific system detected):

| Component |
| --------- |
| `span_name` |

### Hash Computation

Components joined with `"##"` separator and hashed with xxhash to produce signed int64.

## Attribute Mapping

### Prefixes

| Prefix | Source |
| ------ | ------ |
| `resource_*` | OTEL Resource attributes |
| `scope_*` | OTEL Scope/InstrumentationScope attributes |
| `attr_*` | Span attributes |

### Name Normalization

OTEL attribute keys normalized via `NormalizeName()`:

- Dots converted to underscores
- All characters lowercased
- Non-alphanumeric characters become underscores

Example: `http.request.method` → `attr_http_request_method`

### Type Handling

| Source Type | Storage |
| ----------- | ------- |
| Empty values | Nil |
| Bytes | Raw byte array |
| All others | String via `AsString()` |

## Common Resource Attributes

| Attribute | Description |
| --------- | ----------- |
| `resource_service_name` | Service that generated the span |
| `resource_service_version` | Service version |
| `resource_k8s_cluster_name` | Kubernetes cluster |
| `resource_k8s_namespace_name` | Kubernetes namespace |
| `resource_k8s_pod_name` | Kubernetes pod |
| `resource_k8s_deployment_name` | Kubernetes deployment |

## Common Span Attributes

### HTTP Spans

| Attribute | Description |
| --------- | ----------- |
| `attr_http_request_method` | HTTP method (GET, POST, etc.) |
| `attr_url_template` | URL pattern/template |
| `attr_http_response_status_code` | HTTP status code |

### Database Spans

| Attribute | Description |
| --------- | ----------- |
| `attr_db_system_name` | Database system (postgresql, mysql, etc.) |
| `attr_db_namespace` | Database name |
| `attr_db_operation_name` | Operation (SELECT, INSERT, etc.) |
| `attr_db_collection_name` | Table or collection name |
| `attr_server_address` | Database host |

### Messaging Spans

| Attribute | Description |
| --------- | ----------- |
| `attr_messaging_system` | Messaging system (kafka, rabbitmq, etc.) |
| `attr_messaging_operation_type` | Operation (publish, receive, etc.) |
| `attr_messaging_destination_name` | Queue or topic name |

## Example Transformation

### OTEL Input

```text
trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "6e0c63257de34c92"
parent_span_id: "1c3a5f19d2e4b8a7"
name: "GET /api/users/:id"
kind: SPAN_KIND_SERVER
status: { code: STATUS_CODE_OK }
start_time: 1640995200000000000  # nanoseconds
end_time: 1640995200050000000    # 50ms later
attributes: [
  {key: "http.request.method", value: "GET"},
  {key: "url.template", value: "/api/users/:id"},
  {key: "http.response.status_code", value: 200}
]
resource: {
  attributes: [
    {key: "service.name", value: "api-gateway"},
    {key: "k8s.namespace.name", value: "production"},
    {key: "k8s.cluster.name", value: "us-west-2"}
  ]
}
scope: {
  name: "opentelemetry-go"
  version: "1.21.0"
}
```

### Parquet Output

```text
span_trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "6e0c63257de34c92"
span_parent_span_id: "1c3a5f19d2e4b8a7"
span_name: "GET /api/users/:id"
span_kind: "SPAN_KIND_SERVER"
span_status_code: "STATUS_CODE_OK"
span_status_message: ""
span_end_timestamp: 1640995200050
span_duration: 50

chq_timestamp: 1640995200000
chq_tsns: 1640995200000000000
chq_telemetry_type: "traces"
chq_id: "a1b2c3d4e5f6g7h8"
chq_fingerprint: 1234567890123456789

resource_service_name: "api-gateway"
resource_k8s_namespace_name: "production"
resource_k8s_cluster_name: "us-west-2"

scope_name: "opentelemetry-go"
scope_version: "1.21.0"

attr_http_request_method: "GET"
attr_url_template: "/api/users/:id"
attr_http_response_status_code: "200"
```

## Edge Cases

### Timestamp Handling

| Condition | Behavior |
| --------- | -------- |
| Zero start timestamp | Falls back to current system time |
| Zero end timestamp | Falls back to current system time |
| Either timestamp fallback | Duration set to 0 |

### Missing Fingerprint Components

| Missing | Replacement |
| ------- | ----------- |
| `resource_k8s_cluster_name` | `"unknown"` |
| `resource_k8s_namespace_name` | `"unknown"` |
| `resource_service_name` | `"unknown"` |
| Pattern-specific attributes | Empty string in hash |

## File Sorting

Sorted by `[span_trace_id, chq_timestamp]`:

- Groups all spans of a trace together
- Orders spans chronologically within each trace
- Optimizes trace assembly queries

## Required Fields

- `span_trace_id`
- `span_id`
- `span_name`
- `span_kind`
- `span_status_code`
- `span_duration`
- `chq_timestamp`
- `chq_tsns`
- `chq_telemetry_type`
- `chq_fingerprint`
- `chq_id`

## Optional Fields

- `span_parent_span_id` (empty for root spans)
- `span_status_message`
- `chq_customer_id`
- All `resource_*` fields
- All `scope_*` fields
- All `attr_*` fields
