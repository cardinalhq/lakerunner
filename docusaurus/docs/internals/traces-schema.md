---
sidebar_position: 4
---

# Traces Parquet Schema

See [Schema Conventions](./overview.md#schema-conventions) for field prefixes, name normalization, and [type coercion](./overview.md#type-coercion).

## Quick Reference

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `span_trace_id` | string | yes | 32-char hex trace ID |
| `span_id` | string | yes | 16-char hex span ID |
| `span_parent_span_id` | string | no | Parent span ID (empty for root) |
| `span_name` | string | yes | Operation name |
| `span_kind` | string | yes | Span relationship type |
| `span_status_code` | string | yes | Operation status |
| `span_status_message` | string | no | Status message (usually on error) |
| `span_end_timestamp` | int64 | yes | End time (ms since epoch) |
| `span_duration` | int64 | yes | Duration in milliseconds |
| `chq_id` | string | yes | Unique row ID |
| `chq_timestamp` | int64 | yes | Start time (ms since epoch) |
| `chq_tsns` | int64 | yes | Start time (ns since epoch) |
| `chq_fingerprint` | int64 | yes | Semantic span hash |
| `chq_telemetry_type` | string | yes | Always `"traces"` |
| `chq_customer_id` | string | no | Organization ID |

## Span Kind Values

| Value | Description |
| ----- | ----------- |
| `SPAN_KIND_UNSPECIFIED` | Unknown |
| `SPAN_KIND_INTERNAL` | Internal operation |
| `SPAN_KIND_SERVER` | Server-side RPC |
| `SPAN_KIND_CLIENT` | Client-side RPC |
| `SPAN_KIND_PRODUCER` | Async request initiator |
| `SPAN_KIND_CONSUMER` | Async request receiver |

## Status Code Values

| Value | Description |
| ----- | ----------- |
| `STATUS_CODE_UNSET` | Default |
| `STATUS_CODE_OK` | Success |
| `STATUS_CODE_ERROR` | Failure |

## Attribute Prefixes

| Prefix | Source |
| ------ | ------ |
| `resource_*` | OTEL Resource |
| `scope_*` | OTEL InstrumentationScope |
| `attr_*` | Span attributes |

## Fingerprint Calculation

Base components (all spans):
- `resource_k8s_cluster_name` (or "unknown")
- `resource_k8s_namespace_name` (or "unknown")
- `resource_service_name` (or "unknown")
- `span_kind`

Pattern-specific components (first match):

| Pattern | Detected By | Additional Components |
| ------- | ----------- | --------------------- |
| Messaging | `attr_messaging_system` | system, operation_type, destination_name |
| Database | `attr_db_system_name` | span_name, system, namespace, operation, server, collection |
| HTTP | `attr_http_request_method` | method, url_template |
| Default | (none) | span_name |

Components joined with `##`, hashed with xxhash.

## Common Attributes

### HTTP Spans
- `attr_http_request_method`: GET, POST, etc.
- `attr_url_template`: URL pattern
- `attr_http_response_status_code`: HTTP status

### Database Spans
- `attr_db_system_name`: postgresql, mysql, etc.
- `attr_db_namespace`: Database name
- `attr_db_operation_name`: SELECT, INSERT, etc.
- `attr_db_collection_name`: Table name
- `attr_server_address`: Database host

### Messaging Spans
- `attr_messaging_system`: kafka, rabbitmq, etc.
- `attr_messaging_operation_type`: publish, receive
- `attr_messaging_destination_name`: Queue/topic

## Sorting

Files sorted by: `[span_trace_id, chq_timestamp]`

## Example

**Input (OTEL Span):**
```
trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "6e0c63257de34c92"
parent_span_id: "1c3a5f19d2e4b8a7"
name: "GET /api/users/:id"
kind: SPAN_KIND_SERVER
status: STATUS_CODE_OK
start_time: 1640995200000000000
end_time: 1640995200050000000
attributes: {http.request.method: "GET", url.template: "/api/users/:id"}
resource: {service.name: "api", k8s.namespace.name: "prod"}
```

**Output (Parquet):**
```
span_trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "6e0c63257de34c92"
span_parent_span_id: "1c3a5f19d2e4b8a7"
span_name: "GET /api/users/:id"
span_kind: "SPAN_KIND_SERVER"
span_status_code: "STATUS_CODE_OK"
span_duration: 50
chq_timestamp: 1640995200000
chq_fingerprint: 1234567890123456789
chq_telemetry_type: "traces"
resource_service_name: "api"
resource_k8s_namespace_name: "prod"
attr_http_request_method: "GET"
attr_url_template: "/api/users/:id"
```
