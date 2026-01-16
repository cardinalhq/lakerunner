---
sidebar_position: 2
---

# Logs Parquet Schema

See [Schema Conventions](./overview.md#schema-conventions) for field prefixes, name normalization, and [type coercion](./overview.md#type-coercion).

## Quick Reference

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `chq_id` | string | yes | Unique record ID |
| `chq_timestamp` | int64 | yes | Milliseconds since epoch |
| `chq_tsns` | int64 | yes | Nanoseconds since epoch |
| `chq_fingerprint` | int64 | no | Log pattern hash |
| `chq_telemetry_type` | string | yes | Always `"logs"` |
| `log_level` | string | no | TRACE/DEBUG/INFO/WARN/ERROR/FATAL |
| `log_message` | string | no | Log body content |
| `metric_name` | string | yes | Always `"log_events"` |
| `resource_bucket_name` | string | yes | Source S3 bucket |
| `resource_file_name` | string | yes | Source object path |
| `resource_file_type` | string | yes | Normalized filename |

## Attribute Prefixes

| Prefix | Source |
| ------ | ------ |
| `resource_*` | OTEL Resource attributes |
| `scope_*` | OTEL InstrumentationScope |
| `attr_*` | Log record attributes |

## Field Details

### System Fields

**chq_id**: Base32-encoded unique identifier for UI lookups.

**chq_timestamp**: Derived from OTEL `timestamp` → `observed_timestamp` → current time. Nanosecond values (>1e15) auto-converted to milliseconds.

**chq_tsns**: Original nanosecond timestamp preserved.

**chq_fingerprint**: Hash of normalized message with variables (IPs, IDs, timestamps) removed. Same fingerprint = same log pattern. Null if no message.

### Log Fields

**log_level**: Uppercase severity from OTEL `severity_text`.

**log_message**: String conversion of OTEL `body` field.

## Attribute Normalization

OTEL keys are normalized before storage:
- Lowercase
- Dots → underscores
- Special chars → underscores

Example: `http.request.method` → `attr_http_request_method`

## Sorting

Files sorted by: `[service_identifier, fingerprint, timestamp]`

- `service_identifier`: `resource_customer_domain` or `resource_service_name`

## Example

**Input (OTEL):**
```
timestamp: 1640995200000000000
severity_text: "ERROR"
body: "Connection failed to 10.0.0.1"
attributes: {service.name: "api", http.method: "POST"}
resource: {host.name: "server01"}
```

**Output (Parquet):**
```
chq_id: "J7K2M9P8Q1R5"
chq_timestamp: 1640995200000
chq_tsns: 1640995200000000000
chq_fingerprint: 789123456789
chq_telemetry_type: "logs"
log_level: "ERROR"
log_message: "Connection failed to 10.0.0.1"
metric_name: "log_events"
attr_service_name: "api"
attr_http_method: "POST"
resource_host_name: "server01"
```
