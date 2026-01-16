---
sidebar_position: 2
---

# Logs Parquet Schema

Log Parquet files store flattened OpenTelemetry log records optimized for search and pattern analysis.

## System Fields

Fields prefixed with `chq_` are system fields managed by Lakerunner.

### chq_id

**Type:** string (required)

Unique identifier for UI record identification. Generated using `idgen.NextBase32ID()` to ensure uniqueness within the dataset.

### chq_timestamp

**Type:** int64 (required)

Milliseconds since Unix epoch. Derived with priority:

1. OTEL `timestamp` field (converted from nanoseconds if > 1e15)
2. OTEL `observed_timestamp` field
3. Current system time at processing

### chq_tsns

**Type:** int64 (required)

Original timestamp in nanoseconds, preserving full OTEL precision for sub-millisecond accuracy.

### chq_fingerprint

**Type:** int64 (nullable)

Message pattern fingerprint. Messages with the same fingerprint represent the same log pattern with different variable values. Variable content (IPs, IDs, timestamps) is removed before fingerprinting. Null if no message content available.

### chq_telemetry_type

**Type:** string (required)

Always `"logs"` for log records.

### chq_value

**Type:** float64 (deprecated)

Still written with value `1.0` for backward compatibility. Should be ignored by new code.

## Log Fields

### log_level

**Type:** string (nullable)

Log severity in uppercase. Values: TRACE, DEBUG, INFO, WARN, ERROR, FATAL, or empty string. Derived from OTEL `severity_text` field.

### log_message

**Type:** string (nullable)

Human-readable log content from the OTEL `body` field converted via `body.AsString()`. May be null if no message content available.

### metric_name

**Type:** string (required)

Always `"log_events"` for all log records.

## Resource Metadata Fields

Added automatically during ingestion:

### resource_bucket_name

**Type:** string (required)

S3 bucket name where the original log file was stored.

### resource_file_name

**Type:** string (required)

Object path/key with "./" prefix (e.g., "./logs/2022-01-01/app.pb").

### resource_file_type

**Type:** string (required)

Derived from filename by removing non-alphanumeric characters and converting to lowercase. Used for file type identification and routing.

## Attribute Mapping

### Prefixes

| Prefix | Source |
| ------ | ------ |
| `resource_*` | OTEL Resource attributes (deployment/infrastructure metadata) |
| `scope_*` | OTEL Scope/InstrumentationScope attributes |
| `attr_*` | Log record attributes |
| `log_*` | System log fields (log_message, log_level) |

### Name Normalization

OTEL attribute keys are normalized:

- All characters converted to lowercase
- Dots and special characters converted to underscores
- Example: `http.method` → `attr_http_method`

### Type Coercion

All attribute values undergo string conversion:

| Source Type | Result |
| ----------- | ------ |
| Boolean | `"true"` or `"false"` |
| Numeric | String representation |
| Array | JSON string |
| Object | JSON string |
| Null | Parquet null |

## Example Transformation

### OTEL Input

```text
timestamp: 1640995200000000000  # nanoseconds
severity_text: "ERROR"
body: "Database connection failed"
attributes: [
  {key: "service.name", value: "api-gateway"},
  {key: "http.method", value: "POST"},
  {key: "error.code", value: 500}
]
resource: {
  attributes: [
    {key: "host.name", value: "server01"},
    {key: "container.id", value: "abc123"}
  ]
}
scope: {
  name: "my-app-logger"
  version: "2.1.0"
}
```

### Parquet Output

```text
chq_fingerprint: 789123456789
chq_id: "J7K2M9P8Q1R5"
chq_timestamp: 1640995200000
chq_tsns: 1640995200000000000
chq_telemetry_type: "logs"
chq_value: 1.0
log_level: "ERROR"
log_message: "Database connection failed"
metric_name: "log_events"

attr_service_name: "api-gateway"
attr_http_method: "POST"
attr_error_code: "500"

resource_bucket_name: "my-logs-bucket"
resource_file_name: "./2022-01-01/app-logs.pb"
resource_file_type: "20220101applogspb"
resource_host_name: "server01"
resource_container_id: "abc123"

scope_name: "my-app-logger"
scope_version: "2.1.0"
```

## Edge Cases

### Timestamp Handling

| Condition | Behavior |
| --------- | -------- |
| Zero or missing | Uses observed_timestamp, then current time |
| Negative | Uses fallback |
| Nanoseconds (> 1e15) | Converted to milliseconds |
| Future (> now + 1 hour) | Preserved with warning |

### Message Processing

| Condition | Result |
| --------- | ------ |
| Empty string | `log_message: ""` |
| Null/missing body | `log_message: null` |
| Messages > 64KB | May be truncated |
| Invalid UTF-8 | Replaced with Unicode replacement character |

### Fingerprint Generation

| Condition | Result |
| --------- | ------ |
| Empty message | `chq_fingerprint: null` |
| Fingerprinter error | `chq_fingerprint: null` with error logged |
| Very long messages | Fingerprinted on truncated content |

## Required Fields

These fields must always be present:

- `chq_id`
- `chq_timestamp`
- `chq_tsns`
- `chq_telemetry_type`
- `metric_name`
- `resource_bucket_name`
- `resource_file_name`
- `resource_file_type`

## Optional Fields

These fields may be null depending on source data:

- `chq_fingerprint`
- `chq_value` (deprecated)
- `log_level`
- `log_message`
- All `resource_*`, `scope_*`, and `attr_*` attributes

## Performance Considerations

### Field Size Limits

- `log_message`: Recommended max 64KB
- Attribute values: Recommended max 32KB
- Total row size: Target < 1MB for optimal compression

### Compression

- Log messages: High compression due to repeated patterns
- Timestamps: Excellent compression in sorted data
- Dictionary encoding used for low-cardinality string fields

### Storage Layout

- Partitioned by `org_id` and `dateint`
- Sorted by `[service_identifier, fingerprint, timestamp]`
- Row group size: 128MB default
