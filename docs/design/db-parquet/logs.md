# Fields of the "cooked" db/ Parquet for logs

Log parquet files are a flattened version of the OpenTelemetry wire format
build for rapid searching.

## chq prefix

Fields beginning with `chq_` are system fields used by the CardinalHQ Lakerunner data lake system.

### Field Specifications and Data Types

`chq_fingerprint` (int64, nullable) represents the message body content such that
messages which are similar to one another will have the same fingerprint. Detectable items
like IP addresses, identifiers, process IDs, and other variable content are removed, and the
remaining string is fingerprinted using the fingerprinter library. Can be null if no message content
is available for fingerprinting.

`chq_id` (string, required) is used by the CardinalHQ UI to identify this record. Generated
automatically at write time using `idgen.NextBase32ID()` to ensure uniqueness within the dataset.
Format is base32-encoded string (e.g., "abc123def456").

`log_level` (string, nullable) stores the log level in uppercase format. Allowed values are:
TRACE, DEBUG, INFO, WARN, ERROR, FATAL, or empty string. Derived from the OTEL `severity_text` field
if not explicitly set. Can be null if no severity information is available.

`log_message` (string, nullable) contains the human-readable log content from the OTEL `body`
field converted to string via `body.AsString()`. May be blank or null if no message content
is available.

`metric_name` (string, required) is always set to the constant `"log_events"` for all log records.

`chq_telemetry_type` (string, required) is always set to the constant `"logs"` for all log records.

`chq_timestamp` (int64, required) represents milliseconds since the Unix epoch. Derived using
the following priority order:

1. OTEL `timestamp` field (converted from nanoseconds if > 1e15)
2. OTEL `observed_timestamp` field (converted from nanoseconds if > 1e15)
3. Current system time at processing (if both above are missing or invalid)

Must be a positive integer representing a valid timestamp.

`chq_value` (float64, deprecated) is still written with value `1.0` for backward compatibility
but should be ignored by new code. May be removed in a future version.

`chq_tsns` (int64, required) represents the original timestamp in nanoseconds, preserving
the full precision of the OTEL timestamp for cases where sub-millisecond accuracy is needed.
Derived from the raw OTEL timestamp without conversion.

## Field Generation Rules

### Resource Metadata Fields

These fields are automatically added during the ingestion process:

`resource_bucket_name` (string, required) - The S3 bucket name where the original log file was stored.

`resource_file_name` (string, required) - The object path/key with a "./" prefix (e.g., "./logs/2022-01-01/app.pb").

`resource_file_type` (string, required) - Derived from the filename by removing non-alphanumeric characters and converting to lowercase. Used for file type identification and routing.

### Standard Resource Fields

Common resource attributes that may be present:

`resource_service_name` (string, nullable) - Service identifier from OTEL resource attributes.
`resource_host_name` (string, nullable) - Host/server name from OTEL resource attributes.
`resource_container_name` (string, nullable) - Container identifier from OTEL resource attributes.

## Attribute Mapping Rules

Resource, scope, and log attributes from OTEL are flattened and stored with prefixes. All attribute values are converted to their string representation, regardless of the original type.

### Attribute Prefixes and Sources

* `resource_*` - Attributes from the OTEL Resource object, containing deployment/infrastructure metadata
* `scope_*` - Attributes from the OTEL Scope/InstrumentationScope, including library name, version, and schema URL
* `attr_*` - Attributes specific to individual log records from the OTEL LogRecord (formerly `log_*`)
* `log_*` - System fields for log-specific data (e.g., `log_message`, `log_level`)

### Null Handling

If a particular row has a `null` value for an attribute, this typically means:

1. That specific log record did not have that attribute present in the original OTEL data
2. Other rows in the same Parquet file do have this attribute (creating the column in the schema)
3. The attribute was explicitly set to null in the source data

### Attribute Name Normalization

* OTEL attribute keys are normalized using `NormalizeName()` after adding the prefix
* All characters are converted to lowercase
* Dots and special characters are converted to underscores
* Example: `http.method` becomes `attr_http_method`, `HTTP.Status_Code` becomes `attr_http_status_code`

### Type Coercion Rules

All attribute values undergo string conversion:

* Boolean values: `true` → `"true"`, `false` → `"false"`
* Numeric values: `123` → `"123"`, `45.67` → `"45.67"`
* Array values: `[1,2,3]` → `"[1,2,3]"` (JSON representation)
* Object values: `{"key":"val"}` → `"{\"key\":\"val\"}"` (JSON representation)
* Null values: remain as Parquet null

## Complete OTEL → Parquet Examples

These examples show how complete OTEL LogRecord protobuf messages are transformed into Parquet rows.

### Example 1: Simple Text Log

**OTEL Input:**

```protobuf
timestamp: 1640995200000000000  # 2022-01-01 00:00:00 UTC (nanoseconds)
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
    {key: "container.id", value: "abc123"},
    {key: "service.version", value: "1.2.3"}
  ]
}
scope: {
  name: "my-app-logger"
  version: "2.1.0"
  schema_url: "https://opentelemetry.io/schemas/1.9.0"
}
```

**Parquet Output:**

```text
chq_fingerprint: 789123456789 (calculated from "Database connection failed")
chq_id: "J7K2M9P8Q1R5" (generated)
chq_timestamp: 1640995200000 (converted to milliseconds)
chq_tsns: 1640995200000000000 (original nanoseconds)
chq_telemetry_type: "logs"
chq_value: 1.0 (deprecated)
log_level: "ERROR"
log_message: "Database connection failed"
metric_name: "log_events"

attr_service_name: "api-gateway"
attr_http_method: "POST"
attr_error_code: "500"

resource_bucket_name: "my-logs-bucket" (from ingestion context)
resource_file_name: "./2022-01-01/app-logs.pb" (from ingestion context)
resource_file_type: "20220101applogspb" (derived from filename)
resource_host_name: "server01"
resource_container_id: "abc123"
resource_service_version: "1.2.3"

scope_name: "my-app-logger"
scope_version: "2.1.0"
scope_schema_url: "https://opentelemetry.io/schemas/1.9.0"
```

### Example 2: JSON Body (Stored as String)

**OTEL Input:**

```protobuf
timestamp: 1640995260000000000  # 2022-01-01 00:01:00 UTC (nanoseconds)
severity_text: "WARN"
body: "{\"level\":\"warning\",\"message\":\"Rate limit exceeded\",\"user_id\":12345,\"endpoint\":\"/api/users\",\"rate\":\"100/min\"}"
attributes: [
  {key: "trace.id", value: "abc123def456"},
  {key: "span.id", value: "789xyz"}
]
resource: {
  attributes: [
    {key: "service.name", value: "rate-limiter"},
    {key: "deployment.environment", value: "production"}
  ]
}
```

**Parquet Output:**

```text
chq_fingerprint: 456789123456 (calculated from JSON string)
chq_id: "M8N2P7Q9R3S1" (generated)
chq_timestamp: 1640995260000
chq_tsns: 1640995260000000000
chq_telemetry_type: "logs"
chq_value: 1.0 (deprecated)
log_level: "WARN"
log_message: "{\"level\":\"warning\",\"message\":\"Rate limit exceeded\",\"user_id\":12345,\"endpoint\":\"/api/users\",\"rate\":\"100/min\"}"
metric_name: "log_events"

attr_trace_id: "abc123def456"
attr_span_id: "789xyz"

resource_bucket_name: "my-logs-bucket"
resource_file_name: "./2022-01-01/rate-limiter.pb"
resource_file_type: "20220101ratelimiterpb"
resource_service_name: "rate-limiter"
resource_deployment_environment: "production"
```

### Example 3: Missing Timestamp (Uses Current Time)

**OTEL Input:**

```protobuf
# timestamp field is missing or zero
severity_text: "INFO"
body: "Application started successfully"
attributes: [
  {key: "version", value: "1.0.0"}
]
resource: {
  attributes: [
    {key: "service.name", value: "my-app"}
  ]
}
```

**Parquet Output:**

```text
chq_fingerprint: 123456789123 (calculated from "Application started successfully")
chq_id: "Q5R8S2T6U9V3" (generated)
chq_timestamp: 1640995320000 (current time at processing)
chq_tsns: 1640995320000000000
chq_telemetry_type: "logs"
chq_value: 1.0 (deprecated)
log_level: "INFO"
log_message: "Application started successfully"
metric_name: "log_events"

attr_version: "1.0.0"

resource_bucket_name: "my-logs-bucket"
resource_file_name: "./2022-01-01/my-app.pb"
resource_file_type: "20220101myapppb"
resource_service_name: "my-app"
```

### Example 4: Minimal Log (Only Required Fields)

**OTEL Input:**

```protobuf
timestamp: 1640995380000000000
body: "Heartbeat"
```

**Parquet Output:**

```text
chq_fingerprint: 987654321098 (calculated from "Heartbeat")
chq_id: "T7U1V5W9X3Y8" (generated)
chq_timestamp: 1640995380000
chq_tsns: 1640995380000000000
chq_telemetry_type: "logs"
chq_value: 1.0 (deprecated)
log_level: null (no severity_text provided)
log_message: "Heartbeat"
metric_name: "log_events"

resource_bucket_name: "my-logs-bucket"
resource_file_name: "./2022-01-01/minimal.pb"
resource_file_type: "20220101minimalpb"
```

## Edge Cases and Error Handling

### Timestamp Handling

**Invalid or Missing Timestamps:**

* Zero timestamp (0) → Uses observed_timestamp if available, otherwise current time
* Negative timestamp → Uses observed_timestamp if available, otherwise current time
* Timestamp in future (> current time + 1 hour) → Logs warning, but preserves value
* Nanosecond timestamps (> 1e15) → Automatically converted to milliseconds by dividing by 1e6
* Malformed timestamp types → Uses current time and logs error

**Timestamp Conversion Examples:**

```text
Input: 1640995200000000000 (nanoseconds) → Output: 1640995200000 (milliseconds)
Input: 1640995200000 (already milliseconds) → Output: 1640995200000 (unchanged)
Input: 0 or missing → Output: 1640995400000 (current time at processing)
```

### Message and Body Processing

**Empty or Null Body:**

* Empty string `""` → `log_message: ""` (empty but present)
* Null/missing body → `log_message: null`
* Whitespace-only body → Preserved as-is

**Large Message Handling:**

* Messages > 64KB → May be truncated (implementation specific)
* Binary content in body → Converted to string representation or base64
* Invalid UTF-8 sequences → Replaced with Unicode replacement character (�)

**Body Handling:**

* Body is converted to string via `body.AsString()` and stored in `log_message`
* No JSON parsing or extraction is performed

### Severity/Level Processing

**Severity Values:**

* Numeric severity without severity_text → `log_level: null`
* Custom severity values → Preserved as-is (e.g., "CRITICAL", "LOW")
* Empty string severity_text → `log_level: ""` (empty but present)
* Severity text stored as-is from OTEL (no case conversion)

### Attribute Processing

**Special Characters in Attribute Names:**

* Dots converted: `http.method` → `attr_http_method`
* Spaces converted: `user name` → `attr_user_name`
* Special chars converted: `@metadata` → `attr_metadata`

**Large Attribute Values:**

* Values > 32KB → May be truncated
* Binary attribute values → Converted to base64 or hex representation
* Nested objects → Flattened to JSON string representation

**Attribute Type Edge Cases:**

* Boolean attributes: `true` → `"true"`, `false` → `"false"`
* Array attributes: `[1,2,3]` → `"[1,2,3]"`
* Object attributes: `{"a":"b"}` → `"{\"a\":\"b\"}"`
* Null attributes: Stored as Parquet null value

### Fingerprint Calculation

**Fingerprint Generation Failures:**

* Empty message → `chq_fingerprint: null`
* Fingerprinter error → `chq_fingerprint: null`, logs error
* Unicode normalization issues → Best-effort fingerprinting
* Very long messages → Fingerprinted on truncated content

**Fingerprint Consistency:**

* Same logical message should produce same fingerprint across runs
* Variable content (IPs, IDs, timestamps) removed before fingerprinting
* Language/encoding differences normalized

### ID Generation

**Uniqueness Guarantees:**

* IDs unique within single processing session
* Base32 format ensures URL-safe, readable identifiers
* ID generation failures → Process terminates (critical error)

## Validation Test Scenarios

### Minimal Valid Cases

1. **Absolute minimum:** Only timestamp and body
2. **Timestamp missing:** Only body field present
3. **Body missing:** Only timestamp present
4. **Empty everything:** All fields empty or null

### Comprehensive Valid Cases

1. **All fields populated:** Every possible field has valid data
2. **Maximum attribute counts:** 100+ attributes of each type
3. **Unicode content:** Messages with emoji, international characters
4. **Large payloads:** Near maximum size limits for all fields

### JSON Processing Tests

1. **Valid JSON with message:** Standard case with extractable message
2. **Valid JSON without message:** Structured data, no obvious message field
3. **Invalid JSON:** Malformed JSON that should fall back to plain text
4. **Nested JSON:** Complex nested structures with message deep inside
5. **JSON arrays:** Array bodies with/without extractable messages
6. **Mixed content:** JSON with embedded binary or special characters

### Timestamp Edge Cases

1. **Nanosecond conversion:** Various nanosecond timestamp formats
2. **Invalid ranges:** Negative, zero, far future timestamps
3. **Type mismatches:** String timestamps, floating point values
4. **Missing both:** No timestamp or observed_timestamp

### Attribute Validation

1. **Reserved names:** Attributes that conflict with chq fields
2. **Name conflicts:** Same attribute in resource, scope, and log
3. **Type variety:** All supported OTEL attribute value types
4. **Edge names:** Empty keys, very long keys, special characters

### Performance/Scale Tests

1. **Large batches:** 10K+ log records in single processing batch
2. **Wide schemas:** 1000+ unique attribute names
3. **Deep nesting:** Complex nested resource/scope structures
4. **Memory limits:** Processing near system memory constraints

### Error Recovery Tests

1. **Partial failures:** Some records succeed, others fail in batch
2. **Schema evolution:** Adding new fields mid-processing
3. **Resource exhaustion:** Disk space, memory limits during processing
4. **Network failures:** S3 connectivity issues during write

### Data Integrity Tests

1. **Round-trip validation:** OTEL → Parquet → verification of all fields
2. **Fingerprint consistency:** Same content produces same fingerprint
3. **ID uniqueness:** No duplicate IDs across large datasets
4. **Null handling:** Proper distinction between null, empty, and missing

## Schema Evolution and Compatibility

### Required Fields (Never Remove)

These fields must always be present in log Parquet files:

* `chq_id` - Required for UI record identification
* `chq_timestamp` - Required for time-based queries and partitioning
* `chq_tsns` - Required for full timestamp precision
* `chq_telemetry_type` - Always "logs"
* `metric_name` - Always "log_events"
* `resource_bucket_name` - Required for data lineage
* `resource_file_name` - Required for data lineage
* `resource_file_type` - Required for processing routing

### Optional Fields (Nullable)

These fields may be null/missing depending on source data:

* `chq_fingerprint` - Null if message cannot be fingerprinted
* `chq_value` - Deprecated, still written for backward compatibility
* `log_level` - Null if no severity information available
* `log_message` - Null if no extractable message content
* All `resource_*` and `scope_*` attributes - Depend on source OTEL data

### Backward Compatibility

* New required fields added with defaults to maintain compatibility
* Column ordering is not guaranteed - access by name only
* Parquet schema allows missing columns (treated as all-null)

### Forward Compatibility

* New optional fields may be added without breaking existing readers
* Schema discovery should handle unknown columns gracefully
* Reserved namespace: all `chq_*` fields are system-controlled

## Performance Considerations

### Field Size Limits

* `log_message`: Recommended max 64KB, may be truncated beyond this
* Attribute values: Recommended max 32KB each
* Total row size: Target <1MB per row for optimal Parquet compression

### Indexing and Query Optimization

* `chq_timestamp`: Primary partition key, always indexed
* `chq_fingerprint`: Frequently queried, consider secondary indexing
* `log_level`: Low cardinality, good for column store compression
* `resource_service_name`: Common filter, consider indexing

### Parquet-Specific Optimizations

* String fields use dictionary encoding when cardinality allows
* Timestamp stored as INT64 with logical timestamp annotation
* Nullable fields use Parquet's null representation (not string "null")
* Column ordering: Place frequently queried fields first

### Compression Characteristics

* Log messages: High compression ratio due to repeated patterns
* Timestamps: Excellent compression in sorted data
* Attributes: Variable compression depending on cardinality

### Batch Processing Guidelines

* **Optimal batch size:** 10K-100K records per Parquet file
* **Memory usage:** ~500MB per 100K records during processing
* **Write throughput:** Target 1GB/hour sustained for large datasets
* **Schema width:** <1000 unique columns per file for best performance

### Storage Layout Recommendations

* **Partitioning:** By timestamp (hourly or daily) for time-range queries
* **File naming:** Include timestamp range for efficient pruning
* **Compression:** Use SNAPPY for balanced speed/size, GZIP for maximum compression
* **Row group size:** 128MB default, tune based on query patterns
