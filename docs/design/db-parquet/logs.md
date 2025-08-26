# Fields of the "cooked" db/ Parquet for logs

Log parquet files are a flattened version of the OpenTelemetry wire format
build for rapid searching.

## _cardinalhq prefix

Anything beginning with `_cardinalhq` is used by the CardinalHQ Lakerunner data lake
system.

### Field Specifications and Data Types

`_cardinalhq.fingerprint` (int64, nullable) represents the message body content such that
messages which are similar to one another will have the same fingerprint. Detectable items
like IP addresses, identifiers, process IDs, and other variable content are removed, and the
remaining string is fingerprinted using the fingerprinter library. Can be null if no message content
is available for fingerprinting.

`_cardinalhq.id` (string, required) is used by the CardinalHQ UI to identify this record. Generated
automatically at write time using `idgen.NextBase32ID()` to ensure uniqueness within the dataset.
Format is base32-encoded string (e.g., "abc123def456").

`_cardinalhq.level` (string, nullable) stores the log level in uppercase format. Allowed values are:
TRACE, DEBUG, INFO, WARN, ERROR, FATAL, or empty string. Derived from the OTEL `severity_text` field
if not explicitly set. Can be null if no severity information is available.

`_cardinalhq.json` (string, nullable) may contain JSON content extracted from the OTEL message body
when the body itself is a JSON object or contains embedded JSON. The field contains the raw JSON
string after searching for common message keys. Must be valid JSON when present. Can be null if
no JSON content is detected or extractable.

`_cardinalhq.message` (string, nullable) contains the human-readable log content extracted from either:
1. The OTEL `body` field directly (if it's a simple string)
2. Common message keys found within JSON content in the body
3. Processed/cleaned message text after JSON parsing
May be blank or null if no readable message content can be extracted.

`_cardinalhq.name` (string, required) is always set to the constant `"log.events"` for all log records.

`_cardinalhq.telemetry_type` (string, required) is always set to the constant `"logs"` for all log records.

`_cardinalhq.timestamp` (int64, required) represents milliseconds since the Unix epoch. Derived using
the following priority order:
1. OTEL `timestamp` field (converted from nanoseconds if > 1e15)
2. OTEL `observed_timestamp` field (converted from nanoseconds if > 1e15)  
3. Current system time at processing (if both above are missing or invalid)
Must be a positive integer representing a valid timestamp.

`_cardinalhq.value` (float64, deprecated) is no longer used and should not be present in modern files.
This field was historically set to 1.0 but has been removed from the current schema.

## Field Generation Rules

### Resource Metadata Fields

These fields are automatically added during the ingestion process:

`resource.bucket.name` (string, required) - The S3 bucket name where the original log file was stored.

`resource.file.name` (string, required) - The object path/key with a "./" prefix (e.g., "./logs/2022-01-01/app.pb").

`resource.file.type` (string, required) - Derived from the filename by removing non-alphanumeric characters and converting to lowercase. Used for file type identification and routing.

### Standard Resource Fields

Common resource attributes that may be present:

`resource.service.name` (string, nullable) - Service identifier from OTEL resource attributes.
`resource.host.name` (string, nullable) - Host/server name from OTEL resource attributes.
`resource.container.name` (string, nullable) - Container identifier from OTEL resource attributes.

## Attribute Mapping Rules

Resource, scope, and log attributes from OTEL are flattened and stored with prefixes. All attribute values are converted to their string representation, regardless of the original type.

### Attribute Prefixes and Sources

* `resource.*` - Attributes from the OTEL Resource object, containing deployment/infrastructure metadata
* `scope.*` - Attributes from the OTEL Scope/InstrumentationScope, including library name, version, and schema URL
* `log.*` - Attributes specific to individual log records from the OTEL LogRecord

### Null Handling

If a particular row has a `null` value for an attribute, this typically means:
1. That specific log record did not have that attribute present in the original OTEL data
2. Other rows in the same Parquet file do have this attribute (creating the column in the schema)
3. The attribute was explicitly set to null in the source data

### Attribute Name Normalization

- OTEL attribute keys are preserved as-is after the prefix
- Dots in attribute names are kept (e.g., `http.method` becomes `log.http.method`)  
- No case conversion is applied to attribute names
- Special characters in attribute names are preserved

### Type Coercion Rules

All attribute values undergo string conversion:
- Boolean values: `true` → `"true"`, `false` → `"false"`
- Numeric values: `123` → `"123"`, `45.67` → `"45.67"`
- Array values: `[1,2,3]` → `"[1,2,3]"` (JSON representation)
- Object values: `{"key":"val"}` → `"{\"key\":\"val\"}"` (JSON representation)
- Null values: remain as Parquet null

## JSON Message Processing

When the OTEL log body contains JSON content, special processing is applied to extract meaningful message text and preserve structured data.

### JSON Detection

The system attempts to parse the `body` field as JSON in the following cases:
1. The body is a valid JSON object (`{...}`)
2. The body is a valid JSON array (`[...]`)
3. The body contains embedded JSON within a string

### Message Extraction from JSON

When JSON is detected in the body, the system searches for common message keys in this priority order:
1. `message` - Most common message field
2. `msg` - Short form message field
3. `text` - Alternative text field
4. `description` - Descriptive text field
5. `error` - Error message content
6. `reason` - Reason or cause text

The first non-empty string value found is used as `_cardinalhq.message`.

### JSON Content Storage

When JSON processing occurs:
- `_cardinalhq.json` contains the original JSON string (if valid JSON was detected)
- `_cardinalhq.message` contains the extracted message text (if found)
- If no message keys are found, `_cardinalhq.message` may contain the entire JSON as a string

### JSON Processing Examples

**Simple JSON with message:**
```json
Input body: {"level":"error","message":"Database connection failed","timestamp":"2022-01-01T00:00:00Z"}
→ _cardinalhq.message: "Database connection failed"
→ _cardinalhq.json: "{\"level\":\"error\",\"message\":\"Database connection failed\",\"timestamp\":\"2022-01-01T00:00:00Z\"}"
```

**JSON with alternative message key:**
```json
Input body: {"severity":"warning","msg":"Rate limit exceeded","service":"api"}
→ _cardinalhq.message: "Rate limit exceeded"
→ _cardinalhq.json: "{\"severity\":\"warning\",\"msg\":\"Rate limit exceeded\",\"service\":\"api\"}"
```

**JSON without message keys:**
```json
Input body: {"user_id":12345,"action":"login","success":true}
→ _cardinalhq.message: null or empty
→ _cardinalhq.json: "{\"user_id\":12345,\"action\":\"login\",\"success\":true}"
```

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
```
_cardinalhq.fingerprint: 789123456789 (calculated from "Database connection failed")
_cardinalhq.id: "J7K2M9P8Q1R5" (generated)
_cardinalhq.level: "ERROR"
_cardinalhq.message: "Database connection failed"
_cardinalhq.name: "log.events"
_cardinalhq.telemetry_type: "logs"
_cardinalhq.timestamp: 1640995200000 (converted to milliseconds)
_cardinalhq.json: null

log.service.name: "api-gateway"
log.http.method: "POST"
log.error.code: "500"

resource.bucket.name: "my-logs-bucket" (from ingestion context)
resource.file.name: "./2022-01-01/app-logs.pb" (from ingestion context)
resource.file.type: "20220101applogspb" (derived from filename)
resource.host.name: "server01"
resource.container.id: "abc123"
resource.service.version: "1.2.3"

scope.name: "my-app-logger"
scope.version: "2.1.0"
scope.schema_url: "https://opentelemetry.io/schemas/1.9.0"
```

### Example 2: JSON Body with Message Extraction

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
```
_cardinalhq.fingerprint: 456789123456 (calculated from "Rate limit exceeded")
_cardinalhq.id: "M8N2P7Q9R3S1" (generated)
_cardinalhq.level: "WARN"
_cardinalhq.message: "Rate limit exceeded"
_cardinalhq.name: "log.events"
_cardinalhq.telemetry_type: "logs"
_cardinalhq.timestamp: 1640995260000
_cardinalhq.json: "{\"level\":\"warning\",\"message\":\"Rate limit exceeded\",\"user_id\":12345,\"endpoint\":\"/api/users\",\"rate\":\"100/min\"}"

log.trace.id: "abc123def456"
log.span.id: "789xyz"

resource.bucket.name: "my-logs-bucket"
resource.file.name: "./2022-01-01/rate-limiter.pb"
resource.file.type: "20220101ratelimiterpb"
resource.service.name: "rate-limiter"
resource.deployment.environment: "production"
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
```
_cardinalhq.fingerprint: 123456789123 (calculated from "Application started successfully")
_cardinalhq.id: "Q5R8S2T6U9V3" (generated)
_cardinalhq.level: "INFO"
_cardinalhq.message: "Application started successfully"
_cardinalhq.name: "log.events"
_cardinalhq.telemetry_type: "logs"
_cardinalhq.timestamp: 1640995320000 (current time at processing)
_cardinalhq.json: null

log.version: "1.0.0"

resource.bucket.name: "my-logs-bucket"
resource.file.name: "./2022-01-01/my-app.pb"
resource.file.type: "20220101myapppb"
resource.service.name: "my-app"
```

### Example 4: Minimal Log (Only Required Fields)

**OTEL Input:**
```protobuf
timestamp: 1640995380000000000
body: "Heartbeat"
```

**Parquet Output:**
```
_cardinalhq.fingerprint: 987654321098 (calculated from "Heartbeat")
_cardinalhq.id: "T7U1V5W9X3Y8" (generated)
_cardinalhq.level: null (no severity_text provided)
_cardinalhq.message: "Heartbeat"
_cardinalhq.name: "log.events"
_cardinalhq.telemetry_type: "logs"
_cardinalhq.timestamp: 1640995380000
_cardinalhq.json: null

resource.bucket.name: "my-logs-bucket"
resource.file.name: "./2022-01-01/minimal.pb"
resource.file.type: "20220101minimalpb"
```

## Edge Cases and Error Handling

### Timestamp Handling

**Invalid or Missing Timestamps:**
- Zero timestamp (0) → Uses observed_timestamp if available, otherwise current time
- Negative timestamp → Uses observed_timestamp if available, otherwise current time
- Timestamp in future (> current time + 1 hour) → Logs warning, but preserves value
- Nanosecond timestamps (> 1e15) → Automatically converted to milliseconds by dividing by 1e6
- Malformed timestamp types → Uses current time and logs error

**Timestamp Conversion Examples:**
```
Input: 1640995200000000000 (nanoseconds) → Output: 1640995200000 (milliseconds)
Input: 1640995200000 (already milliseconds) → Output: 1640995200000 (unchanged)
Input: 0 or missing → Output: 1640995400000 (current time at processing)
```

### Message and Body Processing

**Empty or Null Body:**
- Empty string `""` → `_cardinalhq.message: ""` (empty but present)
- Null/missing body → `_cardinalhq.message: null`
- Whitespace-only body → Preserved as-is

**Large Message Handling:**
- Messages > 64KB → May be truncated (implementation specific)
- Binary content in body → Converted to string representation or base64
- Invalid UTF-8 sequences → Replaced with Unicode replacement character (�)

**JSON Parsing Failures:**
- Invalid JSON in body → `_cardinalhq.json: null`, `_cardinalhq.message: <original body text>`
- Partially valid JSON → Attempts best-effort parsing, may extract partial content
- JSON with circular references → Parsing fails gracefully, uses original text

### Severity/Level Processing

**Invalid Severity Values:**
- Numeric severity without severity_text → `_cardinalhq.level: null`
- Custom severity values → Preserved as-is (e.g., "CRITICAL", "LOW")
- Empty string severity_text → `_cardinalhq.level: ""` (empty but present)
- Mixed case severity → Converted to uppercase (e.g., "Info" → "INFO")

### Attribute Processing

**Special Characters in Attribute Names:**
- Dots preserved: `http.method` → `log.http.method`
- Spaces converted: `user name` → `log.user name` (spaces preserved)
- Special chars preserved: `@metadata` → `log.@metadata`

**Large Attribute Values:**
- Values > 32KB → May be truncated
- Binary attribute values → Converted to base64 or hex representation
- Nested objects → Flattened to JSON string representation

**Attribute Type Edge Cases:**
- Boolean attributes: `true` → `"true"`, `false` → `"false"`
- Array attributes: `[1,2,3]` → `"[1,2,3]"`
- Object attributes: `{"a":"b"}` → `"{\"a\":\"b\"}"`
- Null attributes: Stored as Parquet null value

### Fingerprint Calculation

**Fingerprint Generation Failures:**
- Empty message → `_cardinalhq.fingerprint: null`
- Fingerprinter error → `_cardinalhq.fingerprint: null`, logs error
- Unicode normalization issues → Best-effort fingerprinting
- Very long messages → Fingerprinted on truncated content

**Fingerprint Consistency:**
- Same logical message should produce same fingerprint across runs
- Variable content (IPs, IDs, timestamps) removed before fingerprinting
- Language/encoding differences normalized

### ID Generation

**Uniqueness Guarantees:**
- IDs unique within single processing session
- Base32 format ensures URL-safe, readable identifiers
- ID generation failures → Process terminates (critical error)

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
1. **Reserved names:** Attributes that conflict with _cardinalhq fields
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
- `_cardinalhq.id` - Required for UI record identification
- `_cardinalhq.name` - Always "log.events" 
- `_cardinalhq.telemetry_type` - Always "logs"
- `_cardinalhq.timestamp` - Required for time-based queries and partitioning
- `resource.bucket.name` - Required for data lineage
- `resource.file.name` - Required for data lineage  
- `resource.file.type` - Required for processing routing

### Optional Fields (Nullable)
These fields may be null/missing depending on source data:
- `_cardinalhq.fingerprint` - Null if message cannot be fingerprinted
- `_cardinalhq.level` - Null if no severity information available
- `_cardinalhq.message` - Null if no extractable message content
- `_cardinalhq.json` - Null if body is not JSON
- All `log.*`, `resource.*`, and `scope.*` attributes - Depend on source OTEL data

### Deprecated Fields
- `_cardinalhq.value` - Removed from current schema, should not appear in new files

### Backward Compatibility
- Older Parquet files may contain `_cardinalhq.value` field - readers should ignore
- New required fields added with defaults to maintain compatibility
- Column ordering is not guaranteed - access by name only
- Parquet schema allows missing columns (treated as all-null)

### Forward Compatibility
- New optional fields may be added without breaking existing readers
- Schema discovery should handle unknown columns gracefully
- Reserved namespace: all `_cardinalhq.*` fields are system-controlled

## Performance Considerations

### Field Size Limits
- `_cardinalhq.message`: Recommended max 64KB, may be truncated beyond this
- `_cardinalhq.json`: Recommended max 32KB for optimal performance
- Attribute values: Recommended max 32KB each
- Total row size: Target <1MB per row for optimal Parquet compression

### Indexing and Query Optimization
- `_cardinalhq.timestamp`: Primary partition key, always indexed
- `_cardinalhq.fingerprint`: Frequently queried, consider secondary indexing
- `_cardinalhq.level`: Low cardinality, good for column store compression
- `resource.service.name`: Common filter, consider indexing

### Parquet-Specific Optimizations
- String fields use dictionary encoding when cardinality allows
- Timestamp stored as INT64 with logical timestamp annotation
- Nullable fields use Parquet's null representation (not string "null")
- Column ordering: Place frequently queried fields first

### Compression Characteristics
- Log messages: High compression ratio due to repeated patterns
- Timestamps: Excellent compression in sorted data
- Attributes: Variable compression depending on cardinality
- JSON fields: Moderate compression, benefits from sorted storage

### Batch Processing Guidelines
- **Optimal batch size:** 10K-100K records per Parquet file
- **Memory usage:** ~500MB per 100K records during processing
- **Write throughput:** Target 1GB/hour sustained for large datasets
- **Schema width:** <1000 unique columns per file for best performance

### Storage Layout Recommendations
- **Partitioning:** By timestamp (hourly or daily) for time-range queries
- **File naming:** Include timestamp range for efficient pruning
- **Compression:** Use SNAPPY for balanced speed/size, GZIP for maximum compression
- **Row group size:** 128MB default, tune based on query patterns
