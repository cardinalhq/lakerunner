# Fields of the "cooked" db/ Parquet for Traces

Traces parquet files contain flattened OpenTelemetry span data optimized for
distributed tracing queries and analysis.

## chq prefix

Fields beginning with `chq_` are system fields used by the CardinalHQ Lakerunner data lake system.

### Field Specifications and Data Types

`chq_customer_id` (string, nullable) is the organization ID that owns this trace data. Set
during ingestion from the storage profile. Used for data isolation and access control.

`chq_fingerprint` (int64, required) is a semantic fingerprint that groups similar spans
together. Computed using xxhash from a combination of resource and span attributes based
on the span type (see Fingerprint Calculation below).

Source: [fingerprinter/spans.go:51-91](../../../internal/oteltools/pkg/fingerprinter/spans.go#L51-L91)

`chq_id` (string, required) is a unique identifier for this span row. Generated during
ingestion using `idgen.NextBase32ID()` to ensure each span has a distinct ID.

Source: [trace_ingest_processor.go:685](../../../internal/metricsprocessing/trace_ingest_processor.go#L685)

`chq_telemetry_type` (string, required) is always set to the constant `"traces"` for all
span records. Used to distinguish telemetry types in unified queries.

`chq_timestamp` (int64, required) represents the span start time in milliseconds since
the Unix epoch. Derived from `span.StartTimestamp()` with fallback to current time if
the start timestamp is zero.

`chq_tsns` (int64, required) represents the span start time in nanoseconds. Preserves
the full precision of the OTEL timestamp for cases where sub-millisecond accuracy is needed.

Source: [ingest_proto_traces.go:242-254](../../../internal/filereader/ingest_proto_traces.go#L242-L254)

## span_ prefix

Fields beginning with `span_` contain core span data from the OpenTelemetry Span object.

### Span Field Specifications

`span_trace_id` (string, required) is the 32-character hex-encoded trace ID that groups
all spans belonging to the same trace. Extracted from `span.TraceID().String()`.

`span_id` (string, required) is the 16-character hex-encoded span ID that uniquely
identifies this span within a trace. Extracted from `span.SpanID().String()`.

`span_parent_span_id` (string, nullable) is the 16-character hex-encoded span ID of the
parent span. Empty string for root spans. Extracted from `span.ParentSpanID().String()`.

`span_name` (string, required) is the operation name of the span. This is the primary
human-readable identifier for the operation being traced. Extracted from `span.Name()`.

`span_kind` (string, required) indicates the relationship between the span and its parent.
Extracted from `span.Kind().String()`. Values:

* `"SPAN_KIND_UNSPECIFIED"` - Default, relationship unknown
* `"SPAN_KIND_INTERNAL"` - Internal operation within an application
* `"SPAN_KIND_SERVER"` - Span represents the server side of a synchronous RPC
* `"SPAN_KIND_CLIENT"` - Span represents the client side of a synchronous RPC
* `"SPAN_KIND_PRODUCER"` - Span represents the initiator of an async request
* `"SPAN_KIND_CONSUMER"` - Span represents the receiver of an async request

`span_status_code` (string, required) indicates the status of the operation. Extracted
from `span.Status().Code().String()`. Values:

* `"STATUS_CODE_UNSET"` - Default status
* `"STATUS_CODE_OK"` - Operation completed successfully
* `"STATUS_CODE_ERROR"` - Operation failed

`span_status_message` (string, nullable) contains an optional human-readable status
message, typically populated when status is ERROR. Extracted from `span.Status().Message()`.

`span_end_timestamp` (int64, required) is the span end time in milliseconds since the
Unix epoch. Derived from `span.EndTimestamp()` with fallback to current time if zero.

`span_duration` (int64, required) is the span duration in milliseconds. Calculated as
`span_end_timestamp - chq_timestamp`. Set to 0 if either timestamp required a fallback.

Source: [ingest_proto_traces.go:232-280](../../../internal/filereader/ingest_proto_traces.go#L232-L280)

## Attribute Mapping Rules

Resource, scope, and span attributes from OTEL are flattened and stored with prefixes.

### Attribute Prefixes and Sources

* `resource_*` - Attributes from the OTEL Resource object, containing deployment/infrastructure metadata
* `scope_*` - Attributes from the OTEL Scope/InstrumentationScope
* `attr_*` - Attributes from the span itself (span attributes)

### Attribute Name Normalization

OTEL attribute keys are normalized using `wkk.NormalizeName()`:

* Dots in attribute names are converted to underscores (e.g., `http.method` becomes `attr_http_method`)
* All characters are lowercased
* Non-alphanumeric characters become underscores

### Type Handling

Attribute values are converted using `otelValueToGoValue()`:

* Empty values: Stored as nil
* Bytes: Stored as raw byte array
* All other types: Converted to string using `AsString()` for safety

Type promotion during mergesort may further convert values to match the merged schema.

Source: [ingest_proto_traces.go:205-218](../../../internal/filereader/ingest_proto_traces.go#L205-L218)

### Common Resource Attributes

These resource attributes are commonly present in trace data:

* `resource_service_name` - Name of the service that generated the span
* `resource_service_version` - Version of the service
* `resource_k8s_cluster_name` - Kubernetes cluster name
* `resource_k8s_namespace_name` - Kubernetes namespace
* `resource_k8s_pod_name` - Kubernetes pod name
* `resource_k8s_deployment_name` - Kubernetes deployment name

### Common Span Attributes

These span attributes are commonly present and used in fingerprint calculation:

* `attr_http_request_method` - HTTP method (GET, POST, etc.)
* `attr_url_template` - URL pattern/template
* `attr_db_system_name` - Database system (postgresql, mysql, etc.)
* `attr_db_namespace` - Database name
* `attr_db_operation_name` - Database operation (SELECT, INSERT, etc.)
* `attr_db_collection_name` - Table or collection name
* `attr_server_address` - Server hostname or IP
* `attr_messaging_system` - Messaging system (kafka, rabbitmq, etc.)
* `attr_messaging_operation_type` - Messaging operation (publish, receive, etc.)
* `attr_messaging_destination_name` - Queue or topic name

## Fingerprint Calculation

The span fingerprint (`chq_fingerprint`) is a semantic hash that groups similar spans
together for analysis. It's computed using xxhash with pattern-specific logic.

### Base Fingerprint Components

All fingerprints include these resource-level attributes:

1. `resource_k8s_cluster_name` (or `"unknown"`)
2. `resource_k8s_namespace_name` (or `"unknown"`)
3. `resource_service_name` (or `"unknown"`)
4. `span_kind`

### Pattern-Specific Components

The fingerprint calculation checks for specific span types in order and adds additional
components based on the pattern detected:

**Messaging Pattern** (when `attr_messaging_system` is present):

* `attr_messaging_system`
* `attr_messaging_operation_type`
* `attr_messaging_destination_name`

**Database Pattern** (when `attr_db_system_name` is present):

* `span_name`
* `attr_db_system_name`
* `attr_db_namespace`
* `attr_db_operation_name`
* `attr_server_address`
* `attr_db_collection_name`

**HTTP Pattern** (when `attr_http_request_method` is present):

* `attr_http_request_method`
* `attr_url_template`

**Default Pattern** (no specific system detected):

* `span_name`

### Hash Computation

Components are joined with `"##"` separator and hashed using xxhash to produce a signed
int64 fingerprint.

Source: [fingerprinter/spans.go:95-152](../../../internal/oteltools/pkg/fingerprinter/spans.go#L95-L152)

## File Sorting and Layout

Traces parquet files are sorted by `[span_trace_id, chq_timestamp]` for optimal query
performance:

* Grouping by `span_trace_id` keeps all spans of a trace together
* Sorting by `chq_timestamp` within a trace shows the execution order

The `TraceIDTimestampSortKeyProvider` implements this sorting strategy during ingestion.

Source: [trace_ingest_processor.go:53-165](../../../internal/metricsprocessing/trace_ingest_processor.go#L53-L165)

## Complete OTEL → Parquet Examples

### Example 1: HTTP Server Span

**OTEL Input:**

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

**Parquet Output:**

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
chq_fingerprint: 1234567890123456789  (computed from HTTP pattern)

resource_service_name: "api-gateway"
resource_k8s_namespace_name: "production"
resource_k8s_cluster_name: "us-west-2"

scope_name: "opentelemetry-go"
scope_version: "1.21.0"

attr_http_request_method: "GET"
attr_url_template: "/api/users/:id"
attr_http_response_status_code: "200"
```

### Example 2: Database Client Span

**OTEL Input:**

```text
trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "8f1e2d3c4b5a6978"
parent_span_id: "6e0c63257de34c92"
name: "SELECT users"
kind: SPAN_KIND_CLIENT
status: { code: STATUS_CODE_OK }
start_time: 1640995200010000000
end_time: 1640995200025000000
attributes: [
  {key: "db.system.name", value: "postgresql"},
  {key: "db.namespace", value: "mydb"},
  {key: "db.operation.name", value: "SELECT"},
  {key: "db.collection.name", value: "users"},
  {key: "server.address", value: "db.example.com"}
]
resource: {
  attributes: [
    {key: "service.name", value: "api-gateway"},
    {key: "k8s.namespace.name", value: "production"}
  ]
}
```

**Parquet Output:**

```text
span_trace_id: "d4cda95b652f4a1592b449d5929fda1b"
span_id: "8f1e2d3c4b5a6978"
span_parent_span_id: "6e0c63257de34c92"
span_name: "SELECT users"
span_kind: "SPAN_KIND_CLIENT"
span_status_code: "STATUS_CODE_OK"
span_status_message: ""
span_end_timestamp: 1640995200025
span_duration: 15

chq_timestamp: 1640995200010
chq_tsns: 1640995200010000000
chq_telemetry_type: "traces"
chq_id: "b2c3d4e5f6g7h8i9"
chq_fingerprint: -987654321098765432  (computed from DB pattern)

resource_service_name: "api-gateway"
resource_k8s_namespace_name: "production"
resource_k8s_cluster_name: null

attr_db_system_name: "postgresql"
attr_db_namespace: "mydb"
attr_db_operation_name: "SELECT"
attr_db_collection_name: "users"
attr_server_address: "db.example.com"
```

### Example 3: Messaging Producer Span

**OTEL Input:**

```text
trace_id: "a1b2c3d4e5f67890a1b2c3d4e5f67890"
span_id: "1234567890abcdef"
parent_span_id: ""
name: "orders send"
kind: SPAN_KIND_PRODUCER
status: { code: STATUS_CODE_OK }
start_time: 1640995200100000000
end_time: 1640995200105000000
attributes: [
  {key: "messaging.system", value: "kafka"},
  {key: "messaging.operation.type", value: "publish"},
  {key: "messaging.destination.name", value: "orders"}
]
resource: {
  attributes: [
    {key: "service.name", value: "order-service"}
  ]
}
```

**Parquet Output:**

```text
span_trace_id: "a1b2c3d4e5f67890a1b2c3d4e5f67890"
span_id: "1234567890abcdef"
span_parent_span_id: ""
span_name: "orders send"
span_kind: "SPAN_KIND_PRODUCER"
span_status_code: "STATUS_CODE_OK"
span_status_message: ""
span_end_timestamp: 1640995200105
span_duration: 5

chq_timestamp: 1640995200100
chq_tsns: 1640995200100000000
chq_telemetry_type: "traces"
chq_id: "c3d4e5f6g7h8i9j0"
chq_fingerprint: 5678901234567890123  (computed from Messaging pattern)

resource_service_name: "order-service"
resource_k8s_namespace_name: null
resource_k8s_cluster_name: null

attr_messaging_system: "kafka"
attr_messaging_operation_type: "publish"
attr_messaging_destination_name: "orders"
```

## Edge Cases and Error Handling

### Timestamp Handling

* Zero start timestamp: Falls back to current system time
* Zero end timestamp: Falls back to current system time
* Duration calculation: Set to 0 if either timestamp required a fallback
* A metric counter tracks timestamp fallback occurrences for monitoring

Source: [ingest_proto_traces.go:242-280](../../../internal/filereader/ingest_proto_traces.go#L242-L280)

### Missing Fingerprint Components

* Missing `resource_k8s_cluster_name`: Uses `"unknown"`
* Missing `resource_k8s_namespace_name`: Uses `"unknown"`
* Missing `resource_service_name`: Uses `"unknown"`
* Missing span attributes for patterns: Uses empty string in hash calculation

### Dropped Spans

Spans are not explicitly dropped during ingestion, but rows missing `chq_timestamp`
are skipped during dateint binning with a warning logged.

## Null Handling

If a particular row has a `null` value for an attribute, this typically means:

1. That specific span did not have that attribute present in the original OTEL data
2. Other rows in the same Parquet file do have this attribute (creating the column in the schema)

## Schema Evolution and Compatibility

### Required Fields (Never Remove)

These fields must always be present in traces Parquet files:

* `span_trace_id` - Required for trace identification
* `span_id` - Required for span identification
* `span_name` - Required for operation identification
* `span_kind` - Required for span relationship
* `span_status_code` - Required for operation status
* `span_duration` - Required for latency queries
* `chq_timestamp` - Required for time-based queries
* `chq_tsns` - Required for full timestamp precision
* `chq_telemetry_type` - Always "traces"
* `chq_fingerprint` - Required for span grouping
* `chq_id` - Required for unique identification

### Optional Fields (Nullable)

These fields may be null/missing depending on source data:

* `span_parent_span_id` - Empty string for root spans
* `span_status_message` - Only populated on errors typically
* `chq_customer_id` - Depends on ingestion context
* All `resource_*` fields - Depend on source OTEL data
* All `scope_*` fields - Depend on source OTEL data
* All `attr_*` fields - Depend on source OTEL data
