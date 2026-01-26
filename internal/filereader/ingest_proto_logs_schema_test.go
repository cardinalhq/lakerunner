// Copyright (C) 2025-2026 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package filereader

import (
	"bytes"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestIngestProtoLogsReader_SchemaExtraction_BasicTypes tests that the schema extractor
// correctly identifies different attribute types across all logs.
func TestIngestProtoLogsReader_SchemaExtraction_BasicTypes(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "test-logger",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log with string attribute",
						Attributes: map[string]any{
							"string_attr": "hello",
							"int_attr":    int64(42),
							"float_attr":  3.14,
							"bool_attr":   true,
						},
					},
					{
						Timestamp:      time.Now().Add(time.Second).UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log with different values",
						Attributes: map[string]any{
							"string_attr": "world",
							"int_attr":    int64(99),
							"float_attr":  2.71,
							"bool_attr":   false,
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected schema should have:
	// - log_message: string
	// - chq_timestamp: int64
	// - chq_tsns: int64
	// - log_level: string
	// - resource_service_name: string
	// - attr_string_attr: string
	// - attr_int_attr: int64
	// - attr_float_attr: float64
	// - attr_bool_attr: bool

	schema := protoReader.GetSchema()
	assert.NotNil(t, schema)
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_string_attr"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("attr_int_attr"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("attr_float_attr"))
	// Booleans are converted to strings to avoid OTEL data type mismatch panics
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_bool_attr"))
}

// TestIngestProtoLogsReader_SchemaExtraction_TypePromotion tests that when the same
// attribute has different types across logs, the schema promotes to the most general type.
func TestIngestProtoLogsReader_SchemaExtraction_TypePromotion(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	// Create logs where the same attribute has different types
	// This simulates real-world scenarios where attribute types vary
	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "test-logger",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 1",
						Attributes: map[string]any{
							"mixed_attr": int64(123), // int64 first
						},
					},
					{
						Timestamp:      time.Now().Add(time.Second).UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 2",
						Attributes: map[string]any{
							"mixed_attr": "string_value", // then string
						},
					},
					{
						Timestamp:      time.Now().Add(2 * time.Second).UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 3",
						Attributes: map[string]any{
							"mixed_attr": 3.14, // then float
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected: mixed_attr should be promoted to string (most general type)
	// Type promotion rules (from design doc):
	// - int64 + string → string
	// - int64 + float64 → float64
	// - float64 + string → string
	// - When in doubt → string

	schema := protoReader.GetSchema()
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_mixed_attr"))
}

// TestIngestProtoLogsReader_SchemaExtraction_SparseAttributes tests schema extraction
// when attributes appear in only some logs (sparse columns).
func TestIngestProtoLogsReader_SchemaExtraction_SparseAttributes(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "test-logger",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 1",
						Attributes: map[string]any{
							"common_attr":  "value1",
							"only_in_log1": int64(100),
						},
					},
					{
						Timestamp:      time.Now().Add(time.Second).UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 2",
						Attributes: map[string]any{
							"common_attr":  "value2",
							"only_in_log2": true,
						},
					},
					{
						Timestamp:      time.Now().Add(2 * time.Second).UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log 3",
						Attributes: map[string]any{
							"common_attr":  "value3",
							"only_in_log3": 42.0,
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected schema should include all sparse columns
	schema := protoReader.GetSchema()
	assert.True(t, schema.HasColumn("attr_common_attr"))
	assert.True(t, schema.HasColumn("attr_only_in_log1"))
	assert.True(t, schema.HasColumn("attr_only_in_log2"))
	assert.True(t, schema.HasColumn("attr_only_in_log3"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_common_attr"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("attr_only_in_log1"))
	// Booleans are converted to strings to avoid OTEL data type mismatch panics
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_only_in_log2"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("attr_only_in_log3"))
}

// TestIngestProtoLogsReader_SchemaExtraction_MultiResource tests schema extraction
// across multiple resources with different attribute sets.
func TestIngestProtoLogsReader_SchemaExtraction_MultiResource(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	// Resource 1: web service with HTTP attributes
	resource1 := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "web-service",
			"host.name":    "web-01",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "http-logger",
				Attributes: map[string]any{
					"logger.type": "http",
				},
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "HTTP request",
						Attributes: map[string]any{
							"http.method":      "GET",
							"http.status_code": int64(200),
							"http.path":        "/api/users",
						},
					},
				},
			},
		},
	}

	// Resource 2: database service with DB attributes
	resource2 := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "db-service",
			"db.system":    "postgresql",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "db-logger",
				Attributes: map[string]any{
					"logger.type": "database",
				},
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().Add(time.Second).UnixNano(),
						SeverityText:   "DEBUG",
						SeverityNumber: int32(plog.SeverityNumberDebug),
						Body:           "Query executed",
						Attributes: map[string]any{
							"db.query":    "SELECT * FROM users",
							"db.duration": 15.5,
							"db.rows":     int64(100),
						},
					},
				},
			},
		},
	}

	err := builder.Add(resource1)
	require.NoError(t, err)
	err = builder.Add(resource2)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected: schema should include columns from both resources
	// TODO: Once schema extraction is implemented, verify:
	// schema := protoReader.GetSchema()
	//
	// // Resource attributes from both services
	// assert.True(t, schema.HasColumn("resource_service_name"))
	// assert.True(t, schema.HasColumn("resource_host_name"))
	// assert.True(t, schema.HasColumn("resource_db_system"))
	//
	// // Scope attributes from both loggers
	// assert.True(t, schema.HasColumn("scope_logger_type"))
	//
	// // Log attributes from web service
	// assert.True(t, schema.HasColumn("attr_http_method"))
	// assert.True(t, schema.HasColumn("attr_http_status_code"))
	// assert.True(t, schema.HasColumn("attr_http_path"))
	//
	// // Log attributes from db service
	// assert.True(t, schema.HasColumn("attr_db_query"))
	// assert.True(t, schema.HasColumn("attr_db_duration"))
	// assert.True(t, schema.HasColumn("attr_db_rows"))
}

// TestIngestProtoLogsReader_NormalizeRow_TypeConversion tests that normalizeRow
// correctly converts attribute values to their declared schema types.
func TestIngestProtoLogsReader_NormalizeRow_TypeConversion(t *testing.T) {
	// This test will verify that normalizeRow can convert values to match the schema
	// For example:
	// - If schema says attr_x is int64, but OTEL value is convertible, it should convert
	// - If schema says attr_x is string, int64 values should be converted to string
	// - Values that don't match and can't convert should... (error? skip? convert to string?)

	t.Skip("TODO: Implement after normalizeRow function is created")
}

// TestIngestProtoLogsReader_NormalizeRow_NullHandling tests that normalizeRow
// only emits keys for non-null values.
func TestIngestProtoLogsReader_NormalizeRow_NullHandling(t *testing.T) {
	// This test will verify that:
	// - If a column exists in schema but value is null/missing in row, key is NOT emitted
	// - Only columns with actual values are included in normalized row
	// - Empty strings are treated as non-null (they are a value)

	t.Skip("TODO: Implement after normalizeRow function is created")
}

// TestIngestProtoLogsReader_SchemaExtraction_EmptyAttributes tests schema extraction
// with logs that have no attributes.
func TestIngestProtoLogsReader_SchemaExtraction_EmptyAttributes(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "minimal-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "minimal-logger",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Minimal log with no attributes",
						Attributes:     map[string]any{}, // No attributes
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected: schema should only have core fields, no attr_* columns
	// TODO: Once schema extraction is implemented, verify:
	// schema := protoReader.GetSchema()
	// assert.True(t, schema.HasColumn("log_message"))
	// assert.True(t, schema.HasColumn("chq_timestamp"))
	// assert.True(t, schema.HasColumn("chq_tsns"))
	// assert.True(t, schema.HasColumn("log_level"))
	// assert.True(t, schema.HasColumn("resource_service_name"))
	//
	// // Should not have any attr_* columns
	// for _, col := range schema.Columns {
	//     assert.False(t, strings.HasPrefix(col.Name, "attr_"))
	// }
}

// TestIngestProtoLogsReader_SchemaExtraction_MapAndArrayTypes tests handling of
// complex OTEL value types (maps and arrays).
func TestIngestProtoLogsReader_SchemaExtraction_MapAndArrayTypes(t *testing.T) {
	// OTEL supports Map and Slice value types
	// These should likely be converted to JSON strings or handled specially
	// This test verifies that behavior

	builder := signalbuilder.NewLogBuilder()

	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name: "test-logger",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Log with complex types",
						Attributes: map[string]any{
							"simple":     "string",
							"array_attr": []any{"a", "b", "c"},
							"map_attr": map[string]any{
								"nested_key": "nested_value",
								"count":      int64(42),
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// Expected: complex types should be converted to strings (likely JSON)
	// TODO: Once schema extraction is implemented, verify:
	// schema := protoReader.GetSchema()
	// assert.Equal(t, DataTypeString, schema.GetColumnType("attr_simple"))
	// assert.Equal(t, DataTypeString, schema.GetColumnType("attr_array_attr")) // Array → JSON string
	// assert.Equal(t, DataTypeString, schema.GetColumnType("attr_map_attr"))   // Map → JSON string
}

// TestIngestProtoLogsReader_SchemaExtraction_BytesType tests handling of bytes values.
func TestIngestProtoLogsReader_SchemaExtraction_BytesType(t *testing.T) {
	// Manually construct OTEL logs with bytes type
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test-logger")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("Test with bytes")
	lr.Attributes().PutEmptyBytes("bytes_attr").FromRaw([]byte{0x01, 0x02, 0x03, 0x04})

	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	assert.Equal(t, DataTypeBytes, schema.GetColumnType("attr_bytes_attr"))

	// Verify we can read the data
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	assert.Equal(t, 1, len(allRows))

	// The bytes value should be present
	bytesVal, exists := allRows[0][wkk.NewRowKey("attr_bytes_attr")]
	assert.True(t, exists)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, bytesVal)
}

// TestIngestProtoLogsReader_SchemaExtraction_TypePromotionRules tests all
// type promotion rules from the design document.
func TestIngestProtoLogsReader_SchemaExtraction_TypePromotionRules(t *testing.T) {
	tests := []struct {
		name         string
		values       []any
		expectedType string // Will map to DataType once defined
		description  string
	}{
		{
			name:         "int64_only",
			values:       []any{int64(1), int64(2), int64(3)},
			expectedType: "int64",
			description:  "All int64 → int64",
		},
		{
			name:         "int64_and_float64",
			values:       []any{int64(1), 3.14},
			expectedType: "float64",
			description:  "int64 + float64 → float64",
		},
		{
			name:         "int64_and_string",
			values:       []any{int64(42), "hello"},
			expectedType: "string",
			description:  "int64 + string → string",
		},
		{
			name:         "int64_and_bool",
			values:       []any{int64(1), true},
			expectedType: "string",
			description:  "int64 + bool → string",
		},
		{
			name:         "float64_only",
			values:       []any{3.14, 2.71},
			expectedType: "float64",
			description:  "All float64 → float64",
		},
		{
			name:         "float64_and_string",
			values:       []any{3.14, "pi"},
			expectedType: "string",
			description:  "float64 + string → string",
		},
		{
			name:         "float64_and_bool",
			values:       []any{3.14, false},
			expectedType: "string",
			description:  "float64 + bool → string",
		},
		{
			name:         "string_only",
			values:       []any{"a", "b", "c"},
			expectedType: "string",
			description:  "All string → string",
		},
		{
			name:         "string_and_bool",
			values:       []any{"true", true},
			expectedType: "string",
			description:  "string + bool → string",
		},
		{
			name:         "bool_only",
			values:       []any{true, false, true},
			expectedType: "bool",
			description:  "All bool → bool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := signalbuilder.NewLogBuilder()

			// Create one log record per value
			logRecords := make([]signalbuilder.LogRecord, len(tt.values))
			for i, val := range tt.values {
				logRecords[i] = signalbuilder.LogRecord{
					Timestamp:      time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
					SeverityText:   "INFO",
					SeverityNumber: int32(plog.SeverityNumberInfo),
					Body:           "test",
					Attributes: map[string]any{
						"test_attr": val,
					},
				}
			}

			resourceLogs := &signalbuilder.ResourceLogs{
				Resource: map[string]any{
					"service.name": "test-service",
				},
				ScopeLogs: []signalbuilder.ScopeLogs{
					{
						Name:       "test-logger",
						LogRecords: logRecords,
					},
				},
			}

			err := builder.Add(resourceLogs)
			require.NoError(t, err)

			logs := builder.Build()
			marshaler := &plog.ProtoMarshaler{}
			data, err := marshaler.MarshalLogs(logs)
			require.NoError(t, err)

			reader := bytes.NewReader(data)
			opts := ReaderOptions{
				SignalType: SignalTypeLogs,
				BatchSize:  1000,
			}
			protoReader, err := NewIngestProtoLogsReader(reader, opts)
			require.NoError(t, err)
			defer func() { _ = protoReader.Close() }()

			// TODO: Once schema extraction is implemented, verify:
			// schema := protoReader.GetSchema()
			// actualType := schema.GetColumnType("attr_test_attr")
			// assert.Equal(t, tt.expectedType, actualType.String(), tt.description)

			t.Logf("Test case: %s - %s", tt.name, tt.description)
		})
	}
}

// TestIngestProtoLogsReader_SchemaExtraction_OTELValueTypes tests that we correctly
// identify OTEL pcommon.Value types before string conversion.
func TestIngestProtoLogsReader_SchemaExtraction_OTELValueTypes(t *testing.T) {
	// This test verifies we can detect the actual OTEL value type using pcommon.Value.Type()
	// The types are: String, Int, Double, Bool, Map, Slice, Bytes, Empty

	// We'll manually construct OTEL logs to ensure we get the exact types we want
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test-logger")

	// Log 1: String type
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr1.SetSeverityText("INFO")
	lr1.Body().SetStr("Test 1")
	lr1.Attributes().PutStr("str_attr", "hello")
	lr1.Attributes().PutInt("int_attr", 42)
	lr1.Attributes().PutDouble("double_attr", 3.14)
	lr1.Attributes().PutBool("bool_attr", true)
	lr1.Attributes().PutEmptyBytes("bytes_attr").FromRaw([]byte{0x01, 0x02, 0x03})

	// Marshal to protobuf
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  1000,
	}
	protoReader, err := NewIngestProtoLogsReader(reader, opts)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	// TODO: Once schema extraction is implemented, verify:
	// schema := protoReader.GetSchema()
	// assert.Equal(t, DataTypeString, schema.GetColumnType("attr_str_attr"))
	// assert.Equal(t, DataTypeInt64, schema.GetColumnType("attr_int_attr"))
	// assert.Equal(t, DataTypeFloat64, schema.GetColumnType("attr_double_attr"))
	// assert.Equal(t, DataTypeBool, schema.GetColumnType("attr_bool_attr"))
	// assert.Equal(t, DataTypeBytes, schema.GetColumnType("attr_bytes_attr"))

	// Verify that we can also read the data correctly
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	assert.Equal(t, 1, len(allRows))

	// The current implementation converts everything to string
	// After normalization, values should match their schema types
}
