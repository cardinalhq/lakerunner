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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestIngestProtoTracesReader_SchemaExtraction_BasicTypes tests that the schema extractor
// correctly identifies different attribute types across all spans.
func TestIngestProtoTracesReader_SchemaExtraction_BasicTypes(t *testing.T) {
	builder := signalbuilder.NewTracesBuilder()

	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name: "test-tracer",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						Name:           "test-span",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"string.attr": "hello",
							"int.attr":    int64(42),
							"float.attr":  3.14,
							"bool.attr":   true,
						},
						Status: signalbuilder.SpanStatus{
							Code: 1,
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceTraces)
	require.NoError(t, err)

	traces := builder.Build()
	marshaler := &ptrace.ProtoMarshaler{}
	data, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoTracesReader(reader, 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Verify core span fields
	assert.Equal(t, DataTypeString, schema.GetColumnType("span_trace_id"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("span_id"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("span_name"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("chq_timestamp"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("chq_fingerprint"))

	// Verify attribute types are preserved
	assert.Equal(t, DataTypeString, schema.GetColumnType("resource_service_name"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_string_attr"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("attr_int_attr"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("attr_float_attr"))
	// Booleans are converted to strings to avoid OTEL data type mismatch panics
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_bool_attr"))
}

// TestIngestProtoTracesReader_SchemaExtraction_TypePromotion tests type promotion
// when the same attribute has different types across spans.
func TestIngestProtoTracesReader_SchemaExtraction_TypePromotion(t *testing.T) {
	builder := signalbuilder.NewTracesBuilder()

	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name: "test-tracer",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						Name:           "span1",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"mixed.attr": int64(42), // int64 first
						},
					},
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "2234567890123456",
						Name:           "span2",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"mixed.attr": "string value", // string later - should promote to string
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceTraces)
	require.NoError(t, err)

	traces := builder.Build()
	marshaler := &ptrace.ProtoMarshaler{}
	data, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoTracesReader(reader, 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Type should be promoted to string (int64 + string → string)
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_mixed_attr"))
}

// TestIngestProtoTracesReader_SchemaExtraction_MultiResource tests schema extraction
// across multiple resources.
func TestIngestProtoTracesReader_SchemaExtraction_MultiResource(t *testing.T) {
	builder := signalbuilder.NewTracesBuilder()

	// First resource with specific attributes
	resourceTraces1 := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "service-1",
			"env":          "production",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						Name:           "span1",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"http.method": "GET",
						},
					},
				},
			},
		},
	}

	// Second resource with different attributes
	resourceTraces2 := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "service-2",
			"region":       "us-west-2",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "2234567890123456",
						Name:           "span2",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"db.system": "postgresql",
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceTraces1)
	require.NoError(t, err)
	err = builder.Add(resourceTraces2)
	require.NoError(t, err)

	traces := builder.Build()
	marshaler := &ptrace.ProtoMarshaler{}
	data, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoTracesReader(reader, 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Schema should include attributes from both resources
	assert.True(t, schema.HasColumn("resource_service_name"))
	assert.True(t, schema.HasColumn("resource_env"))
	assert.True(t, schema.HasColumn("resource_region"))
	assert.True(t, schema.HasColumn("attr_http_method"))
	assert.True(t, schema.HasColumn("attr_db_system"))
}
