// Copyright (C) 2025 CardinalHQ, Inc
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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestNewProtoTracesReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoTracesReader(reader, 1000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL traces")
}

func TestNewProtoTracesReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoTracesReader(emptyReader, 1000)
	// Empty data may create a valid but empty traces object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL traces")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty traces should return EOF immediately
		batch, readErr := reader.Next()
		assert.Nil(t, batch)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func createSimpleSyntheticTraces() []byte {
	builder := signalbuilder.NewTracesBuilder()
	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "test-tracer",
				Version: "1.0.0",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						Name:           "test-operation",
						Kind:           1, // Internal
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
					},
				},
			},
		},
	}
	err := builder.Add(resourceTraces)
	if err != nil {
		panic(err)
	}

	tracesData := builder.Build()
	marshaler := &ptrace.ProtoMarshaler{}
	protoBytes, err := marshaler.MarshalTraces(tracesData)
	if err != nil {
		panic(err)
	}
	return protoBytes
}

func TestProtoTracesReader_EmptySlice(t *testing.T) {
	// Create synthetic test data
	syntheticData := createSimpleSyntheticTraces()

	protoReader, err := NewProtoTracesReader(bytes.NewReader(syntheticData), 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read with empty slice behavior is no longer applicable with Next() method
	// Next() returns a batch or nil, not dependent on slice size
	batch, err := protoReader.Next()
	if batch != nil {
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, batch.Len(), 0)
	}
}

func TestProtoTracesReader_Close(t *testing.T) {
	// Create synthetic test data
	syntheticData := createSimpleSyntheticTraces()

	protoReader, err := NewProtoTracesReader(bytes.NewReader(syntheticData), 1000)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := protoReader.Next()
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch before closing")
	require.Equal(t, 1, batch.Len(), "Should read exactly 1 row before closing")

	// Close should work
	err = protoReader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = protoReader.Next()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = protoReader.Close()
	assert.NoError(t, err)
}

// Test parsing function directly with invalid data
func TestParseProtoToOtelTraces_InvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("invalid protobuf data"))
	_, err := parseProtoToOtelTraces(invalidData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal protobuf traces")
}

// Test parsing function with read error
func TestParseProtoToOtelTraces_ReadError(t *testing.T) {
	errorReader := &errorReaderImpl{shouldError: true}
	_, err := parseProtoToOtelTraces(errorReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read data")
}

func TestProtoTracesReader_SyntheticData(t *testing.T) {
	// Create synthetic trace data with multiple span types
	builder := signalbuilder.NewTracesBuilder()

	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name":    "test-service",
			"service.version": "1.0.0",
			"deployment.env":  "test",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "test-tracer",
				Version: "1.2.3",
				Attributes: map[string]any{
					"scope.type": "instrumentation",
				},
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						ParentSpanID:   "",
						Name:           "root-operation",
						Kind:           3, // Server
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"http.method":      "GET",
							"http.url":         "/api/test",
							"http.status_code": int64(200),
						},
						Status: signalbuilder.SpanStatus{
							Code:    1, // Ok
							Message: "Request completed successfully",
						},
					},
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "2345678901234567",
						ParentSpanID:   "1234567890123456",
						Name:           "database-query",
						Kind:           4, // Client
						StartTimestamp: time.Now().Add(10 * time.Millisecond).UnixNano(),
						EndTimestamp:   time.Now().Add(50 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"db.system":    "postgresql",
							"db.name":      "testdb",
							"db.operation": "SELECT",
							"db.table":     "users",
						},
						Status: signalbuilder.SpanStatus{
							Code: 0, // Unset
						},
					},
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "3456789012345678",
						ParentSpanID:   "1234567890123456",
						Name:           "internal-processing",
						Kind:           1, // Internal
						StartTimestamp: time.Now().Add(60 * time.Millisecond).UnixNano(),
						EndTimestamp:   time.Now().Add(90 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"component":    "data-processor",
							"record.count": int64(42),
						},
						Status: signalbuilder.SpanStatus{
							Code: 0, // Unset
						},
					},
				},
			},
		},
	}

	// Add resource traces to builder
	err := builder.Add(resourceTraces)
	require.NoError(t, err)

	// Build the traces
	tracesData := builder.Build()

	// Create protobuf bytes
	marshaler := &ptrace.ProtoMarshaler{}
	protoBytes, err := marshaler.MarshalTraces(tracesData)
	require.NoError(t, err)

	// Create reader
	protoReader, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read all rows
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read exactly 3 spans")

	// Verify common fields for all spans
	for i, row := range allRows {
		t.Run(fmt.Sprintf("span_%d", i), func(t *testing.T) {
			// Basic span fields
			assert.Contains(t, row, "trace_id")
			assert.Contains(t, row, "span_id")
			assert.Contains(t, row, "name")
			assert.Contains(t, row, "kind")
			assert.Contains(t, row, "start_timestamp")
			assert.Contains(t, row, "end_timestamp")
			assert.Contains(t, row, "status_code")

			// Resource attributes with prefix
			assert.Contains(t, row, "resource.service.name")
			assert.Equal(t, "test-service", row["resource.service.name"])
			assert.Contains(t, row, "resource.service.version")
			assert.Equal(t, "1.0.0", row["resource.service.version"])
			assert.Contains(t, row, "resource.deployment.env")
			assert.Equal(t, "test", row["resource.deployment.env"])

			// Scope attributes with prefix
			assert.Contains(t, row, "scope.scope.type")
			assert.Equal(t, "instrumentation", row["scope.scope.type"])

			// Common trace ID
			assert.Equal(t, "12345678901234567890123456789012", row["trace_id"])
		})
	}

	// Verify specific spans
	rootSpan := allRows[0]
	assert.Equal(t, "1234567890123456", rootSpan["span_id"])
	assert.Equal(t, "root-operation", rootSpan["name"])
	assert.Equal(t, "Client", rootSpan["kind"])
	assert.Equal(t, "Ok", rootSpan["status_code"])
	assert.Contains(t, rootSpan, "span.http.method")
	assert.Equal(t, "GET", rootSpan["span.http.method"])
	assert.Contains(t, rootSpan, "span.http.status_code")
	assert.Equal(t, "200", rootSpan["span.http.status_code"])

	dbSpan := allRows[1]
	assert.Equal(t, "2345678901234567", dbSpan["span_id"])
	assert.Equal(t, "database-query", dbSpan["name"])
	assert.Equal(t, "Producer", dbSpan["kind"])
	assert.Equal(t, "Unset", dbSpan["status_code"])
	assert.Contains(t, dbSpan, "span.db.system")
	assert.Equal(t, "postgresql", dbSpan["span.db.system"])
	assert.Contains(t, dbSpan, "span.db.operation")
	assert.Equal(t, "SELECT", dbSpan["span.db.operation"])

	internalSpan := allRows[2]
	assert.Equal(t, "3456789012345678", internalSpan["span_id"])
	assert.Equal(t, "internal-processing", internalSpan["name"])
	assert.Equal(t, "Internal", internalSpan["kind"])
	assert.Equal(t, "Unset", internalSpan["status_code"])
	assert.Contains(t, internalSpan, "span.component")
	assert.Equal(t, "data-processor", internalSpan["span.component"])
	assert.Contains(t, internalSpan, "span.record.count")
	assert.Equal(t, "42", internalSpan["span.record.count"])

	// Test batched reading with a new reader instance
	protoReader2, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1000)
	require.NoError(t, err)
	defer protoReader2.Close()

	// Read in batches
	var totalBatchedRows int
	for {
		batch, readErr := protoReader2.Next()
		if batch != nil {
			totalBatchedRows += batch.Len()

			// Verify each row that was read
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				assert.Greater(t, len(row), 0, "Batched row %d should have data", i)
				assert.Contains(t, row, "trace_id")
				assert.Contains(t, row, "span_id")
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
	}
	assert.Equal(t, len(allRows), totalBatchedRows, "Batched reading should read same number of rows")

	// Test single row reading
	protoReader3, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1)
	require.NoError(t, err)
	defer protoReader3.Close()

	batch, err := protoReader3.Next()
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch")
	assert.Equal(t, 1, batch.Len(), "Should read exactly 1 row")
	assert.Contains(t, batch.Get(0), "trace_id")
	assert.Contains(t, batch.Get(0), "resource.service.name")

	// Test data exhaustion - continue reading until EOF
	var exhaustRows int
	for {
		batch, readErr := protoReader3.Next()
		if batch != nil {
			exhaustRows += batch.Len()
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
	}
	// Should have read all remaining rows
	assert.Equal(t, len(allRows)-1, exhaustRows, "Should read all remaining rows after first single read")
}

func TestProtoTracesReader_SyntheticMultiResourceTraces(t *testing.T) {
	builder := signalbuilder.NewTracesBuilder()

	// Create traces from multiple services
	resourceTraces1 := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name":    "frontend-service",
			"service.version": "2.1.0",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "frontend-tracer",
				Version: "1.0.0",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "aaaabbbbccccddddeeeeffff00001111",
						SpanID:         "aaaa111122223333",
						Name:           "frontend-request",
						Kind:           3, // Server
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(200 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"http.method": "POST",
							"user.id":     int64(123),
						},
						Status: signalbuilder.SpanStatus{
							Code: 1, // Ok
						},
					},
				},
			},
		},
	}

	resourceTraces2 := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name":    "backend-service",
			"service.version": "1.5.2",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "backend-tracer",
				Version: "2.0.0",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "aaaabbbbccccddddeeeeffff00001111",
						SpanID:         "bbbb444455556666",
						ParentSpanID:   "aaaa111122223333",
						Name:           "backend-processing",
						Kind:           1, // Internal
						StartTimestamp: time.Now().Add(50 * time.Millisecond).UnixNano(),
						EndTimestamp:   time.Now().Add(150 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"operation.type": "data-processing",
							"batch.size":     int64(500),
						},
						Status: signalbuilder.SpanStatus{
							Code: 1, // Ok
						},
					},
					{
						TraceID:        "aaaabbbbccccddddeeeeffff00001111",
						SpanID:         "cccc777788889999",
						ParentSpanID:   "bbbb444455556666",
						Name:           "cache-lookup",
						Kind:           4, // Client
						StartTimestamp: time.Now().Add(60 * time.Millisecond).UnixNano(),
						EndTimestamp:   time.Now().Add(80 * time.Millisecond).UnixNano(),
						Attributes: map[string]any{
							"cache.system": "redis",
							"cache.hit":    "true",
						},
						Status: signalbuilder.SpanStatus{
							Code: 0, // Unset
						},
					},
				},
			},
		},
	}

	// Add multiple resource traces to builder
	err := builder.Add(resourceTraces1)
	require.NoError(t, err)
	err = builder.Add(resourceTraces2)
	require.NoError(t, err)

	// Build traces with multiple resources
	tracesData := builder.Build()

	marshaler := &ptrace.ProtoMarshaler{}
	protoBytes, err := marshaler.MarshalTraces(tracesData)
	require.NoError(t, err)

	protoReader, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read 3 spans from 2 resources")

	// Check resource distribution
	frontendSpans := 0
	backendSpans := 0

	for _, row := range allRows {
		if serviceName, exists := row["resource.service.name"]; exists {
			switch serviceName {
			case "frontend-service":
				frontendSpans++
				assert.Contains(t, row, "resource.service.version")
				assert.Equal(t, "2.1.0", row["resource.service.version"])
			case "backend-service":
				backendSpans++
				assert.Contains(t, row, "resource.service.version")
				assert.Equal(t, "1.5.2", row["resource.service.version"])
			}
		}
		// All spans should be part of the same trace
		assert.Equal(t, "aaaabbbbccccddddeeeeffff00001111", row["trace_id"])
	}

	assert.Equal(t, 1, frontendSpans, "Should have 1 span from frontend service")
	assert.Equal(t, 2, backendSpans, "Should have 2 spans from backend service")
}

func TestProtoTracesReader_SyntheticEdgeCases(t *testing.T) {
	builder := signalbuilder.NewTracesBuilder()

	// Test traces with edge cases
	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "edge-case-service",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "edge-tracer",
				Version: "0.0.1",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "00000000000000000000000000000001",
						SpanID:         "0000000000000001",
						Name:           "", // Empty name
						Kind:           0,  // Unspecified
						StartTimestamp: 0,  // Zero timestamp
						EndTimestamp:   1000000,
						Attributes:     map[string]any{}, // No attributes
						Status: signalbuilder.SpanStatus{
							Code: 0, // Unset
						},
					},
					{
						TraceID:        "ffffffffffffffffffffffffffffff01",
						SpanID:         "ffffffffffffffff",
						Name:           "span-with-zero-values",
						Kind:           1, // Internal
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().UnixNano(), // Same start/end time
						Attributes: map[string]any{
							"zero.int":     int64(0),
							"empty.string": "",
							"false.bool":   "false",
						},
						Status: signalbuilder.SpanStatus{
							Code:    2, // Error
							Message: "Something went wrong",
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceTraces)
	require.NoError(t, err)

	tracesData := builder.Build()

	marshaler := &ptrace.ProtoMarshaler{}
	protoBytes, err := marshaler.MarshalTraces(tracesData)
	require.NoError(t, err)

	protoReader, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 2, len(allRows), "Should read 2 spans with edge cases")

	// First span - empty name, zero timestamp
	emptySpan := allRows[0]
	assert.Equal(t, "", emptySpan["name"])
	assert.Equal(t, "Unspecified", emptySpan["kind"])
	assert.Contains(t, emptySpan, "start_timestamp")
	assert.Contains(t, emptySpan, "end_timestamp")
	assert.Equal(t, "00000000000000000000000000000001", emptySpan["trace_id"])
	assert.Equal(t, "0000000000000001", emptySpan["span_id"])

	// Second span - zero values in attributes
	zeroSpan := allRows[1]
	assert.Equal(t, "span-with-zero-values", zeroSpan["name"])
	assert.Equal(t, "Error", zeroSpan["status_code"])
	assert.Contains(t, zeroSpan, "status_message")
	assert.Equal(t, "Something went wrong", zeroSpan["status_message"])
	assert.Contains(t, zeroSpan, "span.zero.int")
	assert.Equal(t, "0", zeroSpan["span.zero.int"])
	assert.Contains(t, zeroSpan, "span.empty.string")
	assert.Equal(t, "", zeroSpan["span.empty.string"])
	assert.Contains(t, zeroSpan, "span.false.bool")
	assert.Equal(t, "false", zeroSpan["span.false.bool"])
}
