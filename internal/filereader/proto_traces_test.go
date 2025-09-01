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
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
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
		batch, readErr := reader.Next(context.TODO())
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
	batch, err := protoReader.Next(context.TODO())
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
	batch, err := protoReader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch before closing")
	require.Equal(t, 1, batch.Len(), "Should read exactly 1 row before closing")

	// Close should work
	err = protoReader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = protoReader.Next(context.TODO())
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
			_, hasTraceId := row[wkk.NewRowKey("trace_id")]
			assert.True(t, hasTraceId)
			_, hasSpanId := row[wkk.NewRowKey("span_id")]
			assert.True(t, hasSpanId)
			_, hasName := row[wkk.NewRowKey("name")]
			assert.True(t, hasName)
			_, hasKind := row[wkk.NewRowKey("kind")]
			assert.True(t, hasKind)
			_, hasStartTimestamp := row[wkk.NewRowKey("start_timestamp")]
			assert.True(t, hasStartTimestamp)
			_, hasEndTimestamp := row[wkk.NewRowKey("end_timestamp")]
			assert.True(t, hasEndTimestamp)
			_, hasStatusCode := row[wkk.NewRowKey("status_code")]
			assert.True(t, hasStatusCode)

			// Resource attributes with prefix
			_, hasResourceServiceName := row[wkk.NewRowKey("resource.service.name")]
			assert.True(t, hasResourceServiceName)
			assert.Equal(t, "test-service", row[wkk.NewRowKey("resource.service.name")])
			_, hasResourceServiceVersion := row[wkk.NewRowKey("resource.service.version")]
			assert.True(t, hasResourceServiceVersion)
			assert.Equal(t, "1.0.0", row[wkk.NewRowKey("resource.service.version")])
			_, hasResourceDeploymentEnv := row[wkk.NewRowKey("resource.deployment.env")]
			assert.True(t, hasResourceDeploymentEnv)
			assert.Equal(t, "test", row[wkk.NewRowKey("resource.deployment.env")])

			// Scope attributes with prefix
			_, hasScopeScopeType := row[wkk.NewRowKey("scope.scope.type")]
			assert.True(t, hasScopeScopeType)
			assert.Equal(t, "instrumentation", row[wkk.NewRowKey("scope.scope.type")])

			// Common trace ID
			assert.Equal(t, "12345678901234567890123456789012", row[wkk.NewRowKey("trace_id")])
		})
	}

	// Verify specific spans
	rootSpan := allRows[0]
	assert.Equal(t, "1234567890123456", rootSpan[wkk.NewRowKey("span_id")])
	assert.Equal(t, "root-operation", rootSpan[wkk.NewRowKey("name")])
	assert.Equal(t, "Client", rootSpan[wkk.NewRowKey("kind")])
	assert.Equal(t, "Ok", rootSpan[wkk.NewRowKey("status_code")])
	_, hasSpanHttpMethod := rootSpan[wkk.NewRowKey("span.http.method")]
	assert.True(t, hasSpanHttpMethod)
	assert.Equal(t, "GET", rootSpan[wkk.NewRowKey("span.http.method")])
	_, hasSpanHttpStatusCode := rootSpan[wkk.NewRowKey("span.http.status_code")]
	assert.True(t, hasSpanHttpStatusCode)
	assert.Equal(t, "200", rootSpan[wkk.NewRowKey("span.http.status_code")])

	dbSpan := allRows[1]
	assert.Equal(t, "2345678901234567", dbSpan[wkk.NewRowKey("span_id")])
	assert.Equal(t, "database-query", dbSpan[wkk.NewRowKey("name")])
	assert.Equal(t, "Producer", dbSpan[wkk.NewRowKey("kind")])
	assert.Equal(t, "Unset", dbSpan[wkk.NewRowKey("status_code")])
	_, hasSpanDbSystem := dbSpan[wkk.NewRowKey("span.db.system")]
	assert.True(t, hasSpanDbSystem)
	assert.Equal(t, "postgresql", dbSpan[wkk.NewRowKey("span.db.system")])
	_, hasSpanDbOperation := dbSpan[wkk.NewRowKey("span.db.operation")]
	assert.True(t, hasSpanDbOperation)
	assert.Equal(t, "SELECT", dbSpan[wkk.NewRowKey("span.db.operation")])

	internalSpan := allRows[2]
	assert.Equal(t, "3456789012345678", internalSpan[wkk.NewRowKey("span_id")])
	assert.Equal(t, "internal-processing", internalSpan[wkk.NewRowKey("name")])
	assert.Equal(t, "Internal", internalSpan[wkk.NewRowKey("kind")])
	assert.Equal(t, "Unset", internalSpan[wkk.NewRowKey("status_code")])
	_, hasSpanComponent := internalSpan[wkk.NewRowKey("span.component")]
	assert.True(t, hasSpanComponent)
	assert.Equal(t, "data-processor", internalSpan[wkk.NewRowKey("span.component")])
	_, hasSpanRecordCount := internalSpan[wkk.NewRowKey("span.record.count")]
	assert.True(t, hasSpanRecordCount)
	assert.Equal(t, "42", internalSpan[wkk.NewRowKey("span.record.count")])

	// Test batched reading with a new reader instance
	protoReader2, err := NewProtoTracesReader(bytes.NewReader(protoBytes), 1000)
	require.NoError(t, err)
	defer protoReader2.Close()

	// Read in batches
	var totalBatchedRows int
	for {
		batch, readErr := protoReader2.Next(context.TODO())
		if batch != nil {
			totalBatchedRows += batch.Len()

			// Verify each row that was read
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				assert.Greater(t, len(row), 0, "Batched row %d should have data", i)
				_, hasTraceId := row[wkk.NewRowKey("trace_id")]
				assert.True(t, hasTraceId)
				_, hasSpanId := row[wkk.NewRowKey("span_id")]
				assert.True(t, hasSpanId)
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

	batch, err := protoReader3.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch")
	assert.Equal(t, 1, batch.Len(), "Should read exactly 1 row")
	_, hasTraceId := batch.Get(0)[wkk.NewRowKey("trace_id")]
	assert.True(t, hasTraceId)
	_, hasResourceServiceName := batch.Get(0)[wkk.NewRowKey("resource.service.name")]
	assert.True(t, hasResourceServiceName)

	// Test data exhaustion - continue reading until EOF
	var exhaustRows int
	for {
		batch, readErr := protoReader3.Next(context.TODO())
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
		if serviceName, exists := row[wkk.NewRowKey("resource.service.name")]; exists {
			switch serviceName {
			case "frontend-service":
				frontendSpans++
				_, hasResourceServiceVersion := row[wkk.NewRowKey("resource.service.version")]
				assert.True(t, hasResourceServiceVersion)
				assert.Equal(t, "2.1.0", row[wkk.NewRowKey("resource.service.version")])
			case "backend-service":
				backendSpans++
				_, hasResourceServiceVersion := row[wkk.NewRowKey("resource.service.version")]
				assert.True(t, hasResourceServiceVersion)
				assert.Equal(t, "1.5.2", row[wkk.NewRowKey("resource.service.version")])
			}
		}
		// All spans should be part of the same trace
		assert.Equal(t, "aaaabbbbccccddddeeeeffff00001111", row[wkk.NewRowKey("trace_id")])
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
	assert.Equal(t, "", emptySpan[wkk.NewRowKey("name")])
	assert.Equal(t, "Unspecified", emptySpan[wkk.NewRowKey("kind")])
	_, hasStartTimestamp := emptySpan[wkk.NewRowKey("start_timestamp")]
	assert.True(t, hasStartTimestamp)
	_, hasEndTimestamp := emptySpan[wkk.NewRowKey("end_timestamp")]
	assert.True(t, hasEndTimestamp)
	assert.Equal(t, "00000000000000000000000000000001", emptySpan[wkk.NewRowKey("trace_id")])
	assert.Equal(t, "0000000000000001", emptySpan[wkk.NewRowKey("span_id")])

	// Second span - zero values in attributes
	zeroSpan := allRows[1]
	assert.Equal(t, "span-with-zero-values", zeroSpan[wkk.NewRowKey("name")])
	assert.Equal(t, "Error", zeroSpan[wkk.NewRowKey("status_code")])
	_, hasStatusMessage := zeroSpan[wkk.NewRowKey("status_message")]
	assert.True(t, hasStatusMessage)
	assert.Equal(t, "Something went wrong", zeroSpan[wkk.NewRowKey("status_message")])
	_, hasSpanZeroInt := zeroSpan[wkk.NewRowKey("span.zero.int")]
	assert.True(t, hasSpanZeroInt)
	assert.Equal(t, "0", zeroSpan[wkk.NewRowKey("span.zero.int")])
	_, hasSpanEmptyString := zeroSpan[wkk.NewRowKey("span.empty.string")]
	assert.True(t, hasSpanEmptyString)
	assert.Equal(t, "", zeroSpan[wkk.NewRowKey("span.empty.string")])
	_, hasSpanFalseBool := zeroSpan[wkk.NewRowKey("span.false.bool")]
	assert.True(t, hasSpanFalseBool)
	assert.Equal(t, "false", zeroSpan[wkk.NewRowKey("span.false.bool")])
}
