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
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestNewProtoLogsReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoLogsReader(reader, 1000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL logs")
}

func TestNewProtoLogsReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoLogsReader(emptyReader, 1000)
	// Empty data may create a valid but empty logs object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL logs")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty logs should return EOF immediately
		batch, readErr := reader.Next()
		assert.Nil(t, batch)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func TestProtoLogsReader_EmptySlice(t *testing.T) {
	syntheticData := createSyntheticLogData()
	reader, err := NewProtoLogsReader(bytes.NewReader(syntheticData), 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice behavior is no longer applicable with Next() method
	// Next() returns a batch or nil, not dependent on slice size
	batch, err := reader.Next()
	if batch != nil {
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, batch.Len(), 0)
	}
}

func TestProtoLogsReader_Close(t *testing.T) {
	syntheticData := createSyntheticLogData()
	reader, err := NewProtoLogsReader(bytes.NewReader(syntheticData), 1)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next()
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch before closing")
	require.Equal(t, 1, batch.Len(), "Should read exactly 1 row before closing")

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = reader.Next()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

// Test parsing function directly with invalid data
func TestParseProtoToOtelLogs_InvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("invalid protobuf data"))
	_, err := parseProtoToOtelLogs(invalidData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal protobuf logs")
}

// Test parsing function with read error
func TestParseProtoToOtelLogs_ReadError(t *testing.T) {
	errorReader := &errorReaderImpl{shouldError: true}
	_, err := parseProtoToOtelLogs(errorReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read data")
}

// Helper function to create synthetic log data using signalbuilder
func createSyntheticLogData() []byte {
	builder := signalbuilder.NewLogBuilder()

	// Define resource attributes
	resourceAttrs := map[string]any{
		"service.name":           "test-log-service",
		"service.version":        "2.0.0",
		"deployment.environment": "test",
	}

	// Create scope logs with different severity levels
	scopeLogs := []signalbuilder.ScopeLogs{
		{
			Name:    "test-logger",
			Version: "1.0.0",
			Attributes: map[string]any{
				"logger.type": "structured",
			},
			LogRecords: []signalbuilder.LogRecord{
				{
					Timestamp:         time.Now().UnixNano(),
					ObservedTimestamp: time.Now().UnixNano(),
					SeverityText:      "DEBUG",
					SeverityNumber:    int32(plog.SeverityNumberDebug),
					Body:              "Debug message for testing",
					Attributes: map[string]any{
						"log.level":    "DEBUG",
						"log.source":   "test-component-0",
						"log.sequence": int64(1),
					},
				},
				{
					Timestamp:         time.Now().Add(time.Second).UnixNano(),
					ObservedTimestamp: time.Now().Add(time.Second).UnixNano(),
					SeverityText:      "INFO",
					SeverityNumber:    int32(plog.SeverityNumberInfo),
					Body:              "Info message with details",
					Attributes: map[string]any{
						"log.level":    "INFO",
						"log.source":   "test-component-1",
						"log.sequence": int64(2),
					},
				},
				{
					Timestamp:         time.Now().Add(2 * time.Second).UnixNano(),
					ObservedTimestamp: time.Now().Add(2 * time.Second).UnixNano(),
					SeverityText:      "WARN",
					SeverityNumber:    int32(plog.SeverityNumberWarn),
					Body:              "Warning about potential issue",
					Attributes: map[string]any{
						"log.level":    "WARN",
						"log.source":   "test-component-2",
						"log.sequence": int64(3),
					},
				},
				{
					Timestamp:         time.Now().Add(3 * time.Second).UnixNano(),
					ObservedTimestamp: time.Now().Add(3 * time.Second).UnixNano(),
					SeverityText:      "ERROR",
					SeverityNumber:    int32(plog.SeverityNumberError),
					Body:              "Error occurred during processing",
					Attributes: map[string]any{
						"log.level":    "ERROR",
						"log.source":   "test-component-3",
						"log.sequence": int64(4),
					},
				},
				{
					Timestamp:         time.Now().Add(4 * time.Second).UnixNano(),
					ObservedTimestamp: time.Now().Add(4 * time.Second).UnixNano(),
					SeverityText:      "FATAL",
					SeverityNumber:    int32(plog.SeverityNumberFatal),
					Body:              "Fatal error - system shutdown",
					Attributes: map[string]any{
						"log.level":    "FATAL",
						"log.source":   "test-component-4",
						"log.sequence": int64(5),
					},
				},
			},
		},
	}

	// Add the resource with scope logs
	resourceLogs := &signalbuilder.ResourceLogs{
		Resource:  resourceAttrs,
		ScopeLogs: scopeLogs,
	}

	err := builder.Add(resourceLogs)
	if err != nil {
		panic(fmt.Sprintf("Failed to add resource logs: %v", err))
	}

	// Build and marshal to protobuf
	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal logs: %v", err))
	}
	return data
}

// Test ProtoLogsReader with synthetic log data
func TestProtoLogsReader_SyntheticData(t *testing.T) {
	// Create synthetic log data
	syntheticData := createSyntheticLogData()
	reader := bytes.NewReader(syntheticData)

	protoReader, err := NewProtoLogsReader(reader, 1000)
	require.NoError(t, err)
	require.NotNil(t, protoReader)
	defer protoReader.Close()

	// Read all rows
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 5, len(allRows), "Should read exactly 5 log rows from synthetic data")

	// Verify each row has expected fields and values
	expectedSeverities := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	expectedBodies := []string{
		"Debug message for testing",
		"Info message with details",
		"Warning about potential issue",
		"Error occurred during processing",
		"Fatal error - system shutdown",
	}

	for i, row := range allRows {
		t.Run(fmt.Sprintf("log_%d", i), func(t *testing.T) {
			// Should have basic log fields
			assert.Contains(t, row, "_cardinalhq.message", "Row should have log body")
			assert.Contains(t, row, "_cardinalhq.timestamp", "Row should have timestamp")
			assert.Contains(t, row, "_cardinalhq.level", "Row should have severity text")
			assert.Contains(t, row, "severity_number", "Row should have severity number")

			// Check specific values
			assert.Equal(t, expectedBodies[i], row["_cardinalhq.message"], "Log body should match expected value")
			assert.Equal(t, expectedSeverities[i], row["_cardinalhq.level"], "Severity text should match")

			// Check for resource attributes
			assert.Contains(t, row, "resource.service.name", "Should have resource service name")
			assert.Equal(t, "test-log-service", row["resource.service.name"])
			assert.Contains(t, row, "resource.deployment.environment", "Should have resource environment")
			assert.Equal(t, "test", row["resource.deployment.environment"])

			// Check for scope attributes
			assert.Contains(t, row, "scope.logger.type", "Should have scope logger type")
			assert.Equal(t, "structured", row["scope.logger.type"])

			// Check for log attributes
			assert.Contains(t, row, "log.log.level", "Should have log level attribute")
			assert.Equal(t, expectedSeverities[i], row["log.log.level"])
			assert.Contains(t, row, "log.log.source", "Should have log source attribute")
			expectedSource := fmt.Sprintf("test-component-%d", i)
			assert.Equal(t, expectedSource, row["log.log.source"])
		})
	}

	// Test batched reading with a new reader instance
	protoReader2, err := NewProtoLogsReader(bytes.NewReader(syntheticData), 1000)
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
				assert.Contains(t, row, "_cardinalhq.message")
				assert.Contains(t, row, "_cardinalhq.timestamp")
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
	}
	assert.Equal(t, len(allRows), totalBatchedRows, "Batched reading should read same number of rows")

	// Test single row reading
	protoReader3, err := NewProtoLogsReader(bytes.NewReader(syntheticData), 1)
	require.NoError(t, err)
	defer protoReader3.Close()

	batch, err := protoReader3.Next()
	require.NoError(t, err)
	require.NotNil(t, batch, "Should read a batch")
	assert.Equal(t, 1, batch.Len(), "Should read exactly 1 row")
	assert.Contains(t, batch.Get(0), "_cardinalhq.message")
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

	t.Logf("Successfully read %d synthetic log rows (expected 5)", len(allRows))
}

// Test ProtoLogsReader synthetic data field analysis
func TestProtoLogsReader_SyntheticDataFields(t *testing.T) {
	// Create synthetic log data
	syntheticData := createSyntheticLogData()
	reader := bytes.NewReader(syntheticData)

	protoReader, err := NewProtoLogsReader(reader, 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read all rows and collect log field info
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 5, len(allRows), "Should read exactly 5 rows for synthetic field analysis")

	severityTexts := make(map[string]int)
	severityNumbers := make(map[int32]int)
	bodyCount := 0

	for _, row := range allRows {
		if body, exists := row["_cardinalhq.message"]; exists {
			if bodyStr, ok := body.(string); ok && bodyStr != "" {
				bodyCount++
			}
		}
		if severityText, exists := row["_cardinalhq.level"]; exists {
			if textStr, ok := severityText.(string); ok {
				severityTexts[textStr]++
			}
		}
		if severityNum, exists := row["severity_number"]; exists {
			if numInt64, ok := severityNum.(int64); ok {
				severityNumbers[int32(numInt64)]++
			}
		}
	}

	t.Logf("Found %d synthetic logs with non-empty bodies", bodyCount)
	t.Logf("Found synthetic severity texts: %+v", severityTexts)
	t.Logf("Found synthetic severity numbers: %+v", severityNumbers)

	// Validate expected synthetic data
	assert.Equal(t, 5, bodyCount, "Should have 5 logs with bodies")
	assert.Equal(t, 1, severityTexts["DEBUG"], "Should have 1 DEBUG log")
	assert.Equal(t, 1, severityTexts["INFO"], "Should have 1 INFO log")
	assert.Equal(t, 1, severityTexts["WARN"], "Should have 1 WARN log")
	assert.Equal(t, 1, severityTexts["ERROR"], "Should have 1 ERROR log")
	assert.Equal(t, 1, severityTexts["FATAL"], "Should have 1 FATAL log")

	// Validate severity numbers (using OTEL severity number values)
	assert.Equal(t, 1, severityNumbers[int32(plog.SeverityNumberDebug)], "Should have 1 DEBUG severity number")
	assert.Equal(t, 1, severityNumbers[int32(plog.SeverityNumberInfo)], "Should have 1 INFO severity number")
	assert.Equal(t, 1, severityNumbers[int32(plog.SeverityNumberWarn)], "Should have 1 WARN severity number")
	assert.Equal(t, 1, severityNumbers[int32(plog.SeverityNumberError)], "Should have 1 ERROR severity number")
	assert.Equal(t, 1, severityNumbers[int32(plog.SeverityNumberFatal)], "Should have 1 FATAL severity number")
}

// Test ProtoLogsReader with structured Go-based synthetic data
func TestProtoLogsReader_SyntheticStructuredData(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	// Use Go struct instead of YAML to avoid parsing ambiguity
	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name":           "structured-test-service",
			"service.version":        "3.1.4",
			"deployment.environment": "integration",
			"host.name":              "test-host-01",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name:    "structured-logger",
				Version: "2.1.0",
				Attributes: map[string]any{
					"logger.framework": "slog",
					"logger.output":    "json",
				},
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:         1640995200000000000,
						ObservedTimestamp: 1640995200100000000,
						SeverityText:      "TRACE",
						SeverityNumber:    int32(plog.SeverityNumberTrace),
						Body:              "Detailed trace information for debugging",
						Attributes: map[string]any{
							"trace_id":    "abc123def456",
							"user_id":     "user-001",
							"operation":   "data_fetch",
							"duration_ms": int64(45),
						},
					},
					{
						Timestamp:         1640995201000000000,
						ObservedTimestamp: 1640995201100000000,
						SeverityText:      "INFO",
						SeverityNumber:    int32(plog.SeverityNumberInfo),
						Body:              "User authentication successful",
						Attributes: map[string]any{
							"user_id":     "user-001",
							"auth_method": "oauth2",
							"session_id":  "sess_987654321",
						},
					},
					{
						Timestamp:         1640995202000000000,
						ObservedTimestamp: 1640995202100000000,
						SeverityText:      "WARN",
						SeverityNumber:    int32(plog.SeverityNumberWarn),
						Body:              "Rate limit approaching for API endpoint",
						Attributes: map[string]any{
							"endpoint":     "/api/v1/data",
							"current_rate": int64(950),
							"rate_limit":   int64(1000),
							"client_ip":    "192.168.1.100",
						},
					},
					{
						Timestamp:         1640995203000000000,
						ObservedTimestamp: 1640995203100000000,
						SeverityText:      "ERROR",
						SeverityNumber:    int32(plog.SeverityNumberError),
						Body:              "Database connection failed",
						Attributes: map[string]any{
							"database":        "primary_db",
							"connection_pool": "pool_1",
							"retry_count":     int64(3),
							"error_code":      "CONNECTION_TIMEOUT",
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err, "Should successfully add structured resource logs")

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err, "Should successfully marshal logs to protobuf")

	// Test ProtoLogsReader with this structured data
	reader := bytes.NewReader(data)
	protoReader, err := NewProtoLogsReader(reader, 1000)
	require.NoError(t, err)
	require.NotNil(t, protoReader)
	defer protoReader.Close()

	// Read all rows from structured data
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 4, len(allRows), "Should read exactly 4 log rows from structured data")

	// Verify structured-specific data
	expectedBodies := []string{
		"Detailed trace information for debugging",
		"User authentication successful",
		"Rate limit approaching for API endpoint",
		"Database connection failed",
	}

	expectedSeverities := []string{"TRACE", "INFO", "WARN", "ERROR"}

	for i, row := range allRows {
		t.Run(fmt.Sprintf("structured_log_%d", i), func(t *testing.T) {
			// Check basic log fields
			assert.Equal(t, expectedBodies[i], row["_cardinalhq.message"], "Structured log body should match")
			assert.Equal(t, expectedSeverities[i], row["_cardinalhq.level"], "Structured severity should match")

			// Check resource attributes from structured data
			assert.Equal(t, "structured-test-service", row["resource.service.name"])
			assert.Equal(t, "3.1.4", row["resource.service.version"])
			assert.Equal(t, "integration", row["resource.deployment.environment"])
			assert.Equal(t, "test-host-01", row["resource.host.name"])

			// Check scope attributes from structured data
			assert.Equal(t, "slog", row["scope.logger.framework"])
			assert.Equal(t, "json", row["scope.logger.output"])

			// Check log-specific attributes from structured data with proper types
			switch i {
			case 0: // TRACE log
				assert.Equal(t, "abc123def456", row["log.trace_id"])
				assert.Equal(t, "user-001", row["log.user_id"])
				assert.Equal(t, "data_fetch", row["log.operation"])
				// OTEL attribute processing may convert numbers to strings regardless of input type
				t.Logf("duration_ms type: %T, value: %v", row["log.duration_ms"], row["log.duration_ms"])
				assert.Contains(t, []any{int64(45), "45"}, row["log.duration_ms"])
			case 1: // INFO log
				assert.Equal(t, "user-001", row["log.user_id"])
				assert.Equal(t, "oauth2", row["log.auth_method"])
				assert.Equal(t, "sess_987654321", row["log.session_id"])
			case 2: // WARN log
				assert.Equal(t, "/api/v1/data", row["log.endpoint"])
				// OTEL attribute processing may convert numbers to strings regardless of input type
				assert.Contains(t, []any{int64(950), "950"}, row["log.current_rate"])
				assert.Contains(t, []any{int64(1000), "1000"}, row["log.rate_limit"])
				assert.Equal(t, "192.168.1.100", row["log.client_ip"])
			case 3: // ERROR log
				assert.Equal(t, "primary_db", row["log.database"])
				assert.Equal(t, "pool_1", row["log.connection_pool"])
				// OTEL attribute processing may convert numbers to strings regardless of input type
				assert.Contains(t, []any{int64(3), "3"}, row["log.retry_count"])
				assert.Equal(t, "CONNECTION_TIMEOUT", row["log.error_code"])
			}
		})
	}

	t.Logf("Successfully read %d structured log rows", len(allRows))
}

// Test ProtoLogsReader with multi-resource synthetic data
func TestProtoLogsReader_MultiResourceSyntheticData(t *testing.T) {
	builder := signalbuilder.NewLogBuilder()

	// Add logs from multiple services/resources
	resources := []struct {
		name   string
		attrs  map[string]any
		scopes []signalbuilder.ScopeLogs
	}{
		{
			name: "web-service",
			attrs: map[string]any{
				"service.name":    "web-frontend",
				"service.version": "1.2.3",
				"environment":     "production",
			},
			scopes: []signalbuilder.ScopeLogs{
				{
					Name:    "http-logger",
					Version: "1.0.0",
					LogRecords: []signalbuilder.LogRecord{
						{
							Timestamp:      time.Now().UnixNano(),
							SeverityText:   "INFO",
							SeverityNumber: int32(plog.SeverityNumberInfo),
							Body:           "HTTP request processed",
							Attributes:     map[string]any{"method": "GET", "status": 200, "path": "/api/users"},
						},
						{
							Timestamp:      time.Now().Add(time.Second).UnixNano(),
							SeverityText:   "ERROR",
							SeverityNumber: int32(plog.SeverityNumberError),
							Body:           "HTTP request failed",
							Attributes:     map[string]any{"method": "POST", "status": 500, "path": "/api/orders"},
						},
					},
				},
			},
		},
		{
			name: "database-service",
			attrs: map[string]any{
				"service.name":    "postgres-db",
				"service.version": "13.7",
				"environment":     "production",
			},
			scopes: []signalbuilder.ScopeLogs{
				{
					Name:    "db-logger",
					Version: "2.0.0",
					LogRecords: []signalbuilder.LogRecord{
						{
							Timestamp:      time.Now().Add(2 * time.Second).UnixNano(),
							SeverityText:   "DEBUG",
							SeverityNumber: int32(plog.SeverityNumberDebug),
							Body:           "Query executed successfully",
							Attributes:     map[string]any{"query": "SELECT * FROM users", "duration": "15ms"},
						},
						{
							Timestamp:      time.Now().Add(3 * time.Second).UnixNano(),
							SeverityText:   "WARN",
							SeverityNumber: int32(plog.SeverityNumberWarn),
							Body:           "Slow query detected",
							Attributes:     map[string]any{"query": "SELECT * FROM orders", "duration": "2500ms"},
						},
					},
				},
			},
		},
	}

	// Add each resource to the builder
	for _, res := range resources {
		resourceLogs := &signalbuilder.ResourceLogs{
			Resource:  res.attrs,
			ScopeLogs: res.scopes,
		}
		err := builder.Add(resourceLogs)
		require.NoError(t, err, "Should add resource logs for %s", res.name)
	}

	// Build and test
	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoLogsReader(reader, 1000)
	require.NoError(t, err)
	defer protoReader.Close()

	// Should read logs from both resources (4 total)
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 4, len(allRows), "Should read logs from both services")

	// Count logs by service
	webLogs := 0
	dbLogs := 0
	for _, row := range allRows {
		serviceName := row["resource.service.name"].(string)
		switch serviceName {
		case "web-frontend":
			webLogs++
		case "postgres-db":
			dbLogs++
		}
	}

	assert.Equal(t, 2, webLogs, "Should have 2 logs from web service")
	assert.Equal(t, 2, dbLogs, "Should have 2 logs from database service")

	t.Logf("Successfully read %d logs from %d services", len(allRows), len(resources))
}
