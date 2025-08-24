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
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestNewProtoLogsReader(t *testing.T) {
	// Test with valid gzipped protobuf data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	// Verify the reader was initialized properly
	assert.NotNil(t, reader.logs)
	assert.False(t, reader.closed)
	assert.Equal(t, 0, reader.resourceIndex)
	assert.Equal(t, 0, reader.scopeIndex)
	assert.Equal(t, 0, reader.logIndex)
}

func TestNewProtoLogsReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoLogsReader(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL logs")
}

func TestNewProtoLogsReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoLogsReader(emptyReader)
	// Empty data may create a valid but empty logs object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL logs")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty logs should return EOF immediately
		rows := make([]Row, 1)
		rows[0] = make(Row)
		n, readErr := reader.Read(rows)
		assert.Equal(t, 0, n)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func TestProtoLogsReader_Read(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 525, len(allRows), "Should read exactly 525 rows from logs_160396104.binpb")

	// Verify each row has expected fields
	for i, row := range allRows {
		t.Run(fmt.Sprintf("row_%d", i), func(t *testing.T) {
			// Should have basic log fields
			assert.Contains(t, row, "body", "Row should have log body")
			assert.Contains(t, row, "timestamp", "Row should have timestamp")

			// Check that body is not empty
			assert.NotEmpty(t, row["body"], "Log body should not be empty")

			// Other fields may or may not be present depending on the log
			// but if present, should have valid values
			if severity, exists := row["severity_text"]; exists {
				assert.IsType(t, "", severity, "Severity text should be string")
			}
			if severityNum, exists := row["severity_number"]; exists {
				assert.IsType(t, int32(0), severityNum, "Severity number should be int32")
			}
		})
	}

	t.Logf("Successfully read %d log rows", len(allRows))
}

func TestProtoLogsReader_ReadBatched(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read in batches of 3
	var totalRows int
	batchSize := 3

	for {
		rows := make([]Row, batchSize)
		for i := range rows {
			rows[i] = make(Row)
		}

		n, err := reader.Read(rows)
		totalRows += n

		// Verify each row that was read
		for i := 0; i < n; i++ {
			assert.Greater(t, len(rows[i]), 0, "Row %d should have data", i)
			assert.Contains(t, rows[i], "body", "Row %d should have body field", i)
			assert.Contains(t, rows[i], "timestamp", "Row %d should have timestamp field", i)
		}

		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, 525, totalRows, "Should read exactly 525 rows in batches")
	t.Logf("Read %d rows in batches of %d (expected 525)", totalRows, batchSize)
}

func TestProtoLogsReader_ReadSingleRow(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read one row at a time
	rows := make([]Row, 1)
	rows[0] = make(Row)

	n, err := reader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Greater(t, len(rows[0]), 0, "First row should have data")
	assert.Contains(t, rows[0], "body")
	assert.Contains(t, rows[0], "timestamp")
}

func TestProtoLogsReader_ResourceAndScopeAttributes(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read first few rows to check for resource/scope attributes
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Greater(t, n, 0, "Should read at least one row for attribute checking")

	// Check if any rows have resource or scope attributes
	foundResourceAttr := false
	foundScopeAttr := false
	foundLogAttr := false

	for i := 0; i < n; i++ {
		for key := range rows[i] {
			if strings.HasPrefix(key, "resource.") {
				foundResourceAttr = true
				t.Logf("Found resource attribute: %s = %v", key, rows[i][key])
			}
			if strings.HasPrefix(key, "scope.") {
				foundScopeAttr = true
				t.Logf("Found scope attribute: %s = %v", key, rows[i][key])
			}
			if strings.HasPrefix(key, "log.") {
				foundLogAttr = true
				t.Logf("Found log attribute: %s = %v", key, rows[i][key])
			}
		}
	}

	t.Logf("Found resource attributes: %v, scope attributes: %v, log attributes: %v",
		foundResourceAttr, foundScopeAttr, foundLogAttr)
}

func TestProtoLogsReader_EmptySlice(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice
	n, err := reader.Read([]Row{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProtoLogsReader_Close(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Should read exactly 1 row before closing")

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	rows[0] = make(Row)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

func TestProtoLogsReader_ExhaustData(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read all data
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	totalRows := len(allRows)

	// Further reads should return EOF
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))

	assert.Equal(t, 525, totalRows, "Should read exactly 525 rows before exhaustion")
	t.Logf("Successfully exhausted reader after reading %d rows (expected 525)", totalRows)
}

func TestProtoLogsReader_LogFields(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/logs/otel-logs.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoLogsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows and collect log field info
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 525, len(allRows), "Should read exactly 525 rows for field analysis")

	severityTexts := make(map[string]int)
	severityNumbers := make(map[int32]int)
	bodyCount := 0

	for _, row := range allRows {
		if body, exists := row["body"]; exists {
			if bodyStr, ok := body.(string); ok && bodyStr != "" {
				bodyCount++
			}
		}
		if severityText, exists := row["severity_text"]; exists {
			if textStr, ok := severityText.(string); ok {
				severityTexts[textStr]++
			}
		}
		if severityNum, exists := row["severity_number"]; exists {
			if numInt32, ok := severityNum.(int32); ok {
				severityNumbers[numInt32]++
			}
		}
	}

	t.Logf("Found %d logs with non-empty bodies", bodyCount)
	t.Logf("Found severity texts: %+v", severityTexts)
	t.Logf("Found severity numbers: %+v", severityNumbers)

	// Basic validation - at least some logs should have bodies
	assert.Greater(t, bodyCount, 0, "Should have at least some logs with bodies")
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

// Helper function to create synthetic log data
func createSyntheticLogData() []byte {
	logs := plog.NewLogs()

	// Create a resource log
	resourceLog := logs.ResourceLogs().AppendEmpty()

	// Add resource attributes
	resourceLog.Resource().Attributes().PutStr("service.name", "test-log-service")
	resourceLog.Resource().Attributes().PutStr("service.version", "2.0.0")
	resourceLog.Resource().Attributes().PutStr("deployment.environment", "test")

	// Create scope logs
	scopeLog := resourceLog.ScopeLogs().AppendEmpty()
	scopeLog.Scope().SetName("test-logger")
	scopeLog.Scope().SetVersion("1.0.0")
	scopeLog.Scope().Attributes().PutStr("logger.type", "structured")

	// Create log records with different severity levels
	severities := []struct {
		text   string
		number plog.SeverityNumber
		body   string
	}{
		{"DEBUG", plog.SeverityNumberDebug, "Debug message for testing"},
		{"INFO", plog.SeverityNumberInfo, "Info message with details"},
		{"WARN", plog.SeverityNumberWarn, "Warning about potential issue"},
		{"ERROR", plog.SeverityNumberError, "Error occurred during processing"},
		{"FATAL", plog.SeverityNumberFatal, "Fatal error - system shutdown"},
	}

	baseTime := time.Now()
	for i, sev := range severities {
		logRecord := scopeLog.LogRecords().AppendEmpty()
		logRecord.Body().SetStr(sev.body)
		logRecord.SetSeverityText(sev.text)
		logRecord.SetSeverityNumber(sev.number)
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(time.Duration(i) * time.Second)))
		logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(time.Duration(i) * time.Second)))

		// Add log-specific attributes
		logRecord.Attributes().PutStr("log.level", sev.text)
		logRecord.Attributes().PutStr("log.source", fmt.Sprintf("test-component-%d", i))
		logRecord.Attributes().PutInt("log.sequence", int64(i+1))
	}

	// Marshal to protobuf
	marshaler := &plog.ProtoMarshaler{}
	data, _ := marshaler.MarshalLogs(logs)
	return data
}

// Test ProtoLogsReader with synthetic log data
func TestProtoLogsReader_SyntheticData(t *testing.T) {
	// Create synthetic log data
	syntheticData := createSyntheticLogData()
	reader := bytes.NewReader(syntheticData)

	protoReader, err := NewProtoLogsReader(reader)
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
			assert.Contains(t, row, "body", "Row should have log body")
			assert.Contains(t, row, "timestamp", "Row should have timestamp")
			assert.Contains(t, row, "severity_text", "Row should have severity text")
			assert.Contains(t, row, "severity_number", "Row should have severity number")

			// Check specific values
			assert.Equal(t, expectedBodies[i], row["body"], "Log body should match expected value")
			assert.Equal(t, expectedSeverities[i], row["severity_text"], "Severity text should match")

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

	t.Logf("Successfully read %d synthetic log rows (expected 5)", len(allRows))
}

// Test ProtoLogsReader synthetic data field analysis
func TestProtoLogsReader_SyntheticDataFields(t *testing.T) {
	// Create synthetic log data
	syntheticData := createSyntheticLogData()
	reader := bytes.NewReader(syntheticData)

	protoReader, err := NewProtoLogsReader(reader)
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
		if body, exists := row["body"]; exists {
			if bodyStr, ok := body.(string); ok && bodyStr != "" {
				bodyCount++
			}
		}
		if severityText, exists := row["severity_text"]; exists {
			if textStr, ok := severityText.(string); ok {
				severityTexts[textStr]++
			}
		}
		if severityNum, exists := row["severity_number"]; exists {
			if numInt32, ok := severityNum.(int32); ok {
				severityNumbers[numInt32]++
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
