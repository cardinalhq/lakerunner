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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProtoTracesReader(t *testing.T) {
	// Test with valid gzipped protobuf data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	require.NotNil(t, protoReader)
	defer protoReader.Close()

	// Verify the reader was initialized properly
	assert.NotNil(t, protoReader.traces)
	assert.False(t, protoReader.closed)
	assert.Equal(t, 0, protoReader.traceResourceIndex)
	assert.Equal(t, 0, protoReader.traceQueueIndex)
}

func TestNewProtoTracesReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoTracesReader(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL traces")
}

func TestNewProtoTracesReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoTracesReader(emptyReader)
	// Empty data may create a valid but empty traces object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL traces")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty traces should return EOF immediately
		rows := make([]Row, 1)
		rows[0] = make(Row)
		n, readErr := reader.Read(rows)
		assert.Equal(t, 0, n)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func TestProtoTracesReader_Read(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read all rows
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 246, len(allRows), "Should read exactly 246 span rows from otel-traces.binpb.gz")

	// Verify each row has expected fields
	for i, row := range allRows {
		t.Run(fmt.Sprintf("row_%d", i), func(t *testing.T) {
			// Should have basic span fields
			assert.Contains(t, row, "trace_id", "Row should have trace ID")
			assert.Contains(t, row, "span_id", "Row should have span ID")
			assert.Contains(t, row, "name", "Row should have span name")
			assert.Contains(t, row, "kind", "Row should have span kind")

			// Check that trace_id and span_id are not empty
			assert.NotEmpty(t, row["trace_id"], "Trace ID should not be empty")
			assert.NotEmpty(t, row["span_id"], "Span ID should not be empty")
			assert.NotEmpty(t, row["name"], "Span name should not be empty")

			// Other fields may or may not be present depending on the span
			// but if present, should have valid values
			if kind, exists := row["kind"]; exists {
				assert.IsType(t, "", kind, "Kind should be string")
			}
			if status, exists := row["status_code"]; exists {
				assert.IsType(t, "", status, "Status code should be string")
			}
		})
	}

	t.Logf("Successfully read %d span rows (expected 246)", len(allRows))
}

func TestProtoTracesReader_ReadBatched(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read in batches of 5
	var totalRows int
	batchSize := 5

	for {
		rows := make([]Row, batchSize)
		for i := range rows {
			rows[i] = make(Row)
		}

		n, err := protoReader.Read(rows)
		totalRows += n

		// Verify each row that was read
		for i := 0; i < n; i++ {
			assert.Greater(t, len(rows[i]), 0, "Row %d should have data", i)
			assert.Contains(t, rows[i], "trace_id", "Row %d should have trace_id field", i)
			assert.Contains(t, rows[i], "span_id", "Row %d should have span_id field", i)
			assert.Contains(t, rows[i], "name", "Row %d should have name field", i)
		}

		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, 246, totalRows, "Should read exactly 246 rows in batches")
	t.Logf("Read %d rows in batches of %d (expected 246)", totalRows, batchSize)
}

func TestProtoTracesReader_ReadSingleRow(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read one row at a time
	rows := make([]Row, 1)
	rows[0] = make(Row)

	n, err := protoReader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Greater(t, len(rows[0]), 0, "First row should have data")
	assert.Contains(t, rows[0], "trace_id")
	assert.Contains(t, rows[0], "span_id")
	assert.Contains(t, rows[0], "name")
}

func TestProtoTracesReader_ResourceAndScopeAttributes(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read first few rows to check for resource/scope attributes
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := protoReader.Read(rows)
	require.NoError(t, err)
	require.Greater(t, n, 0, "Should read at least one row for attribute checking")

	// Check if any rows have resource or scope attributes
	foundResourceAttr := false
	foundScopeAttr := false
	foundSpanAttr := false

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
			if strings.HasPrefix(key, "span.") {
				foundSpanAttr = true
				t.Logf("Found span attribute: %s = %v", key, rows[i][key])
			}
		}
	}

	t.Logf("Found resource attributes: %v, scope attributes: %v, span attributes: %v",
		foundResourceAttr, foundScopeAttr, foundSpanAttr)
}

func TestProtoTracesReader_EmptySlice(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read with empty slice
	n, err := protoReader.Read([]Row{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProtoTracesReader_Close(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := protoReader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Should read exactly 1 row before closing")

	// Close should work
	err = protoReader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	rows[0] = make(Row)
	_, err = protoReader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = protoReader.Close()
	assert.NoError(t, err)
}

func TestProtoTracesReader_ExhaustData(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read all data
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	totalRows := len(allRows)

	// Further reads should return EOF
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := protoReader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))

	assert.Equal(t, 246, totalRows, "Should read exactly 246 rows before exhaustion")
	t.Logf("Successfully exhausted reader after reading %d rows (expected 246)", totalRows)
}

func TestProtoTracesReader_SpanFields(t *testing.T) {
	// Load test data
	file, err := os.Open("testdata/otel-traces.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	protoReader, err := NewProtoTracesReader(gzReader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Read all rows and collect span field info
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 246, len(allRows), "Should read exactly 246 rows for field analysis")

	spanKinds := make(map[string]int)
	statusCodes := make(map[string]int)
	nameCount := 0

	for _, row := range allRows {
		if name, exists := row["name"]; exists {
			if nameStr, ok := name.(string); ok && nameStr != "" {
				nameCount++
			}
		}
		if kind, exists := row["kind"]; exists {
			if kindStr, ok := kind.(string); ok {
				spanKinds[kindStr]++
			}
		}
		if statusCode, exists := row["status_code"]; exists {
			if statusStr, ok := statusCode.(string); ok {
				statusCodes[statusStr]++
			}
		}
	}

	t.Logf("Found %d spans with names", nameCount)
	t.Logf("Found span kinds: %+v", spanKinds)
	t.Logf("Found status codes: %+v", statusCodes)

	// Basic validation - all spans should have names
	assert.Equal(t, 246, nameCount, "Should have 246 spans with names")

	// Verify specific span type distribution based on test data
	assert.Equal(t, 82, spanKinds["Client"], "Should have 82 Client spans")
	assert.Equal(t, 100, spanKinds["Internal"], "Should have 100 Internal spans")
	assert.Equal(t, 64, spanKinds["Server"], "Should have 64 Server spans")
	assert.Equal(t, 246, statusCodes["Unset"], "Should have 246 Unset status codes")
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
