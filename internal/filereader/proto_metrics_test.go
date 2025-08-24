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

func TestNewProtoMetricsReader(t *testing.T) {
	// Test with valid gzipped protobuf data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	// Verify the reader was initialized properly
	assert.NotNil(t, reader.datapointRows)
	assert.False(t, reader.closed)
	assert.Equal(t, 0, reader.currentIndex)
}

func TestNewProtoMetricsReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoMetricsReader(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL metrics")
}

func TestNewProtoMetricsReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoMetricsReader(emptyReader)
	// Empty data may create a valid but empty metrics object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL metrics")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty metrics should return EOF immediately
		rows := make([]Row, 1)
		rows[0] = make(Row)
		n, readErr := reader.Read(rows)
		assert.Equal(t, 0, n)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func TestProtoMetricsReader_Read(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Greater(t, len(allRows), 79, "Should read more rows now that we iterate over datapoints, not just metrics")

	// Verify each row has expected fields
	for i, row := range allRows {
		t.Run(fmt.Sprintf("row_%d", i), func(t *testing.T) {
			// Should have basic metric fields
			assert.Contains(t, row, "name", "Row should have metric name")
			assert.Contains(t, row, "type", "Row should have metric type")
			assert.Contains(t, row, "timestamp", "Row should have datapoint timestamp")

			// Check that name and type are not empty
			assert.NotEmpty(t, row["name"], "Metric name should not be empty")
			assert.NotEmpty(t, row["type"], "Metric type should not be empty")

			// All metric types should now have rollup fields in CardinalHQ format
			assert.Contains(t, row, "rollup_count", "Row should have rollup_count field")
			assert.Contains(t, row, "rollup_avg", "Row should have rollup_avg field")
			assert.Contains(t, row, "_cardinalhq.value", "Row should have _cardinalhq.value field")

			// Other fields may or may not be present depending on the metric
			// but if present, should have valid values
			if desc, exists := row["description"]; exists {
				assert.IsType(t, "", desc, "Description should be string")
			}
			if unit, exists := row["unit"]; exists {
				assert.IsType(t, "", unit, "Unit should be string")
			}
		})
	}

	t.Logf("Successfully read %d datapoint rows (was 79 metric rows)", len(allRows))
}

func TestProtoMetricsReader_ReadBatched(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read in batches of 5
	var totalRows int
	batchSize := 5

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
			assert.Contains(t, rows[i], "name", "Row %d should have name field", i)
			assert.Contains(t, rows[i], "type", "Row %d should have type field", i)
		}

		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Greater(t, totalRows, 79, "Should read more datapoint rows than the original 79 metric rows")
	t.Logf("Read %d datapoint rows in batches of %d (was 79 metric rows)", totalRows, batchSize)
}

func TestProtoMetricsReader_ReadSingleRow(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read one row at a time
	rows := make([]Row, 1)
	rows[0] = make(Row)

	n, err := reader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Greater(t, len(rows[0]), 0, "First row should have data")
	assert.Contains(t, rows[0], "name")
	assert.Contains(t, rows[0], "type")
}

func TestProtoMetricsReader_ResourceAndScopeAttributes(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read first few rows to check for resource/scope attributes
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 3, n, "Should read exactly 3 rows for attribute checking")

	// Check if any rows have resource or scope attributes
	foundResourceAttr := false
	foundScopeAttr := false

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
		}
	}

	t.Logf("Found resource attributes: %v, scope attributes: %v", foundResourceAttr, foundScopeAttr)
}

func TestProtoMetricsReader_EmptySlice(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice
	n, err := reader.Read([]Row{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProtoMetricsReader_Close(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Should read exactly 1 datapoint row before closing")

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

func TestProtoMetricsReader_ExhaustData(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
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

	assert.Greater(t, totalRows, 79, "Should read more datapoint rows than the original 79 metric rows")
	t.Logf("Successfully exhausted reader after reading %d datapoint rows (was 79 metric rows)", totalRows)
}

func TestProtoMetricsReader_MetricTypes(t *testing.T) {
	// Load test data
	file, err := os.Open("../../testdata/metrics/otel-metrics.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gzReader.Close()

	reader, err := NewProtoMetricsReader(gzReader)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows and collect metric types
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Greater(t, len(allRows), 79, "Should read more datapoint rows than the original 79 metric rows")

	metricTypes := make(map[string]int)
	metricNames := make(map[string]int)

	for _, row := range allRows {
		if metricType, exists := row["type"]; exists {
			if typeStr, ok := metricType.(string); ok {
				metricTypes[typeStr]++
			}
		}
		if metricName, exists := row["name"]; exists {
			if nameStr, ok := metricName.(string); ok {
				metricNames[nameStr]++
			}
		}
	}

	t.Logf("Found metric types: %+v", metricTypes)
	t.Logf("Found %d unique metric names", len(metricNames))

	// Verify that we still find the expected metric types and names
	assert.Equal(t, 3, len(metricTypes), "Should find exactly 3 metric types")
	assert.Equal(t, 58, len(metricNames), "Should find exactly 58 unique metric names")

	// Note: counts will be higher since we now have one row per datapoint
	assert.Greater(t, metricTypes["Sum"], 58, "Should have more Sum datapoint rows than 58 Sum metrics")
	assert.Greater(t, metricTypes["Histogram"], 14, "Should have more Histogram datapoint rows than 14 Histogram metrics")
	assert.Greater(t, metricTypes["Gauge"], 7, "Should have more Gauge datapoint rows than 7 Gauge metrics")
}

// Test parsing function directly with invalid data
func TestParseProtoToOtelMetrics_InvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("invalid protobuf data"))
	_, err := parseProtoToOtelMetrics(invalidData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal protobuf metrics")
}

// Test parsing function with read error
func TestParseProtoToOtelMetrics_ReadError(t *testing.T) {
	errorReader := &errorReaderImpl{shouldError: true}
	_, err := parseProtoToOtelMetrics(errorReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read data")
}

// errorReaderImpl is a test reader that can simulate read errors
type errorReaderImpl struct {
	shouldError bool
}

func (e *errorReaderImpl) Read(p []byte) (int, error) {
	if e.shouldError {
		return 0, errors.New("simulated read error")
	}
	return 0, io.EOF
}
