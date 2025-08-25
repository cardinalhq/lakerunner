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
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskSortingReader_BasicSorting(t *testing.T) {
	// Create test data with unsorted metric names, TIDs, and timestamps
	testRows := []Row{
		{
			"_cardinalhq.name":      "metric_z",
			"_cardinalhq.tid":       int64(200),
			"_cardinalhq.timestamp": int64(3000),
			"value":                 float64(1.0),
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(1000),
			"value":                 float64(2.0),
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(2000),
			"value":                 float64(3.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all rows
	var allRows []Row
	for {
		rows := make([]Row, 10)
		n, err := sortingReader.Read(rows)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			allRows = append(allRows, rows[i])
		}
	}

	// Verify sorting: metric_a (both rows), then metric_z
	require.Len(t, allRows, 3)

	// First two rows should be metric_a, sorted by timestamp
	assert.Equal(t, "metric_a", allRows[0]["_cardinalhq.name"])
	assert.Equal(t, int64(1000), allRows[0]["_cardinalhq.timestamp"])

	assert.Equal(t, "metric_a", allRows[1]["_cardinalhq.name"])
	assert.Equal(t, int64(2000), allRows[1]["_cardinalhq.timestamp"])

	// Last row should be metric_z
	assert.Equal(t, "metric_z", allRows[2]["_cardinalhq.name"])
	assert.Equal(t, int64(3000), allRows[2]["_cardinalhq.timestamp"])
}

func TestDiskSortingReader_TypePreservation(t *testing.T) {
	// Test various types that need to be preserved through CBOR
	testRow := Row{
		"_cardinalhq.name":      "test_metric",
		"_cardinalhq.tid":       int64(12345),
		"_cardinalhq.timestamp": int64(1640995200000),
		"string_field":          "test_string",
		"int64_field":           int64(9223372036854775807), // Max int64
		"float64_field":         float64(3.14159),
		"byte_slice":            []byte{0x01, 0x02, 0x03},
		"float64_slice":         []float64{1.1, 2.2, 3.3},
		"bool_field":            true,
		"nil_field":             nil,
	}

	mockReader := NewMockReader([]Row{testRow})
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read the row back
	rows := make([]Row, 1)
	n, err := sortingReader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	decoded := rows[0]

	// Verify all types are preserved
	assert.Equal(t, "test_metric", decoded["_cardinalhq.name"])
	assert.Equal(t, int64(12345), decoded["_cardinalhq.tid"])
	assert.Equal(t, int64(1640995200000), decoded["_cardinalhq.timestamp"])
	assert.Equal(t, "test_string", decoded["string_field"])
	assert.Equal(t, int64(9223372036854775807), decoded["int64_field"])
	assert.Equal(t, float64(3.14159), decoded["float64_field"])
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded["byte_slice"])
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, decoded["float64_slice"])
	assert.Equal(t, true, decoded["bool_field"])
	assert.Nil(t, decoded["nil_field"])
}

func TestDiskSortingReader_EmptyInput(t *testing.T) {
	mockReader := NewMockReader([]Row{})
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Should get EOF immediately
	rows := make([]Row, 1)
	n, err := sortingReader.Read(rows)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestDiskSortingReader_MissingFields(t *testing.T) {
	// Test row missing required sort fields
	testRows := []Row{
		{
			"_cardinalhq.name": "metric_a",
			// Missing TID and timestamp
			"value": float64(1.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Should fail when trying to read due to missing sort fields
	rows := make([]Row, 1)
	_, err = sortingReader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required sort key fields")
}

func TestDiskSortingReader_CleanupOnError(t *testing.T) {
	// Create reader that will fail during reading
	mockReader := &MockReader{
		rows:      []Row{{"test": "value"}},
		readError: fmt.Errorf("simulated read error"),
	}

	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)

	tempFileName := sortingReader.tempFile.Name()

	// Try to read - should fail
	rows := make([]Row, 1)
	_, err = sortingReader.Read(rows)
	assert.Error(t, err)

	// Close should clean up temp file
	err = sortingReader.Close()
	assert.NoError(t, err)

	// Verify temp file was removed
	_, err = os.Stat(tempFileName)
	assert.True(t, os.IsNotExist(err), "Temp file should be removed")
}

// MockReader for testing
type MockReader struct {
	rows       []Row
	currentIdx int
	readError  error
	closed     bool
}

func NewMockReader(rows []Row) *MockReader {
	return &MockReader{rows: rows}
}

func (m *MockReader) Read(rows []Row) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if m.readError != nil {
		return 0, m.readError
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) && m.currentIdx < len(m.rows) {
		resetRow(&rows[n])
		for k, v := range m.rows[m.currentIdx] {
			rows[n][k] = v
		}
		n++
		m.currentIdx++
	}

	if m.currentIdx >= len(m.rows) && n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (m *MockReader) Close() error {
	m.closed = true
	return nil
}

func (m *MockReader) RowCount() int64 {
	return int64(m.currentIdx)
}
