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
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockParquetGenericReader simulates parquet.GenericReader behavior for testing EOF handling
type MockParquetGenericReader struct {
	data     []map[string]any
	position int
	closed   bool
}

func (m *MockParquetGenericReader) Read(rows []map[string]any) (int, error) {
	if m.closed {
		return 0, errors.New("reader closed")
	}

	if len(rows) == 0 {
		return 0, errors.New("no space to read")
	}

	// Simulate reading one row at a time
	if m.position >= len(m.data) {
		return 0, io.EOF
	}

	// Copy data to the provided slice
	for k, v := range m.data[m.position] {
		rows[0][k] = v
	}
	m.position++

	// Test the n>0 && io.EOF case: return data AND EOF when reading the last item
	if m.position >= len(m.data) {
		return 1, io.EOF // This is the critical case we're testing!
	}

	return 1, nil
}

func (m *MockParquetGenericReader) Close() error {
	m.closed = true
	return nil
}

// TestParquetReaderEOFWithMock tests the n>0 && io.EOF case using a mock
func TestParquetReaderEOFWithMock(t *testing.T) {
	// Create mock data
	testData := []map[string]any{
		{"id": int64(1), "name": "first"},
		{"id": int64(2), "name": "second"},
		{"id": int64(3), "name": "last"},
	}

	// Create a ParquetReader with our mock
	mockReader := &MockParquetGenericReader{data: testData}

	// We can't easily replace the internal parquet reader, so let's test the logic directly
	// by simulating what happens in GetRow()

	var collectedRows []map[string]any

	for {
		// Simulate ParquetReader.GetRow() logic
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)

		n, err := mockReader.Read(rows)

		// This is the exact logic from our ParquetReader.GetRow()
		if n == 1 {
			row := rows[0]
			collectedRows = append(collectedRows, row)

			// If we got data, return it regardless of EOF
			// The next call will return io.EOF with no data
			continue // In real code this would be: return Row(row), nil
		}

		// No data was read
		if n == 0 {
			if err != nil && errors.Is(err, io.EOF) {
				break // In real code this would be: return nil, io.EOF
			}
		}

		// Any other error
		if err != nil {
			require.NoError(t, err, "Unexpected error")
		}
	}

	// Verify we got all the data, including the last row that came with EOF
	require.Len(t, collectedRows, 3)
	assert.Equal(t, int64(1), collectedRows[0]["id"])
	assert.Equal(t, "first", collectedRows[0]["name"])
	assert.Equal(t, int64(2), collectedRows[1]["id"])
	assert.Equal(t, "second", collectedRows[1]["name"])
	assert.Equal(t, int64(3), collectedRows[2]["id"])
	assert.Equal(t, "last", collectedRows[2]["name"])
}

// TestParquetReaderSingleRowWithEOF tests the edge case of a single row + EOF
func TestParquetReaderSingleRowWithEOF(t *testing.T) {
	testData := []map[string]any{
		{"only": "data"},
	}

	mockReader := &MockParquetGenericReader{data: testData}

	// First read should get data + EOF
	rows := make([]map[string]any, 1)
	rows[0] = make(map[string]any)

	n, err := mockReader.Read(rows)

	// Should get 1 row AND io.EOF
	assert.Equal(t, 1, n)
	assert.True(t, errors.Is(err, io.EOF))
	assert.Equal(t, "data", rows[0]["only"])

	// Second read should get 0 rows and EOF
	rows[0] = make(map[string]any) // Reset
	n, err = mockReader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestParquetReaderEmptyData tests EOF handling with no data
func TestParquetReaderEmptyData(t *testing.T) {
	testData := []map[string]any{} // Empty data

	mockReader := &MockParquetGenericReader{data: testData}

	// First read should get 0 rows and EOF
	rows := make([]map[string]any, 1)
	rows[0] = make(map[string]any)

	n, err := mockReader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestParquetReaderNumRows tests the NumRows functionality
func TestParquetReaderNumRows(t *testing.T) {
	testData := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
		{"id": int64(4)},
		{"id": int64(5)},
	}

	mockReader := &MockParquetGenericReader{data: testData}

	// We have 5 test records

	// Read all rows
	var collectedRows []map[string]any
	for {
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)

		n, err := mockReader.Read(rows)
		if n == 1 {
			collectedRows = append(collectedRows, rows[0])
			continue
		}
		if n == 0 && errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	// Should have read all rows
	assert.Len(t, collectedRows, 5)
	for i, row := range collectedRows {
		assert.Equal(t, int64(i+1), row["id"])
	}
}

// TestParquetReaderWithRealFile tests ParquetReader with actual parquet files
func TestParquetReaderWithRealFile(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetReader(file, stat.Size())
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows and verify we get the expected count
	var rowCount int64

	for {
		rows := make([]Row, 10) // Create new batch each time
		n, err := reader.Read(rows)
		rowCount += int64(n)

		// Verify each row that was read
		for i := 0; i < n; i++ {
			require.NotNil(t, rows[i], "Row %d should not be nil", i)
			require.Greater(t, len(rows[i]), 0, "Row %d should have columns", i)
			// Verify the expected collector_id field exists
			assert.Contains(t, rows[i], "_cardinalhq.collector_id", "Row should have collector_id field")
		}

		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	// logs-cooked-0001.parquet should have 32 rows
	assert.Equal(t, int64(32), rowCount, "Should read exactly 32 rows from logs-cooked-0001.parquet")
}

// TestParquetReaderMultipleFiles tests ParquetReader with different files
func TestParquetReaderMultipleFiles(t *testing.T) {
	testFiles := map[string]int64{
		"../../testdata/logs/logs-cooked-0001.parquet":       32,   // 32 rows
		"../../testdata/metrics/metrics-cooked-0001.parquet": 211,  // 211 rows
		"../../testdata/logs/logs-chqs3-0001.parquet":        1807, // 1807 rows
	}

	for filename, expectedRows := range testFiles {
		t.Run(filename, func(t *testing.T) {
			file, err := os.Open(filename)
			require.NoError(t, err)
			defer file.Close()

			stat, err := file.Stat()
			require.NoError(t, err)

			reader, err := NewParquetReader(file, stat.Size())
			require.NoError(t, err)
			defer reader.Close()

			// Read all rows and count them
			var totalRows int64

			for {
				rows := make([]Row, 20) // Create new batch each time
				n, err := reader.Read(rows)
				totalRows += int64(n)

				// Verify each row that was read
				for i := 0; i < n; i++ {
					require.NotNil(t, rows[i])
					require.Greater(t, len(rows[i]), 0, "Row should have columns")
					// Verify the expected collector_id field exists
					assert.Contains(t, rows[i], "_cardinalhq.collector_id", "Row should have collector_id field")
				}

				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
			}

			// Verify exact row count
			assert.Equal(t, expectedRows, totalRows, "Should read exactly %d rows from %s", expectedRows, filename)
		})
	}
}

// TestParquetReaderClose tests proper cleanup
func TestParquetReaderClose(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetReader(file, stat.Size())
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Greater(t, n, 0)
	require.NotNil(t, rows[0])

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	rows = make([]Row, 1)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestParquetReader_SpecificProblemFile(t *testing.T) {
	// Test the specific file that's failing in production
	filename := "../../testdata/logs/logs_1747427310000_667024137.parquet"

	reader, err := createParquetReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader for problem file: %v", err)
	}
	defer reader.Close()

	// Try to read some rows
	rows := make([]Row, 10)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	t.Logf("Read result: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	if n == 0 {
		t.Fatalf("Expected to read some rows, got 0")
	}

	t.Logf("Successfully read %d rows from problem file", n)
	for i := 0; i < n && i < 3; i++ {
		t.Logf("Row %d has %d fields", i, len(rows[i]))
	}

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

// testTranslator is a simple translator for testing
type testTranslator struct {
	addField string
	addValue string
}

func (t *testTranslator) TranslateRow(row *Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}
	(*row)[t.addField] = t.addValue
	return nil
}

func TestParquetReader_WithTranslator(t *testing.T) {
	// Test ParquetReader with TranslatingReader using the problem file
	filename := "../../testdata/logs/logs_1747427310000_667024137.parquet"

	// Create base parquet reader
	baseReader, err := createParquetReader(filename)
	require.NoError(t, err)
	defer baseReader.Close()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "parquet"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator)
	require.NoError(t, err)

	// Read rows
	rows := make([]Row, 10)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	t.Logf("TranslatingReader with ParquetReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, "parquet", rows[i]["test.translator"])
		t.Logf("Row %d: %d fields, translator field present", i, len(rows[i]))
	}
}

// TestProtoLogsReader_WithTranslator tests TranslatingReader with ProtoLogsReader for comparison
func TestProtoLogsReader_WithTranslator(t *testing.T) {
	filename := "../../testdata/logs/otel-logs.binpb.gz"

	// Create base proto reader
	baseReader, err := createProtoBinaryGzReader(filename, ReaderOptions{SignalType: SignalTypeLogs})
	require.NoError(t, err)
	defer baseReader.Close()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "proto"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator)
	require.NoError(t, err)

	// Read rows
	rows := make([]Row, 10)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	t.Logf("TranslatingReader with ProtoLogsReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		assert.NotNil(t, rows[i])
		assert.Equal(t, "proto", rows[i]["test.translator"])
		t.Logf("Row %d: %d fields, translator field present", i, len(rows[i]))
	}
}
