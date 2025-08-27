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

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TestPreorderedParquetRawReaderNext tests the actual Next() method behavior
func TestPreorderedParquetRawReaderNext(t *testing.T) {
	// Test with a real file to verify Next() behavior
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewPreorderedParquetRawReader(file, stat.Size(), 10) // Small batch size
	require.NoError(t, err)
	defer reader.Close()

	var totalRows int64
	batchCount := 0

	// Test actual Next() behavior
	for {
		batch, err := reader.Next()
		if batch != nil {
			batchCount++
			totalRows += int64(batch.Len())

			// Verify batch structure
			assert.LessOrEqual(t, batch.Len(), 10, "Batch should respect batch size limit")
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				assert.NotNil(t, row, "Row %d in batch %d should not be nil", i, batchCount)
				assert.Greater(t, len(row), 0, "Row %d in batch %d should have fields", i, batchCount)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "Next() should not return other errors")
	}

	// Verify we read the expected number of rows in multiple batches
	assert.Equal(t, int64(32), totalRows, "Should read all 32 rows from the file")
	assert.GreaterOrEqual(t, batchCount, 4, "Should read in at least 4 batches with batch size 10")
	assert.Equal(t, totalRows, reader.TotalRowsReturned(), "TotalRowsReturned should match actual rows read")
}

// TestPreorderedParquetRawReaderBatching tests batching behavior with real file
func TestPreorderedParquetRawReaderBatching(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	// Test different batch sizes
	testCases := []struct {
		batchSize          int
		expectedMinBatches int
		expectedMaxBatches int
	}{
		{batchSize: 5, expectedMinBatches: 7, expectedMaxBatches: 7},  // 32/5 = 6.4 -> 7 batches
		{batchSize: 10, expectedMinBatches: 4, expectedMaxBatches: 4}, // 32/10 = 3.2 -> 4 batches
		{batchSize: 50, expectedMinBatches: 1, expectedMaxBatches: 1}, // 32/50 = 0.64 -> 1 batch
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("BatchSize%d", tc.batchSize), func(t *testing.T) {
			reader, err := NewPreorderedParquetRawReader(file, stat.Size(), tc.batchSize)
			require.NoError(t, err)
			defer reader.Close()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := reader.Next()
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())
					assert.LessOrEqual(t, batch.Len(), tc.batchSize, "Batch should not exceed batch size")
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
			}

			assert.Equal(t, int64(32), totalRows, "Should read all 32 rows")
			assert.GreaterOrEqual(t, batchCount, tc.expectedMinBatches, "Should have at least %d batches", tc.expectedMinBatches)
			assert.LessOrEqual(t, batchCount, tc.expectedMaxBatches, "Should have at most %d batches", tc.expectedMaxBatches)
		})
	}
}

// TestPreorderedParquetRawReaderWithRealFile tests PreorderedParquetRawReader with actual parquet files
func TestPreorderedParquetRawReaderWithRealFile(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewPreorderedParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows and verify we get the expected count
	var rowCount int64

	for {
		batch, err := reader.Next()
		if batch != nil {
			rowCount += int64(batch.Len())

			// Verify each row that was read
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				require.NotNil(t, row, "Row %d should not be nil", i)
				require.Greater(t, len(row), 0, "Row %d should have columns", i)
				// Verify the expected collector_id field exists
				_, hasCollectorId := row[wkk.RowKeyCCollectorID]
				assert.True(t, hasCollectorId, "Row should have collector_id field")
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	// logs-cooked-0001.parquet should have 32 rows
	assert.Equal(t, int64(32), rowCount, "Should read exactly 32 rows from logs-cooked-0001.parquet")
}

// TestPreorderedParquetRawReaderMultipleFiles tests PreorderedParquetRawReader with different files
func TestPreorderedParquetRawReaderMultipleFiles(t *testing.T) {
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

			reader, err := NewPreorderedParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err)
			defer reader.Close()

			// Read all rows and count them
			var totalRows int64

			for {
				batch, err := reader.Next()
				if batch != nil {
					totalRows += int64(batch.Len())

					// Verify each row that was read
					for i := 0; i < batch.Len(); i++ {
						row := batch.Get(i)
						require.NotNil(t, row)
						require.Greater(t, len(row), 0, "Row should have columns")
						// Verify the expected collector_id field exists
						_, hasCollectorId := row[wkk.RowKeyCCollectorID]
						assert.True(t, hasCollectorId, "Row should have collector_id field")
					}
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

// TestPreorderedParquetRawReaderClose tests proper cleanup
func TestPreorderedParquetRawReaderClose(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewPreorderedParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next()
	require.NotNil(t, batch)
	require.Greater(t, batch.Len(), 0)
	// EOF is acceptable if we got data (it means we read all the data in one batch)
	require.True(t, err == nil || errors.Is(err, io.EOF), "Expected no error or EOF, got: %v", err)
	require.NotNil(t, batch.Get(0))

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = reader.Next()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestPreorderedParquetRawReader_SpecificProblemFile(t *testing.T) {
	// Test the specific file that's failing in production
	filename := "../../testdata/logs/logs_1747427310000_667024137.parquet"

	reader, err := createParquetReader(filename, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to create reader for problem file: %v", err)
	}
	defer reader.Close()

	// Try to read some rows
	batch, err := reader.Next()
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("Read result: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	if n == 0 {
		t.Fatalf("Expected to read some rows, got 0")
	}

	t.Logf("Successfully read %d rows from problem file", n)
	for i := 0; i < n && i < 3; i++ {
		t.Logf("Row %d has %d fields", i, len(batch.Get(i)))
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
	(*row)[wkk.NewRowKey(t.addField)] = t.addValue
	return nil
}

func TestPreorderedParquetRawReader_WithTranslator(t *testing.T) {
	// Test PreorderedParquetRawReader with TranslatingReader using the problem file
	filename := "../../testdata/logs/logs_1747427310000_667024137.parquet"

	// Create base parquet reader
	baseReader, err := createParquetReader(filename, ReaderOptions{})
	require.NoError(t, err)
	defer baseReader.Close()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "parquet"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator, 1000)
	require.NoError(t, err)

	// Read rows
	batch, err := reader.Next()
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with PreorderedParquetRawReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		row := batch.Get(i)
		assert.NotNil(t, row)
		assert.Equal(t, "parquet", row[wkk.NewRowKey("test.translator")])
		t.Logf("Row %d: %d fields, translator field present", i, len(row))
	}
}

// TestProtoLogsReader_WithTranslator tests TranslatingReader with ProtoLogsReader for comparison
func TestProtoLogsReader_WithTranslator(t *testing.T) {
	filename := createSyntheticLogsFile(t, true)

	// Create base proto reader
	baseReader, err := createProtoBinaryGzReader(filename, ReaderOptions{SignalType: SignalTypeLogs})
	require.NoError(t, err)
	defer baseReader.Close()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "proto"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator, 1000)
	require.NoError(t, err)

	// Read rows
	batch, err := reader.Next()
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with ProtoLogsReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		row := batch.Get(i)
		assert.NotNil(t, row)
		assert.Equal(t, "proto", row[wkk.NewRowKey("test.translator")])
		t.Logf("Row %d: %d fields, translator field present", i, len(row))
	}
}
