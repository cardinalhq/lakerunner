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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TestParquetRawReaderNext tests the actual Next() method behavior
func TestParquetRawReaderNext(t *testing.T) {
	// Test with a real file to verify Next() behavior
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetRawReader(file, stat.Size(), 10) // Small batch size
	require.NoError(t, err)
	defer reader.Close()

	var totalRows int64
	batchCount := 0

	// Test actual Next() behavior
	for {
		batch, err := reader.Next(context.TODO())
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

// TestParquetRawReaderBatching tests batching behavior with real file
func TestParquetRawReaderBatching(t *testing.T) {
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
			reader, err := NewParquetRawReader(file, stat.Size(), tc.batchSize)
			require.NoError(t, err)
			defer reader.Close()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := reader.Next(context.TODO())
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

// TestParquetRawReaderWithRealFile tests ParquetRawReader with actual parquet files
func TestParquetRawReaderWithRealFile(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows and verify we get the expected count
	var rowCount int64

	for {
		batch, err := reader.Next(context.TODO())
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

// TestParquetRawReaderMultipleFiles tests ParquetRawReader with different files
func TestParquetRawReaderMultipleFiles(t *testing.T) {
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

			reader, err := NewParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err)
			defer reader.Close()

			// Read all rows and count them
			var totalRows int64

			for {
				batch, err := reader.Next(context.TODO())
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

// TestParquetRawReaderClose tests proper cleanup
func TestParquetRawReaderClose(t *testing.T) {
	file, err := os.Open("../../testdata/logs/logs-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next(context.TODO())
	require.NotNil(t, batch)
	require.Greater(t, batch.Len(), 0)
	// EOF is acceptable if we got data (it means we read all the data in one batch)
	require.True(t, err == nil || errors.Is(err, io.EOF), "Expected no error or EOF, got: %v", err)
	require.NotNil(t, batch.Get(0))

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = reader.Next(context.TODO())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestParquetRawReader_SpecificProblemFile(t *testing.T) {
	// Test the specific file that's failing in production
	filename := "../../testdata/logs/logs_1747427310000_667024137.parquet"

	reader, err := createParquetReader(filename, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to create reader for problem file: %v", err)
	}
	defer reader.Close()

	// Try to read some rows
	batch, err := reader.Next(context.TODO())
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

func TestParquetRawReader_WithTranslator(t *testing.T) {
	// Test ParquetRawReader with TranslatingReader using the problem file
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
	batch, err := reader.Next(context.TODO())
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with ParquetRawReader: n=%d, err=%v", n, err)

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

// TestIngestProtoLogsReader_WithTranslator tests TranslatingReader with IngestProtoLogsReader for comparison
func TestIngestProtoLogsReader_WithTranslator(t *testing.T) {
	filename := createSyntheticLogsFile(t, true)

	// Create base proto reader
	options := ReaderOptions{SignalType: SignalTypeLogs}
	baseReader, err := createProtoBinaryGzReader(filename, options)
	require.NoError(t, err)
	defer baseReader.Close()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "proto"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator, 1000)
	require.NoError(t, err)

	// Read rows
	batch, err := reader.Next(context.TODO())
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with IngestProtoLogsReader: n=%d, err=%v", n, err)

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

func TestParquetRawReader_CompactTestFiles(t *testing.T) {
	// Test files from compact-test-0001 with expected record counts
	// ParquetRawReader returns all rows without filtering
	expectedCounts := map[string]int64{
		"tbl_299476429685392687.parquet": 227,
		"tbl_299476441865651503.parquet": 227,
		"tbl_299476446630380847.parquet": 227,
		"tbl_299476458558980900.parquet": 231,
		"tbl_299476464716219172.parquet": 227,
		"tbl_299476475503969060.parquet": 227,
		"tbl_299476481342440751.parquet": 227,
		"tbl_299476495972173103.parquet": 231,
		"tbl_299476496878142244.parquet": 227,
		"tbl_299476509242950436.parquet": 227,
		"tbl_299476513621803812.parquet": 227,
		"tbl_299476526607368996.parquet": 227,
	}

	for filename, expectedCount := range expectedCounts {
		t.Run(filename, func(t *testing.T) {
			fullPath := fmt.Sprintf("../../testdata/metrics/compact-test-0001/%s", filename)

			file, err := os.Open(fullPath)
			require.NoError(t, err, "Failed to open file: %s", fullPath)
			defer file.Close()

			stat, err := file.Stat()
			require.NoError(t, err)

			reader, err := NewParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err, "Failed to create reader for file: %s", filename)
			defer reader.Close()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := reader.Next(context.TODO())
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())

					// Verify batch structure for first batch only to avoid spam
					if batchCount == 1 {
						assert.Greater(t, batch.Len(), 0, "First batch should have rows")
						for i := 0; i < batch.Len() && i < 3; i++ {
							row := batch.Get(i)
							assert.NotNil(t, row, "Row %d should not be nil", i)
							assert.Greater(t, len(row), 0, "Row %d should have fields", i)
						}
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Reader error in file %s", filename)
			}

			assert.Equal(t, expectedCount, totalRows, "File %s should have exactly %d records", filename, expectedCount)
			assert.Equal(t, totalRows, reader.TotalRowsReturned(), "TotalRowsReturned should match for file %s", filename)

			t.Logf("File %s: successfully read %d records in %d batches", filename, totalRows, batchCount)
		})
	}
}

func TestParquetRawReader_TIDConversion(t *testing.T) {
	// Test TID field type in actual compact test file data
	fullPath := "../../testdata/metrics/compact-test-0001/tbl_299476429685392687.parquet"

	file, err := os.Open(fullPath)
	require.NoError(t, err, "Failed to open test file")
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err, "Failed to create reader")
	defer reader.Close()

	// Read first batch
	batch, err := reader.Next(context.TODO())
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}

	if batch == nil || batch.Len() == 0 {
		t.Fatal("No rows returned from ParquetReader")
	}

	// Examine first row TID field type
	row := batch.Get(0)
	require.NotNil(t, row)

	// Check what type the TID field actually is
	tidValue, hasTID := row[wkk.RowKeyCTID]
	t.Logf("Row has TID field: %v", hasTID)

	if hasTID {
		t.Logf("TID value: %v (type: %T)", tidValue, tidValue)

		// Test if it's int64 as expected
		if tidInt64, isInt64 := tidValue.(int64); isInt64 {
			t.Logf("TID is int64: %d", tidInt64)
		} else if tidStr, isString := tidValue.(string); isString {
			t.Logf("TID is still string: %s", tidStr)
			// Try manual conversion
			converted, err := strconv.ParseInt(tidStr, 10, 64)
			t.Logf("Manual conversion result: %d (err: %v)", converted, err)
		} else {
			t.Logf("TID is unexpected type: %T", tidValue)
		}
	} else {
		t.Log("No TID field found")
	}

	// Also check timestamp
	timestampValue, hasTimestamp := row[wkk.RowKeyCTimestamp]
	t.Logf("Row has timestamp field: %v", hasTimestamp)
	if hasTimestamp {
		t.Logf("Timestamp value: %v (type: %T)", timestampValue, timestampValue)
	}
}

func TestDiskSortingReader_WithParquetCompactTestFiles(t *testing.T) {
	// Test DiskSortingReader(CookedMetricTranslatingReader(ParquetReader)) combination
	// CookedMetricTranslatingReader filters out rows with NaN values
	expectedCounts := map[string]int64{
		"tbl_299476429685392687.parquet": 227,
		"tbl_299476441865651503.parquet": 226, // 1 NaN row filtered
		"tbl_299476446630380847.parquet": 227,
		"tbl_299476458558980900.parquet": 231,
		"tbl_299476464716219172.parquet": 226, // 1 NaN row filtered
		"tbl_299476475503969060.parquet": 227,
		"tbl_299476481342440751.parquet": 227,
		"tbl_299476495972173103.parquet": 231,
		"tbl_299476496878142244.parquet": 226, // 1 NaN row filtered
		"tbl_299476509242950436.parquet": 227,
		"tbl_299476513621803812.parquet": 227,
		"tbl_299476526607368996.parquet": 227,
	}

	// Use the standard metric sorting provider
	keyProvider := &MetricSortKeyProvider{}

	for filename, expectedCount := range expectedCounts {
		t.Run(filename, func(t *testing.T) {
			fullPath := fmt.Sprintf("../../testdata/metrics/compact-test-0001/%s", filename)

			// Open fresh file for the DiskSortingReader test
			file, err := os.Open(fullPath)
			require.NoError(t, err, "Failed to open file: %s", fullPath)
			defer file.Close()

			stat, err := file.Stat()
			require.NoError(t, err)

			// Create the ParquetReader
			parquetReader, err := NewParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err, "Failed to create ParquetReader for file: %s", filename)
			defer parquetReader.Close()

			// Wrap with CookedMetricTranslatingReader to handle metric-specific transformations
			translatingReader := NewCookedMetricTranslatingReader(parquetReader)
			defer translatingReader.Close()

			// Wrap with DiskSortingReader
			diskSortingReader, err := NewDiskSortingReader(translatingReader, keyProvider, 1000)
			require.NoError(t, err, "Failed to create DiskSortingReader for file: %s", filename)
			defer diskSortingReader.Close()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := diskSortingReader.Next(context.TODO())
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())

					// Verify batch structure for first batch only
					if batchCount == 1 {
						assert.Greater(t, batch.Len(), 0, "First batch should have rows")
						for i := 0; i < batch.Len() && i < 3; i++ {
							row := batch.Get(i)
							assert.NotNil(t, row, "Row %d should not be nil", i)
							assert.Greater(t, len(row), 0, "Row %d should have fields", i)
						}
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "DiskSortingReader error in file %s", filename)
			}

			assert.Equal(t, expectedCount, totalRows, "DiskSortingReader should preserve all %d records from %s", expectedCount, filename)
			assert.Equal(t, totalRows, diskSortingReader.TotalRowsReturned(), "DiskSortingReader TotalRowsReturned should match for file %s", filename)

			t.Logf("File %s: DiskSortingReader successfully read %d records in %d batches", filename, totalRows, batchCount)
		})
	}
}

// TestParquetRawReaderSmallFiles tests the NewParquetRawReader with small files from seglog-990
func TestParquetRawReaderSmallFiles(t *testing.T) {
	// Test the small files directly with NewParquetRawReader
	testFiles := []struct {
		filename string
		expectedRecords int
	}{
		{"tbl_301228791710090615.parquet", 480},
		{"tbl_301228792783832948.parquet", 456},
		{"tbl_301228792733501300.parquet", 1414}, // A larger file for comparison
	}
	
	ctx := context.Background()
	
	for _, testFile := range testFiles {
		t.Run(testFile.filename, func(t *testing.T) {
			filePath := fmt.Sprintf("../../testdata/metrics/seglog-990/source/%s", testFile.filename)
			
			file, err := os.Open(filePath)
			require.NoError(t, err, "Should open parquet file")
			defer file.Close()
			
			stat, err := file.Stat()
			require.NoError(t, err, "Should stat parquet file")
			
			reader, err := NewParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err, "Should create NewParquetRawReader")
			defer reader.Close()
			
			recordCount := 0
			for {
				batch, err := reader.Next(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						t.Logf("File %s: Got EOF after reading %d records", testFile.filename, recordCount)
						break
					}
					require.NoError(t, err, "Next should not fail")
				}
				
				if batch == nil {
					t.Logf("File %s: Got nil batch after reading %d records", testFile.filename, recordCount)
					break
				}
				
				batchSize := batch.Len()
				recordCount += batchSize
				t.Logf("File %s: Read batch of %d records (total: %d)", testFile.filename, batchSize, recordCount)
				
				if batchSize == 0 {
					t.Logf("File %s: Got empty batch, stopping", testFile.filename)
					break
				}
			}
			
			t.Logf("File %s: Final count %d, expected %d", testFile.filename, recordCount, testFile.expectedRecords)
			require.Equal(t, testFile.expectedRecords, recordCount, "Should read correct number of records from %s", testFile.filename)
		})
	}
}
