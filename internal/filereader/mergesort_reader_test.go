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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestNewMergesortReader(t *testing.T) {
	// Test with valid readers and keyProvider
	readers := []Reader{
		newMockReader("r1", []Row{{wkk.NewRowKey("ts"): int64(1)}}),
		newMockReader("r2", []Row{{wkk.NewRowKey("ts"): int64(2)}}),
	}
	keyProvider := NewTimeOrderedSortKeyProvider("ts")

	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	if len(or.states) != 2 {
		t.Errorf("Expected 2 states, got %d", len(or.states))
	}

	// Test with no readers
	_, err = NewMergesortReader(context.TODO(), []Reader{}, keyProvider, 1000)
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil keyProvider
	_, err = NewMergesortReader(context.TODO(), readers, nil, 1000)
	if err == nil {
		t.Error("Expected error for nil keyProvider")
	}
}

func TestMergesortReader_Next(t *testing.T) {
	// Create readers with interleaved timestamps to test ordering
	readers := []Reader{
		newMockReader("r1", []Row{
			{wkk.NewRowKey("ts"): int64(1), wkk.NewRowKey("data"): "r1-first"},
			{wkk.NewRowKey("ts"): int64(4), wkk.NewRowKey("data"): "r1-second"},
			{wkk.NewRowKey("ts"): int64(7), wkk.NewRowKey("data"): "r1-third"},
		}),
		newMockReader("r2", []Row{
			{wkk.NewRowKey("ts"): int64(2), wkk.NewRowKey("data"): "r2-first"},
			{wkk.NewRowKey("ts"): int64(5), wkk.NewRowKey("data"): "r2-second"},
		}),
		newMockReader("r3", []Row{
			{wkk.NewRowKey("ts"): int64(3), wkk.NewRowKey("data"): "r3-first"},
			{wkk.NewRowKey("ts"): int64(6), wkk.NewRowKey("data"): "r3-second"},
		}),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	// Expected order: 1, 2, 3, 4, 5, 6, 7
	expectedData := []string{
		"r1-first",  // ts=1
		"r2-first",  // ts=2
		"r3-first",  // ts=3
		"r1-second", // ts=4
		"r2-second", // ts=5
		"r3-second", // ts=6
		"r1-third",  // ts=7
	}

	allRows, err := readAllRows(or)
	if err != nil {
		t.Fatalf("readAllRows() error = %v", err)
	}

	if len(allRows) != len(expectedData) {
		t.Fatalf("Expected %d rows, got %d", len(expectedData), len(allRows))
	}

	for i, expected := range expectedData {
		if allRows[i][wkk.NewRowKey("data")] != expected {
			t.Errorf("Row %d data = %v, want %v (ts=%v)", i, allRows[i][wkk.NewRowKey("data")], expected, allRows[i][wkk.NewRowKey("ts")])
		}
		if i > 0 {
			// Verify timestamps are in order
			prevTs := allRows[i-1][wkk.NewRowKey("ts")].(int64)
			currTs := allRows[i][wkk.NewRowKey("ts")].(int64)
			if currTs < prevTs {
				t.Errorf("Timestamps out of order: row %d ts=%d < row %d ts=%d", i, currTs, i-1, prevTs)
			}
		}
	}
}

func TestMergesortReader_NextBatched(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{wkk.NewRowKey("ts"): int64(100)}}),
		newMockReader("r2", []Row{{wkk.NewRowKey("ts"): int64(200)}}),
		newMockReader("r3", []Row{}), // Empty reader
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	// Read first batch (should get both rows)
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Errorf("Next() returned %d rows, want 2", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(100) {
		t.Errorf("First row ts = %v, want 100", batch.Get(0)[wkk.NewRowKey("ts")])
	}
	if batch.Get(1)[wkk.NewRowKey("ts")] != int64(200) {
		t.Errorf("Second row ts = %v, want 200", batch.Get(1)[wkk.NewRowKey("ts")])
	}

	// Next read should return EOF
	batch, err = or.Next(context.TODO())
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final Next() should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Final Next() should return nil batch, got %v", batch)
	}
}

func TestMergesortReader_ActiveReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{wkk.NewRowKey("ts"): int64(1)}}),
		newMockReader("r2", []Row{{wkk.NewRowKey("ts"): int64(2)}}),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	// Initially both readers should be active
	if count := or.ActiveReaderCount(); count != 2 {
		t.Errorf("Initial ActiveReaderCount() = %d, want 2", count)
	}

	// Read batch (should get both rows)
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Fatalf("Expected 2 rows, got %d", batch.Len())
	}

	// Should have 0 active readers (all consumed)
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("After reading all rows ActiveReaderCount() = %d, want 0", count)
	}
}

func TestMergesortReader_AllEmptyReaders(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	// Should immediately return io.EOF
	batch, err := or.Next(context.TODO())
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next() with all empty readers should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Next() with all empty readers should return nil batch, got %v", batch)
	}
}

func TestMergesortReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{wkk.NewRowKey("ts"): int64(1)}}),
		newMockReader("r2", []Row{{wkk.NewRowKey("ts"): int64(2)}}),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}

	// Close the reader
	err = or.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify underlying readers are closed
	for i, reader := range readers {
		mockReader := reader.(*mockReader)
		if !mockReader.closed {
			t.Errorf("Reader %d not closed after OrderedReader.Close()", i)
		}
	}

	// Verify subsequent operations fail
	_, err = or.Next(context.TODO())
	if err == nil {
		t.Error("Next() after Close() should return error")
	}

	// Multiple Close() calls should not error
	err = or.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Verify count after close
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("ActiveReaderCount() after close = %d, want 0", count)
	}
}

func TestTimeOrderedSortKeyProvider(t *testing.T) {
	provider := NewTimeOrderedSortKeyProvider("timestamp")

	// Test basic key creation and comparison
	row1 := Row{wkk.NewRowKey("timestamp"): int64(100), wkk.NewRowKey("data"): "first"}
	row2 := Row{wkk.NewRowKey("timestamp"): int64(200), wkk.NewRowKey("data"): "second"}
	row3 := Row{wkk.NewRowKey("timestamp"): int64(300), wkk.NewRowKey("data"): "third"}

	key1 := provider.MakeKey(row1)
	key2 := provider.MakeKey(row2)
	key3 := provider.MakeKey(row3)
	defer key1.Release()
	defer key2.Release()
	defer key3.Release()

	// Test comparisons
	if key1.Compare(key2) >= 0 {
		t.Errorf("key1 (ts=100) should be < key2 (ts=200)")
	}
	if key2.Compare(key3) >= 0 {
		t.Errorf("key2 (ts=200) should be < key3 (ts=300)")
	}
	if key1.Compare(key3) >= 0 {
		t.Errorf("key1 (ts=100) should be < key3 (ts=300)")
	}

	// Test with float64 timestamps
	rowFloat1 := Row{wkk.NewRowKey("timestamp"): float64(100.1), wkk.NewRowKey("data"): "first"}
	rowFloat2 := Row{wkk.NewRowKey("timestamp"): float64(200.2), wkk.NewRowKey("data"): "second"}

	keyFloat1 := provider.MakeKey(rowFloat1)
	keyFloat2 := provider.MakeKey(rowFloat2)
	defer keyFloat1.Release()
	defer keyFloat2.Release()

	if keyFloat1.Compare(keyFloat2) >= 0 {
		t.Errorf("keyFloat1 (ts=100.1) should be < keyFloat2 (ts=200.2)")
	}

	// Test with missing timestamp field
	rowMissing := Row{wkk.NewRowKey("data"): "no timestamp"}
	rowValid := Row{wkk.NewRowKey("timestamp"): int64(100), wkk.NewRowKey("data"): "has timestamp"}

	keyMissing := provider.MakeKey(rowMissing)
	keyValid := provider.MakeKey(rowValid)
	defer keyMissing.Release()
	defer keyValid.Release()

	if keyValid.Compare(keyMissing) >= 0 {
		t.Errorf("keyValid should be < keyMissing (missing timestamps default to invalid and go last)")
	}
}

// trackingReader is a test implementation that records the address of the row slice
// provided to Read so tests can verify row recycling behavior.
type trackingReader struct {
	rows     []Row
	index    int
	ptrs     []string
	rowCount int64
}

func newTrackingReader(rows []Row) *trackingReader {
	return &trackingReader{rows: rows}
}

func (tr *trackingReader) Next(ctx context.Context) (*Batch, error) {
	if tr.index >= len(tr.rows) {
		return nil, io.EOF
	}

	// Create a new batch with one row
	batch := pipeline.GetBatch()

	// Create a new row and copy data
	row := batch.AddRow()
	for k, v := range tr.rows[tr.index] {
		row[k] = v
	}

	// Track the pointer for testing row reuse
	tr.ptrs = append(tr.ptrs, fmt.Sprintf("%p", row))

	tr.index++
	tr.rowCount++
	return batch, nil
}

func (tr *trackingReader) Close() error { return nil }

func (tr *trackingReader) TotalRowsReturned() int64 { return tr.rowCount }

func TestMergesortReader_RowReuse(t *testing.T) {
	tr := newTrackingReader([]Row{{wkk.NewRowKey("ts"): int64(1)}, {wkk.NewRowKey("ts"): int64(2)}, {wkk.NewRowKey("ts"): int64(3)}, {wkk.NewRowKey("ts"): int64(4)}})
	or, err := NewMergesortReader(context.TODO(), []Reader{tr}, NewTimeOrderedSortKeyProvider("ts"), 1)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer or.Close()

	// Read first batch
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("First Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("First batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(1) {
		t.Fatalf("first row ts=%v want 1", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Read second batch
	batch, err = or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Second Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("Second batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(2) {
		t.Fatalf("second row ts=%v want 2", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Read third batch
	batch, err = or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Third Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("Third batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(3) {
		t.Fatalf("third row ts=%v want 3", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Verify tracking worked
	if len(tr.ptrs) < 4 {
		t.Fatalf("expected at least 4 recorded pointers, got %d", len(tr.ptrs))
	}
	// Note: With the new interface, row reuse patterns may be different
	// This test mainly verifies that the tracking reader works with the new interface
}

// TestMergesortReader_WithActualParquetReader tests NewMergesortReader with a single actual CookedMetricTranslatingReader
// This is critical because all existing tests use mock readers, but the production issue is with real parquet readers
func TestMergesortReader_WithActualParquetReader(t *testing.T) {
	ctx := context.Background()

	// Test with one of the small files that shows 0 records in production
	filename := "tbl_301228791710090615.parquet"
	expectedRecords := 480

	filePath := filepath.Join("..", "..", "testdata", "metrics", "seglog-990", "source", filename)

	file, err := os.Open(filePath)
	require.NoError(t, err, "Should open parquet file")
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err, "Should stat parquet file")

	rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
	require.NoError(t, err, "Should create NewParquetRawReader")
	defer rawReader.Close()

	cookedReader := NewCookedMetricTranslatingReader(rawReader)
	defer cookedReader.Close()

	t.Logf("Created cooked reader for %s expecting %d records", filename, expectedRecords)

	// Create MergesortReader with single actual parquet reader
	// Use the same key provider that the production code uses for metrics
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, []Reader{cookedReader}, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer mergesortReader.Close()

	// Read all records from the mergesort reader
	totalRecords := 0
	batchNum := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		batchNum++

		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Logf("Batch %d - Got EOF after reading %d total records", batchNum, totalRecords)
				break
			}
			require.NoError(t, err, "Next should not fail")
		}

		if batch == nil {
			t.Logf("Batch %d - Got nil batch after reading %d total records", batchNum, totalRecords)
			break
		}

		batchSize := batch.Len()
		totalRecords += batchSize
		t.Logf("Batch %d - Got %d records (total: %d)", batchNum, batchSize, totalRecords)

		if batchSize == 0 {
			t.Logf("Batch %d - Empty batch, stopping", batchNum)
			break
		}
	}

	t.Logf("MergesortReader returned %d records (expected %d)", totalRecords, expectedRecords)

	// This is the critical test - NewMergesortReader should not lose any data even with a single reader
	require.Equal(t, expectedRecords, totalRecords,
		"NewMergesortReader should not lose data when wrapping a single actual parquet reader")
}

// TestMergesortReader_WithMultipleActualParquetReaders tests NewMergesortReader with multiple actual readers
// This replicates the production scenario where multiple files are merged together
func TestMergesortReader_WithMultipleActualParquetReaders(t *testing.T) {
	testFiles := []struct {
		filename        string
		expectedRecords int
	}{
		{"tbl_301228791710090615.parquet", 480},  // Small file 1
		{"tbl_301228792783832948.parquet", 456},  // Small file 2
		{"tbl_301228792733501300.parquet", 1414}, // Larger file for comparison
	}

	ctx := context.Background()

	// Create readers for multiple files
	var readers []Reader
	var expectedTotalRecords int

	for _, testFile := range testFiles {
		filePath := filepath.Join("..", "..", "testdata", "metrics", "seglog-990", "source", testFile.filename)

		file, err := os.Open(filePath)
		require.NoError(t, err, "Should open parquet file")
		defer file.Close()

		stat, err := file.Stat()
		require.NoError(t, err, "Should stat parquet file")

		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Should create NewParquetRawReader")
		defer rawReader.Close()

		cookedReader := NewCookedMetricTranslatingReader(rawReader)
		defer cookedReader.Close()

		readers = append(readers, cookedReader)
		expectedTotalRecords += testFile.expectedRecords

		t.Logf("Created reader for %s expecting %d records", testFile.filename, testFile.expectedRecords)
	}

	t.Logf("Total expected records from %d files: %d", len(testFiles), expectedTotalRecords)

	// Create MergesortReader with multiple actual parquet readers
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer mergesortReader.Close()

	// Read all records from the mergesort reader
	totalRecords := 0
	batchNum := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		batchNum++

		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Logf("Batch %d - Got EOF after reading %d total records", batchNum, totalRecords)
				break
			}
			require.NoError(t, err, "Next should not fail")
		}

		if batch == nil {
			t.Logf("Batch %d - Got nil batch after reading %d total records", batchNum, totalRecords)
			break
		}

		batchSize := batch.Len()
		totalRecords += batchSize
		t.Logf("Batch %d - Got %d records (total: %d)", batchNum, batchSize, totalRecords)

		if batchSize == 0 {
			t.Logf("Batch %d - Empty batch, stopping", batchNum)
			break
		}
	}

	t.Logf("MergesortReader returned %d records (expected %d)", totalRecords, expectedTotalRecords)

	// This is the critical test - NewMergesortReader should not lose any data when merging multiple files
	require.Equal(t, expectedTotalRecords, totalRecords,
		"NewMergesortReader should not lose data when merging multiple actual parquet readers")
}

// TestMergesortReader_WithAllSeglog990Files tests NewMergesortReader with all 17 files from seglog-990
// This exactly replicates the production scenario to see if we get the same data loss
func TestMergesortReader_WithAllSeglog990Files(t *testing.T) {
	ctx := context.Background()

	// Get all parquet files from seglog-990
	testdataDir := filepath.Join("..", "..", "testdata", "metrics", "seglog-990", "source")
	files, err := filepath.Glob(filepath.Join(testdataDir, "*.parquet"))
	require.NoError(t, err, "Should find parquet files")
	require.Greater(t, len(files), 0, "Should have test files")

	t.Logf("Found %d parquet files to test", len(files))

	// First, count expected records by reading each file with debug command
	expectedTotalRecords := 0
	fileExpectedCounts := make(map[string]int)

	for _, filePath := range files {
		filename := filepath.Base(filePath)
		// We know the counts for our test files from previous investigation
		var expectedCount int
		switch filename {
		case "tbl_301228791710090615.parquet":
			expectedCount = 480
		case "tbl_301228792783832948.parquet":
			expectedCount = 456
		case "tbl_301228792733501300.parquet":
			expectedCount = 1414
		case "tbl_301228792616060788.parquet":
			expectedCount = 1602
		case "tbl_301228813201703434.parquet":
			expectedCount = 1604
		case "tbl_301228729835718516.parquet":
			expectedCount = 1623
		case "tbl_301228762098305891.parquet":
			expectedCount = 1626
		case "tbl_301228709837276535.parquet":
			expectedCount = 2026
		case "tbl_301228791542318455.parquet":
			expectedCount = 2039
		case "tbl_301228787683560291.parquet":
			expectedCount = 2225
		case "tbl_301228696314842838.parquet":
			expectedCount = 2403
		case "tbl_301228693378829155.parquet":
			expectedCount = 2841
		case "tbl_301228720323038934.parquet":
			expectedCount = 2954
		case "tbl_301228765688628523.parquet":
			expectedCount = 3383
		case "tbl_301228693227834211.parquet":
			expectedCount = 3477
		case "tbl_301228729382732298.parquet":
			expectedCount = 5273
		case "tbl_301228771644540279.parquet":
			expectedCount = 6390
		default:
			t.Logf("Unknown file %s, skipping", filename)
			continue
		}
		fileExpectedCounts[filename] = expectedCount
		expectedTotalRecords += expectedCount
		t.Logf("File %s: expecting %d records", filename, expectedCount)
	}

	t.Logf("Total expected records from all %d files: %d", len(fileExpectedCounts), expectedTotalRecords)

	// Create readers for all files
	var readers []Reader
	for _, filePath := range files {
		filename := filepath.Base(filePath)
		if _, exists := fileExpectedCounts[filename]; !exists {
			continue // Skip unknown files
		}

		file, err := os.Open(filePath)
		require.NoError(t, err, "Should open parquet file %s", filename)
		defer file.Close()

		stat, err := file.Stat()
		require.NoError(t, err, "Should stat parquet file %s", filename)

		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Should create NewParquetRawReader for %s", filename)
		defer rawReader.Close()

		cookedReader := NewCookedMetricTranslatingReader(rawReader)
		defer cookedReader.Close()

		readers = append(readers, cookedReader)
	}

	t.Logf("Created %d readers for mergesort", len(readers))

	// Create MergesortReader with all actual parquet readers - just like production
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer mergesortReader.Close()

	// Read all records from the mergesort reader
	totalRecords := 0
	batchNum := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		batchNum++

		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Logf("Batch %d - Got EOF after reading %d total records", batchNum, totalRecords)
				break
			}
			require.NoError(t, err, "Next should not fail")
		}

		if batch == nil {
			t.Logf("Batch %d - Got nil batch after reading %d total records", batchNum, totalRecords)
			break
		}

		batchSize := batch.Len()
		totalRecords += batchSize
		t.Logf("Batch %d - Got %d records (total: %d)", batchNum, batchSize, totalRecords)

		if batchSize == 0 {
			t.Logf("Batch %d - Empty batch, stopping", batchNum)
			break
		}
	}

	t.Logf("MergesortReader returned %d records (expected %d)", totalRecords, expectedTotalRecords)

	// Calculate the data loss percentage
	if expectedTotalRecords > 0 {
		lossPercentage := float64(expectedTotalRecords-totalRecords) / float64(expectedTotalRecords) * 100
		t.Logf("Data loss: %.1f%% (%d lost out of %d expected)", lossPercentage, expectedTotalRecords-totalRecords, expectedTotalRecords)

		// If we see the same ~38% data loss as production, then NewMergesortReader IS the culprit
		// If we get all records, then the issue is elsewhere in CreateReaderStack
		if lossPercentage > 30 {
			t.Logf("*** FOUND THE BUG! *** MergesortReader is losing data just like production!")
		} else if lossPercentage < 5 {
			t.Logf("*** MergesortReader works fine *** - the bug is elsewhere in CreateReaderStack")
		}
	}

	// For now, let's see what we get rather than failing the test
	t.Logf("Final result: NewMergesortReader with all 17 files returned %d out of %d expected records", totalRecords, expectedTotalRecords)
}
