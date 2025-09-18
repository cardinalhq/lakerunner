//go:build integration
// +build integration

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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAggregatingMetricsReader_Seglog990DataLoss tests aggregation with actual seglog-990 data
// to reproduce the 51.8% data loss issue. This test uses multiple files merged together
// (like production) to trigger the aggregation issue.
func TestAggregatingMetricsReader_Seglog990DataLoss(t *testing.T) {
	// Test aggregation with actual seglog-990 data to reproduce the 51.8% data loss
	// This test uses multiple files merged together (like production) to trigger the aggregation issue
	ctx := context.Background()
	seglogDir := "../../testdata/metrics/seglog-990/"

	// Read ALL parquet files and merge them
	sourceDir := filepath.Join(seglogDir, "source")
	files, err := os.ReadDir(sourceDir)
	require.NoError(t, err, "Failed to read source directory")
	require.Greater(t, len(files), 0, "No source files found")

	// Create readers for ALL files to fully replicate production scenario
	var readers []Reader
	var totalInputCount int64

	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), ".parquet") {
			continue // Skip non-parquet files
		}

		testFile := filepath.Join(sourceDir, fileInfo.Name())
		file, err := os.Open(testFile)
		require.NoError(t, err, "Failed to open test file %s", fileInfo.Name())
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		require.NoError(t, err, "Failed to stat test file")

		// Create parquet reader
		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		// Create translating reader
		cookingReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		// Count input records for this file
		fileInputCount := int64(0)
		for {
			batch, err := cookingReader.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Error reading input records from %s", fileInfo.Name())
			}
			fileInputCount += int64(batch.Len())
		}

		t.Logf("Input file %s has %d records", fileInfo.Name(), fileInputCount)
		totalInputCount += fileInputCount

		// Reset the reader for aggregation test
		_ = file.Close()
		file, err = os.Open(testFile)
		require.NoError(t, err, "Failed to reopen test file")
		defer func() { _ = file.Close() }()

		rawReader, err = NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		cookingReader = NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		readers = append(readers, cookingReader)
	}

	t.Logf("Total input from %d files: %d records", len(readers), totalInputCount)
	require.Greater(t, totalInputCount, int64(0), "Test files should have records")

	// Create mergesort reader to combine all files (like production)
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergedReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Failed to create mergesort reader")
	defer func() { _ = mergedReader.Close() }()

	// Create aggregating reader on the merged stream (same as production: 10000ms = 10s aggregation)
	aggregatingReader, err := NewAggregatingMetricsReader(mergedReader, 10000, 1000)
	require.NoError(t, err, "Failed to create aggregating reader")
	defer func() { _ = aggregatingReader.Close() }()

	// Count output records
	outputCount := int64(0)
	for {
		batch, err := aggregatingReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "Error reading aggregated records")
		}
		outputCount += int64(batch.Len())
	}

	t.Logf("Output from aggregating reader: %d records", outputCount)

	// Calculate data loss
	dataLoss := totalInputCount - outputCount
	dataLossPercent := float64(dataLoss) / float64(totalInputCount) * 100

	t.Logf("Data loss: %d records (%.1f%%)", dataLoss, dataLossPercent)
	t.Logf("This replicates the production scenario with multiple merged files")

	// The key assertion: we should get SOME output records
	require.Greater(t, outputCount, int64(0), "Aggregating reader should produce some records")

	// Document the data loss for investigation
	if dataLossPercent > 10.0 {
		t.Logf("⚠️  Data loss detected: %.1f%% - this replicates the production issue", dataLossPercent)
		t.Logf("   Input: %d, Output: %d", totalInputCount, outputCount)
		t.Logf("   The issue occurs when processing merged files with overlapping metrics")
	} else {
		t.Logf("✅ Low data loss: %.1f%% - aggregation working as expected", dataLossPercent)
	}
}

// TestAggregatingMetricsReader_ProductionDoubleResourceBug tests the production bug where
// CreateReaderStack merges readers AND keeps original readers, then createAggregatingReader
// merges the SAME original readers again, creating a resource conflict.
func TestAggregatingMetricsReader_ProductionDoubleResourceBug(t *testing.T) {
	// Test that replicates the EXACT production bug:
	// CreateReaderStack merges readers AND keeps original readers
	// createAggregatingReader then merges the SAME original readers again
	// This creates a resource conflict where both operations read from same readers
	ctx := context.Background()
	seglogDir := "../../testdata/metrics/seglog-990/"

	// Read ALL parquet files and create the readers (like CreateReaderStack)
	sourceDir := filepath.Join(seglogDir, "source")
	files, err := os.ReadDir(sourceDir)
	require.NoError(t, err, "Failed to read source directory")
	require.Greater(t, len(files), 0, "No source files found")

	// Create readers for ALL files
	var originalReaders []Reader
	var totalInputCount int64

	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), ".parquet") {
			continue // Skip non-parquet files
		}

		testFile := filepath.Join(sourceDir, fileInfo.Name())
		file, err := os.Open(testFile)
		require.NoError(t, err, "Failed to open test file %s", fileInfo.Name())
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		require.NoError(t, err, "Failed to stat test file")

		// Create parquet reader
		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		// Create translating reader
		cookingReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		// Count input records for this file
		fileInputCount := int64(0)
		for {
			batch, err := cookingReader.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Error reading input records from %s", fileInfo.Name())
			}
			fileInputCount += int64(batch.Len())
		}

		t.Logf("Input file %s has %d records", fileInfo.Name(), fileInputCount)
		totalInputCount += fileInputCount

		// Reset the reader for the test
		_ = file.Close()
		file, err = os.Open(testFile)
		require.NoError(t, err, "Failed to reopen test file")
		defer func() { _ = file.Close() }()

		rawReader, err = NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		cookingReader = NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		originalReaders = append(originalReaders, cookingReader)
	}

	t.Logf("Total input from %d files: %d records", len(originalReaders), totalInputCount)
	require.Equal(t, int64(41816), totalInputCount, "Should have exactly 41,816 records from seglog-990")

	// *** THIS IS THE PRODUCTION BUG ***
	// Step 1: CreateReaderStack creates a merge (but result is ignored by createAggregatingReader)
	keyProvider := GetCurrentMetricSortKeyProvider()
	createReaderStackMerge, err := NewMergesortReader(ctx, originalReaders, keyProvider, 1000)
	require.NoError(t, err, "Failed to create CreateReaderStack merge reader")
	defer func() { _ = createReaderStackMerge.Close() }()

	// In production, this merged reader is created but then createAggregatingReader ignores it
	// and works directly on the original readers, causing the resource conflict

	// Step 2: createAggregatingReader tries to merge the SAME original readers again
	// This is where the bug happens - reading from readers that are already being consumed
	createAggregatingMerge, err := NewMergesortReader(ctx, originalReaders, &MetricSortKeyProvider{}, 1000)
	require.NoError(t, err, "Failed to create createAggregatingReader merge reader")
	defer func() { _ = createAggregatingMerge.Close() }()

	// Step 3: Aggregation (works fine if it gets data)
	aggregatingReader, err := NewAggregatingMetricsReader(createAggregatingMerge, 10000, 1000)
	require.NoError(t, err, "Failed to create aggregating reader")
	defer func() { _ = aggregatingReader.Close() }()

	// Count output records
	outputCount := int64(0)
	for {
		batch, err := aggregatingReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "Error reading aggregated records")
		}
		outputCount += int64(batch.Len())
	}

	t.Logf("Output from aggregating reader: %d records", outputCount)

	// Calculate data loss
	dataLoss := totalInputCount - outputCount
	dataLossPercent := float64(dataLoss) / float64(totalInputCount) * 100

	t.Logf("Data loss: %d records (%.1f%%)", dataLoss, dataLossPercent)

	// THIS IS THE BUG: we lose a significant portion of data due to double resource use
	if dataLossPercent > 50.0 {
		t.Logf("⚠️  SEVERE DATA LOSS: %.1f%% - this is the production bug!", dataLossPercent)
		t.Logf("   The bug: CreateReaderStack and createAggregatingReader both try to read from the same readers")
		t.Logf("   This causes resource conflicts and massive data loss")
		t.Logf("   Input: %d, Expected: %d, Got: %d", totalInputCount, totalInputCount, outputCount)
	}

	// The key assertion: we get SOME records but lose most of them
	require.Greater(t, outputCount, int64(0), "Should get at least some records despite the bug")
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
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		require.NoError(t, err, "Should stat parquet file %s", filename)

		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Should create NewParquetRawReader for %s", filename)
		defer func() { _ = rawReader.Close() }()

		cookedReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookedReader.Close() }()

		readers = append(readers, cookedReader)
	}

	t.Logf("Created %d readers for mergesort", len(readers))

	// Create MergesortReader with all actual parquet readers - just like production
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer func() { _ = mergesortReader.Close() }()

	// Count total records
	totalRecords := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "Should successfully read batch")
		}

		totalRecords += batch.Len()
	}

	t.Logf("Total records processed: %d", totalRecords)
	t.Logf("Expected records: %d", expectedTotalRecords)

	// Data loss calculation
	dataLoss := expectedTotalRecords - totalRecords
	dataLossPercent := 0.0
	if expectedTotalRecords > 0 {
		dataLossPercent = float64(dataLoss) / float64(expectedTotalRecords) * 100
	}

	t.Logf("Data loss: %d records (%.1f%%)", dataLoss, dataLossPercent)

	// Verify we process ALL records from mergesort (no data loss at this level)
	require.Equal(t, expectedTotalRecords, totalRecords,
		"MergesortReader should output ALL records from ALL input files (expected %d, got %d)",
		expectedTotalRecords, totalRecords)
}
