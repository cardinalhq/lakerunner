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

package cmd

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
)

func TestProcessBatchGroups(t *testing.T) {
	tmpdir := t.TempDir()
	ll := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create test data - 3 small parquet files with different record counts
	testFiles := createTestParquetFiles(t, tmpdir, []int{100, 150, 200})

	// Create HourlyResult objects representing files that should be merged (same SplitKey)
	splitKey1 := logcrunch.SplitKey{
		DateInt:       20250822,
		Hour:          10,
		IngestDateint: 20250822,
		FileIndex:     0,
	}

	splitKey2 := logcrunch.SplitKey{
		DateInt:       20250822,
		Hour:          11, // Different hour, should be separate group
		IngestDateint: 20250822,
		FileIndex:     0,
	}

	// Group files: first two files go to splitKey1 (should be merged), third file goes to splitKey2
	allFileRecords := map[logcrunch.SplitKey][]logcrunch.HourlyResult{
		splitKey1: {
			{
				FileName:     testFiles[0],
				RecordCount:  100,
				FileSize:     1000,
				Fingerprints: mapset.NewSet[int64](1, 2, 3),
				FirstTS:      1000000,
				LastTS:       1001000,
			},
			{
				FileName:     testFiles[1],
				RecordCount:  150,
				FileSize:     1500,
				Fingerprints: mapset.NewSet[int64](4, 5, 6),
				FirstTS:      1000500, // Overlapping time range
				LastTS:       1002000,
			},
		},
		splitKey2: {
			{
				FileName:     testFiles[2],
				RecordCount:  200,
				FileSize:     2000,
				Fingerprints: mapset.NewSet[int64](7, 8, 9),
				FirstTS:      1003000,
				LastTS:       1004000,
			},
		},
	}

	// Process the batch groups
	finalResults, err := processBatchGroups(ll, allFileRecords, tmpdir, 1000)
	require.NoError(t, err)

	// Verify results
	assert.Len(t, finalResults, 2, "Should have 2 final results (one per SplitKey)")

	// Check splitKey1 (merged result)
	result1, exists := finalResults[splitKey1]
	require.True(t, exists, "Should have result for splitKey1")
	assert.Equal(t, int64(250), result1.RecordCount, "Should have merged record count (100+150)")
	assert.Equal(t, int64(1000000), result1.FirstTS, "Should have earliest timestamp")
	assert.Equal(t, int64(1002000), result1.LastTS, "Should have latest timestamp")

	// Check that fingerprints were merged
	expectedFingerprints := mapset.NewSet[int64](1, 2, 3, 4, 5, 6)
	assert.True(t, result1.Fingerprints.Equal(expectedFingerprints), "Should have merged fingerprints")

	// Verify the merged file actually contains the correct number of records
	verifyParquetRecordCount(t, result1.FileName, 250)

	// Check splitKey2 (single file, no merging)
	result2, exists := finalResults[splitKey2]
	require.True(t, exists, "Should have result for splitKey2")
	assert.Equal(t, int64(200), result2.RecordCount, "Should have original record count")
	assert.Equal(t, testFiles[2], result2.FileName, "Should use original filename for single file")

	// Verify the single file still contains the correct number of records
	verifyParquetRecordCount(t, result2.FileName, 200)
}

func TestMergeHourlyResults(t *testing.T) {
	tmpdir := t.TempDir()
	ll := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create test parquet files with known record counts
	testFiles := createTestParquetFiles(t, tmpdir, []int{75, 125, 300})

	// Create HourlyResult objects to merge
	results := []logcrunch.HourlyResult{
		{
			FileName:     testFiles[0],
			RecordCount:  75,
			FileSize:     750,
			Fingerprints: mapset.NewSet[int64](1, 2),
			FirstTS:      1000000,
			LastTS:       1001000,
		},
		{
			FileName:     testFiles[1],
			RecordCount:  125,
			FileSize:     1250,
			Fingerprints: mapset.NewSet[int64](3, 4, 5),
			FirstTS:      999500, // Earlier timestamp
			LastTS:       1000800,
		},
		{
			FileName:     testFiles[2],
			RecordCount:  300,
			FileSize:     3000,
			Fingerprints: mapset.NewSet[int64](4, 6, 7), // Overlapping fingerprint (4)
			FirstTS:      1000200,
			LastTS:       1002500, // Latest timestamp
		},
	}

	// Merge the results
	merged, err := mergeHourlyResults(ll, results, tmpdir, 1000)
	require.NoError(t, err)
	require.NotNil(t, merged)

	// Verify merged metadata
	assert.Equal(t, int64(500), merged.RecordCount, "Should have total record count (75+125+300)")
	assert.Equal(t, int64(999500), merged.FirstTS, "Should have earliest timestamp")
	assert.Equal(t, int64(1002500), merged.LastTS, "Should have latest timestamp")

	// Check fingerprints were merged correctly (union, so duplicate 4 should appear once)
	expectedFingerprints := mapset.NewSet[int64](1, 2, 3, 4, 5, 6, 7)
	assert.True(t, merged.Fingerprints.Equal(expectedFingerprints), "Should have union of all fingerprints")

	// Most importantly: verify the merged file actually contains all records
	verifyParquetRecordCount(t, merged.FileName, 500)
}

func TestBatchProcessingRecordLoss(t *testing.T) {
	// This test specifically addresses the bug where batch processing
	// was losing records from the second file (473 + 387 = 860, but only 473 output)
	tmpdir := t.TempDir()
	ll := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create test files matching the original bug report: 473 and 387 records
	testFiles := createTestParquetFiles(t, tmpdir, []int{473, 387})

	// Both files have the same SplitKey (same hour), so they should be merged
	splitKey := logcrunch.SplitKey{
		DateInt:       20250822,
		Hour:          22,
		IngestDateint: 20250822,
		FileIndex:     0,
	}

	allFileRecords := map[logcrunch.SplitKey][]logcrunch.HourlyResult{
		splitKey: {
			{
				FileName:     testFiles[0],
				RecordCount:  473,
				FileSize:     4730,
				Fingerprints: mapset.NewSet[int64](1, 2, 3),
				FirstTS:      1755900342142, // Based on original log timestamps
				LastTS:       1755900360000,
			},
			{
				FileName:     testFiles[1],
				RecordCount:  387,
				FileSize:     3870,
				Fingerprints: mapset.NewSet[int64](4, 5, 6),
				FirstTS:      1755900570000,
				LastTS:       1755900580000,
			},
		},
	}

	// Process the batch groups
	finalResults, err := processBatchGroups(ll, allFileRecords, tmpdir, 1000)
	require.NoError(t, err)

	// Should have exactly one result (merged)
	assert.Len(t, finalResults, 1, "Should have 1 final result")

	result, exists := finalResults[splitKey]
	require.True(t, exists, "Should have result for the splitKey")

	// Critical test: ensure NO records are lost during merging
	expectedTotalRecords := int64(473 + 387) // 860
	assert.Equal(t, expectedTotalRecords, result.RecordCount,
		"CRITICAL: All records must be preserved during batch merging (original bug was losing records)")

	// Verify the actual file contains all the records
	verifyParquetRecordCount(t, result.FileName, int(expectedTotalRecords))

	// Verify timestamp ranges were merged correctly
	assert.Equal(t, int64(1755900342142), result.FirstTS, "Should have earliest timestamp from both files")
	assert.Equal(t, int64(1755900580000), result.LastTS, "Should have latest timestamp from both files")

	t.Logf("SUCCESS: Batch merging preserved all %d records (%d + %d)",
		expectedTotalRecords, 473, 387)
}

func TestMergeHourlyResultsSingleFile(t *testing.T) {
	tmpdir := t.TempDir()
	ll := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create a single test file
	testFiles := createTestParquetFiles(t, tmpdir, []int{42})

	result := []logcrunch.HourlyResult{
		{
			FileName:     testFiles[0],
			RecordCount:  42,
			FileSize:     420,
			Fingerprints: mapset.NewSet[int64](1, 2, 3),
			FirstTS:      1000000,
			LastTS:       1001000,
		},
	}

	// Merge single result (should return as-is)
	merged, err := mergeHourlyResults(ll, result, tmpdir, 1000)
	require.NoError(t, err)
	require.NotNil(t, merged)

	// Should be identical to input
	assert.Equal(t, result[0].FileName, merged.FileName)
	assert.Equal(t, result[0].RecordCount, merged.RecordCount)
	assert.Equal(t, result[0].FileSize, merged.FileSize)
	assert.True(t, result[0].Fingerprints.Equal(merged.Fingerprints))
	assert.Equal(t, result[0].FirstTS, merged.FirstTS)
	assert.Equal(t, result[0].LastTS, merged.LastTS)
}

// createTestParquetFiles creates test parquet files with the specified record counts
func createTestParquetFiles(t *testing.T, tmpdir string, recordCounts []int) []string {
	t.Helper()

	var filenames []string
	for i, count := range recordCounts {
		// Create a simple schema for test data
		testData := make([]map[string]any, count)
		for j := 0; j < count; j++ {
			testData[j] = map[string]any{
				"_cardinalhq.timestamp": int64(1000000 + j),
				"message":               "test message",
				"level":                 "info",
				"file_index":            int64(i),
				"record_index":          int64(j),
			}
		}

		// Build schema from first record
		nmb := buffet.NewNodeMapBuilder()
		require.NoError(t, nmb.Add(testData[0]))

		// Create writer
		writer, err := buffet.NewWriter("test", tmpdir, nmb.Build(), 0)
		require.NoError(t, err)

		// Write all test data
		for _, record := range testData {
			require.NoError(t, writer.Write(record))
		}

		// Close and get results
		results, err := writer.Close()
		require.NoError(t, err)
		require.Len(t, results, 1, "Expected single output file")

		// Verify the file contains the expected number of records
		assert.Equal(t, int64(count), results[0].RecordCount, "File should contain expected record count")

		filenames = append(filenames, results[0].FileName)
	}

	return filenames
}

// verifyParquetRecordCount reads a parquet file and verifies it contains the expected number of records
func verifyParquetRecordCount(t *testing.T, filename string, expectedCount int) {
	t.Helper()

	fh, err := filecrunch.LoadSchemaForFile(filename)
	require.NoError(t, err)
	defer fh.Close()

	reader := parquet.NewReader(fh.File, fh.Schema)
	defer reader.Close()

	actualCount := 0
	for {
		rec := map[string]any{}
		if err := reader.Read(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		actualCount++
	}

	assert.Equal(t, expectedCount, actualCount, "File %s should contain %d records, but found %d", filename, expectedCount, actualCount)
}
