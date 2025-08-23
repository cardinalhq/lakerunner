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

package logcrunch

import (
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/buffet"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
)

// TestMultipleFilesPerHour verifies that when log volume exceeds rpfEstimate,
// multiple files are created per dateint-hour instead of losing data.
func TestMultipleFilesPerHour(t *testing.T) {
	tmpdir := t.TempDir()

	// Create test data that will exceed a small rpfEstimate
	records := make([]map[string]any, 150) // 150 records
	baseTime := time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC).UnixMilli()

	for i := 0; i < 150; i++ {
		records[i] = map[string]any{
			"_cardinalhq.timestamp": baseTime + int64(i*1000), // Same hour, different seconds
			"msg":                   "test message " + string(rune('0'+i%10)),
			"level":                 "info",
		}
	}

	// Create a test parquet file using the same pattern as existing tests
	inputFile, err := os.CreateTemp(tmpdir, "test-*.parquet")
	require.NoError(t, err)
	defer os.Remove(inputFile.Name())

	// Create schema for log records (match existing test pattern)
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"msg":                   parquet.String(),
		"level":                 parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

	pw := parquet.NewWriter(inputFile, schema)
	for _, record := range records {
		require.NoError(t, pw.Write(record))
	}
	require.NoError(t, pw.Close())
	require.NoError(t, inputFile.Close())

	// Load and process
	fh, err := filecrunch.LoadSchemaForFile(inputFile.Name())
	require.NoError(t, err)
	defer fh.Close()

	// Process with small rpfEstimate to force multiple files
	const smallRpfEstimate = 50 // Force 3 files: 50 + 50 + 50
	ingestDateint := int32(20230101)

	ll := slog.New(slog.NewTextHandler(io.Discard, nil))
	results, err := buffet.ProcessAndSplit(ll, fh, tmpdir, ingestDateint, smallRpfEstimate)
	require.NoError(t, err)

	// Should get multiple files for the same hour
	hourKeys := make([]buffet.SplitKey, 0)
	totalRecords := int64(0)

	for key, result := range results {
		assert.Equal(t, int32(20230101), key.DateInt, "All files should have same DateInt")
		assert.Equal(t, int16(14), key.Hour, "All files should have same Hour")
		assert.Equal(t, ingestDateint, key.IngestDateint, "All files should have same IngestDateint")

		hourKeys = append(hourKeys, key)
		totalRecords += result.RecordCount

		// Verify file exists and has reasonable size
		assert.FileExists(t, result.FileName)
		assert.Greater(t, result.FileSize, int64(0))
		assert.Greater(t, result.RecordCount, int64(0))
		assert.LessOrEqual(t, result.RecordCount, int64(smallRpfEstimate))
	}

	// Should have multiple files (at least 3 for 150 records with limit 50)
	assert.GreaterOrEqual(t, len(hourKeys), 3, "Should create multiple files when exceeding rpfEstimate")

	// Total records should match input
	assert.Equal(t, int64(150), totalRecords, "No records should be lost")

	// FileIndex should be unique and sequential
	fileIndices := make([]int16, len(hourKeys))
	for i, key := range hourKeys {
		fileIndices[i] = key.FileIndex
	}

	// Check that FileIndex values are unique and start from 0
	uniqueIndices := make(map[int16]bool)
	minIndex, maxIndex := int16(999), int16(-1)

	for _, idx := range fileIndices {
		assert.False(t, uniqueIndices[idx], "FileIndex %d should be unique", idx)
		uniqueIndices[idx] = true

		if idx < minIndex {
			minIndex = idx
		}
		if idx > maxIndex {
			maxIndex = idx
		}
	}

	assert.Equal(t, int16(0), minIndex, "FileIndex should start from 0")
	assert.Equal(t, int16(len(hourKeys)-1), maxIndex, "FileIndex should be sequential")
}

// TestSingleFileWhenUnderLimit verifies that when records fit in one file,
// only one file with FileIndex 0 is created.
func TestSingleFileWhenUnderLimit(t *testing.T) {
	tmpdir := t.TempDir()

	// Create small test data that won't exceed rpfEstimate
	records := make([]map[string]any, 10) // Only 10 records
	baseTime := time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC).UnixMilli()

	for i := 0; i < 10; i++ {
		records[i] = map[string]any{
			"_cardinalhq.timestamp": baseTime + int64(i*1000),
			"msg":                   "test message",
			"level":                 "info",
		}
	}

	// Create a test parquet file using the same pattern as existing tests
	inputFile, err := os.CreateTemp(tmpdir, "test-*.parquet")
	require.NoError(t, err)
	defer os.Remove(inputFile.Name())

	// Create schema for log records (match existing test pattern)
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"msg":                   parquet.String(),
		"level":                 parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

	pw := parquet.NewWriter(inputFile, schema)
	for _, record := range records {
		require.NoError(t, pw.Write(record))
	}
	require.NoError(t, pw.Close())
	require.NoError(t, inputFile.Close())

	// Load and process
	fh, err := filecrunch.LoadSchemaForFile(inputFile.Name())
	require.NoError(t, err)
	defer fh.Close()

	// Process with large rpfEstimate to ensure single file
	const largeRpfEstimate = 1000
	ingestDateint := int32(20230101)

	ll := slog.New(slog.NewTextHandler(io.Discard, nil))
	results, err := buffet.ProcessAndSplit(ll, fh, tmpdir, ingestDateint, largeRpfEstimate)
	require.NoError(t, err)

	// Should get exactly one file
	assert.Len(t, results, 1, "Should create exactly one file when under limit")

	for key, result := range results {
		assert.Equal(t, int16(0), key.FileIndex, "Single file should have FileIndex 0")
		assert.Equal(t, int64(10), result.RecordCount, "Should contain all records")
	}
}
