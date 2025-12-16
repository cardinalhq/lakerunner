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

package factories

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAggParquet_EmptyCounts(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "empty.parquet")

	size, err := WriteAggParquet(context.Background(), filename, map[LogAggKey]int64{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// File should not be created
	_, err = os.Stat(filename)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteAggParquet_SingleRow(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "single.parquet")

	counts := map[LogAggKey]int64{
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 42,
	}

	size, err := WriteAggParquet(context.Background(), filename, counts)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	// Verify the parquet file can be read and has correct content
	rows := readAggParquet(t, filename)
	require.Len(t, rows, 1)

	assert.Equal(t, int64(1234567890000), rows[0].TimestampBucket)
	assert.Equal(t, "info", rows[0].LogLevel)
	assert.Equal(t, "example.com", rows[0].StreamId)
	assert.Equal(t, AggFrequency, rows[0].Frequency)
	assert.Equal(t, int64(42), rows[0].Count)
}

func TestWriteAggParquet_MultipleRows(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "multi.parquet")

	counts := map[LogAggKey]int64{
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}:  10,
		{TimestampBucket: 1234567890000, LogLevel: "error", StreamId: "example.com"}: 5,
		{TimestampBucket: 1234567900000, LogLevel: "info", StreamId: "example.com"}:  20,
		{TimestampBucket: 1234567900000, LogLevel: "info", StreamId: "other.com"}:    15,
	}

	size, err := WriteAggParquet(context.Background(), filename, counts)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	rows := readAggParquet(t, filename)
	require.Len(t, rows, 4)

	// Build a map for easier verification
	resultMap := make(map[LogAggKey]int64)
	for _, row := range rows {
		key := LogAggKey{
			TimestampBucket: row.TimestampBucket,
			LogLevel:        row.LogLevel,
			StreamId:        row.StreamId,
		}
		resultMap[key] = row.Count
		// Verify frequency is always 10000
		assert.Equal(t, AggFrequency, row.Frequency)
	}

	// Verify counts match
	for key, expectedCount := range counts {
		assert.Equal(t, expectedCount, resultMap[key], "Count mismatch for key %+v", key)
	}
}

func TestWriteAggParquet_SortedByTimestampBucket(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "sorted.parquet")

	// Input counts in non-sorted order
	counts := map[LogAggKey]int64{
		{TimestampBucket: 3000000000000, LogLevel: "info", StreamId: "a.com"}: 1,
		{TimestampBucket: 1000000000000, LogLevel: "info", StreamId: "a.com"}: 2,
		{TimestampBucket: 2000000000000, LogLevel: "info", StreamId: "a.com"}: 3,
	}

	size, err := WriteAggParquet(context.Background(), filename, counts)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	rows := readAggParquet(t, filename)
	require.Len(t, rows, 3)

	// Verify rows are sorted by timestamp_bucket
	assert.Equal(t, int64(1000000000000), rows[0].TimestampBucket)
	assert.Equal(t, int64(2000000000000), rows[1].TimestampBucket)
	assert.Equal(t, int64(3000000000000), rows[2].TimestampBucket)
}

func TestWriteAggParquet_EmptyStringValues(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "empty_strings.parquet")

	counts := map[LogAggKey]int64{
		{TimestampBucket: 1234567890000, LogLevel: "", StreamId: ""}:     5,
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: ""}: 10,
	}

	size, err := WriteAggParquet(context.Background(), filename, counts)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	rows := readAggParquet(t, filename)
	require.Len(t, rows, 2)

	// Verify empty strings are preserved
	resultMap := make(map[LogAggKey]int64)
	for _, row := range rows {
		key := LogAggKey{
			TimestampBucket: row.TimestampBucket,
			LogLevel:        row.LogLevel,
			StreamId:        row.StreamId,
		}
		resultMap[key] = row.Count
	}

	assert.Equal(t, int64(5), resultMap[LogAggKey{TimestampBucket: 1234567890000, LogLevel: "", StreamId: ""}])
	assert.Equal(t, int64(10), resultMap[LogAggKey{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: ""}])
}

func TestWriteAggParquet_LargeDataset(t *testing.T) {
	tmpdir := t.TempDir()
	filename := filepath.Join(tmpdir, "large.parquet")

	// Generate 1000 unique keys
	counts := make(map[LogAggKey]int64)
	for i := int64(0); i < 100; i++ {
		for j := 0; j < 10; j++ {
			key := LogAggKey{
				TimestampBucket: 1234567890000 + i*10000, // 10s buckets
				LogLevel:        []string{"info", "warn", "error", "debug", "trace"}[j%5],
				StreamId:        []string{"service-a", "service-b"}[j%2],
			}
			counts[key] = i + int64(j)
		}
	}

	size, err := WriteAggParquet(context.Background(), filename, counts)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	rows := readAggParquet(t, filename)
	// We should have at most 1000 rows, but some keys may overlap
	assert.Greater(t, len(rows), 0)

	// Verify all rows have correct frequency
	for _, row := range rows {
		assert.Equal(t, AggFrequency, row.Frequency)
	}

	// Verify sorted by timestamp
	for i := 1; i < len(rows); i++ {
		assert.LessOrEqual(t, rows[i-1].TimestampBucket, rows[i].TimestampBucket,
			"Rows should be sorted by timestamp_bucket")
	}
}

func TestWriteAggParquet_FileCreationError(t *testing.T) {
	// Try to write to a directory that doesn't exist
	filename := "/nonexistent/directory/test.parquet"

	counts := map[LogAggKey]int64{
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 1,
	}

	_, err := WriteAggParquet(context.Background(), filename, counts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create agg parquet file")
}

// readAggParquet reads the parquet file and returns all rows
func readAggParquet(t *testing.T, filename string) []LogAggRow {
	t.Helper()

	f, err := os.Open(filename)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	require.NoError(t, err)

	pf, err := parquet.OpenFile(f, stat.Size())
	require.NoError(t, err)

	reader := parquet.NewGenericReader[LogAggRow](pf)
	defer func() { _ = reader.Close() }()

	rows := make([]LogAggRow, reader.NumRows())
	n, err := reader.Read(rows)
	// Read() returns io.EOF when all rows have been read, even if n > 0
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, int(reader.NumRows()), n)

	return rows[:n]
}
