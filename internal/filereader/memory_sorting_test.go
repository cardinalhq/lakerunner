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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestMemorySortingReader_SortsByKey(t *testing.T) {
	// Create rows in unsorted order
	inputRows := []Row{
		{
			wkk.RowKeyCName:        "memory.usage", // Should come after cpu.usage
			wkk.RowKeyCTID:         int64(12345),
			wkk.RowKeyCTimestamp:   int64(10000),
			wkk.NewRowKey("value"): 85.0,
		},
		{
			wkk.RowKeyCName:        "cpu.usage",
			wkk.RowKeyCTID:         int64(54321), // Higher TID, should come after 12345
			wkk.RowKeyCTimestamp:   int64(10000),
			wkk.NewRowKey("value"): 65.0,
		},
		{
			wkk.RowKeyCName:        "cpu.usage",
			wkk.RowKeyCTID:         int64(12345),
			wkk.RowKeyCTimestamp:   int64(20000), // Later timestamp
			wkk.NewRowKey("value"): 95.0,
		},
		{
			wkk.RowKeyCName:        "cpu.usage",
			wkk.RowKeyCTID:         int64(12345),
			wkk.RowKeyCTimestamp:   int64(10000), // Earlier timestamp, should come first
			wkk.NewRowKey("value"): 75.0,
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &MetricSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Should have 4 rows in sorted order
	require.Len(t, allRows, 4)

	// Verify sorting: [cpu.usage:12345:10000, cpu.usage:12345:20000, cpu.usage:54321:10000, memory.usage:12345:10000]
	expectedOrder := []struct {
		name      string
		tid       int64
		timestamp int64
	}{
		{"cpu.usage", 12345, 10000},
		{"cpu.usage", 12345, 20000},
		{"cpu.usage", 54321, 10000},
		{"memory.usage", 12345, 10000},
	}

	for i, expected := range expectedOrder {
		assert.Equal(t, expected.name, allRows[i][wkk.RowKeyCName], "Row %d name mismatch", i)
		assert.Equal(t, expected.tid, allRows[i][wkk.RowKeyCTID], "Row %d TID mismatch", i)
		assert.Equal(t, expected.timestamp, allRows[i][wkk.RowKeyCTimestamp], "Row %d timestamp mismatch", i)
	}
}

func TestMemorySortingReader_EmptyInput(t *testing.T) {
	mockReader := NewMockReader([]Row{})
	sortingReader, err := NewMemorySortingReader(mockReader, &MetricSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	batch, err := sortingReader.Next(context.TODO())
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, batch)
}

func TestMemorySortingReader_MissingFields(t *testing.T) {
	// Rows with missing required fields should be sorted to the end
	inputRows := []Row{
		{
			wkk.RowKeyCName:        "cpu.usage",
			wkk.RowKeyCTID:         int64(12345),
			wkk.RowKeyCTimestamp:   int64(10000),
			wkk.NewRowKey("value"): 75.0,
		},
		{
			// Missing TID - should be sorted to end
			wkk.RowKeyCName:        "memory.usage",
			wkk.RowKeyCTimestamp:   int64(10000),
			wkk.NewRowKey("value"): 85.0,
		},
		{
			// Missing name - should be sorted to end
			wkk.RowKeyCTID:         int64(12345),
			wkk.RowKeyCTimestamp:   int64(10000),
			wkk.NewRowKey("value"): 65.0,
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &MetricSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	require.Len(t, allRows, 3)

	// First row should be the valid one
	assert.Equal(t, "cpu.usage", allRows[0][wkk.RowKeyCName])
	assert.Equal(t, int64(12345), allRows[0][wkk.RowKeyCTID])

	// Invalid rows should be at the end (exact order may vary)
	// Just verify we have one row without name and one without TID
	hasRowWithoutName := false
	hasRowWithoutTID := false
	for i := 1; i < len(allRows); i++ {
		if _, hasName := allRows[i][wkk.RowKeyCName]; !hasName {
			hasRowWithoutName = true
		}
		if _, hasTID := allRows[i][wkk.RowKeyCTID]; !hasTID {
			hasRowWithoutTID = true
		}
	}
	assert.True(t, hasRowWithoutName, "Should have a row without name")
	assert.True(t, hasRowWithoutTID, "Should have a row without TID")
}

// reverseTimestampSortKeyProvider provides reversed timestamp sorting for testing
type reverseTimestampSortKeyProvider struct{}

func (p *reverseTimestampSortKeyProvider) MakeKey(row Row) SortKey {
	return &reverseTimestampSortKey{
		timestamp: row[wkk.RowKeyCTimestamp].(int64),
	}
}

type reverseTimestampSortKey struct {
	timestamp int64
}

func (k *reverseTimestampSortKey) Compare(other SortKey) int {
	o := other.(*reverseTimestampSortKey)
	// Reverse order: larger timestamps come first
	if k.timestamp > o.timestamp {
		return -1
	}
	if k.timestamp < o.timestamp {
		return 1
	}
	return 0
}

func (k *reverseTimestampSortKey) Release() {
	// No pooling for test-only key
}

func TestMemorySortingReader_CustomSortFunction(t *testing.T) {
	// Test with a custom sort function that sorts by timestamp only (reverse order)
	inputRows := []Row{
		{
			wkk.RowKeyCName:        "cpu.usage",
			wkk.RowKeyCTimestamp:   int64(30000),
			wkk.NewRowKey("value"): 75.0,
		},
		{
			wkk.RowKeyCName:        "memory.usage",
			wkk.RowKeyCTimestamp:   int64(10000), // Should come last (earliest timestamp)
			wkk.NewRowKey("value"): 85.0,
		},
		{
			wkk.RowKeyCName:        "disk.usage",
			wkk.RowKeyCTimestamp:   int64(20000),
			wkk.NewRowKey("value"): 65.0,
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &reverseTimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	require.Len(t, allRows, 3)

	// Verify reverse timestamp ordering: 30000, 20000, 10000
	assert.Equal(t, int64(30000), allRows[0][wkk.RowKeyCTimestamp])
	assert.Equal(t, int64(20000), allRows[1][wkk.RowKeyCTimestamp])
	assert.Equal(t, int64(10000), allRows[2][wkk.RowKeyCTimestamp])
}

func TestMemorySortingReader_TimestampOnlySort(t *testing.T) {
	// Test the built-in TimestampSort function
	inputRows := []Row{
		{
			wkk.RowKeyCName:      "memory.usage", // Different names, but should sort by timestamp
			wkk.RowKeyCTimestamp: int64(30000),
		},
		{
			wkk.RowKeyCName:      "cpu.usage",
			wkk.RowKeyCTimestamp: int64(10000), // Should come first
		},
		{
			wkk.RowKeyCName:      "disk.usage",
			wkk.RowKeyCTimestamp: int64(20000),
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &TimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	require.Len(t, allRows, 3)

	// Verify timestamp ordering: 10000, 20000, 30000
	assert.Equal(t, int64(10000), allRows[0][wkk.RowKeyCTimestamp])
	assert.Equal(t, int64(20000), allRows[1][wkk.RowKeyCTimestamp])
	assert.Equal(t, int64(30000), allRows[2][wkk.RowKeyCTimestamp])
}
