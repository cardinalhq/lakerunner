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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemorySortingReader_SortsByKey(t *testing.T) {
	// Create rows in unsorted order
	inputRows := []Row{
		{
			"_cardinalhq.name":      "memory.usage", // Should come after cpu.usage
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"value":                 85.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(54321), // Higher TID, should come after 12345
			"_cardinalhq.timestamp": int64(10000),
			"value":                 65.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(20000), // Later timestamp
			"value":                 95.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000), // Earlier timestamp, should come first
			"value":                 75.0,
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, MetricNameTidTimestampSort(), 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
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
		assert.Equal(t, expected.name, allRows[i]["_cardinalhq.name"], "Row %d name mismatch", i)
		assert.Equal(t, expected.tid, allRows[i]["_cardinalhq.tid"], "Row %d TID mismatch", i)
		assert.Equal(t, expected.timestamp, allRows[i]["_cardinalhq.timestamp"], "Row %d timestamp mismatch", i)
	}
}

func TestMemorySortingReader_EmptyInput(t *testing.T) {
	mockReader := NewMockReader([]Row{})
	sortingReader, err := NewMemorySortingReader(mockReader, MetricNameTidTimestampSort(), 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	batch, err := sortingReader.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, batch)
}

func TestMemorySortingReader_MissingFields(t *testing.T) {
	// Rows with missing required fields should be sorted to the end
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"value":                 75.0,
		},
		{
			// Missing TID - should be sorted to end
			"_cardinalhq.name":      "memory.usage",
			"_cardinalhq.timestamp": int64(10000),
			"value":                 85.0,
		},
		{
			// Missing name - should be sorted to end
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"value":                 65.0,
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, MetricNameTidTimestampSort(), 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	require.Len(t, allRows, 3)

	// First row should be the valid one
	assert.Equal(t, "cpu.usage", allRows[0]["_cardinalhq.name"])
	assert.Equal(t, int64(12345), allRows[0]["_cardinalhq.tid"])

	// Invalid rows should be at the end (exact order may vary)
	// Just verify we have one row without name and one without TID
	hasRowWithoutName := false
	hasRowWithoutTID := false
	for i := 1; i < len(allRows); i++ {
		if _, hasName := allRows[i]["_cardinalhq.name"]; !hasName {
			hasRowWithoutName = true
		}
		if _, hasTID := allRows[i]["_cardinalhq.tid"]; !hasTID {
			hasRowWithoutTID = true
		}
	}
	assert.True(t, hasRowWithoutName, "Should have a row without name")
	assert.True(t, hasRowWithoutTID, "Should have a row without TID")
}

func TestMemorySortingReader_CustomSortFunction(t *testing.T) {
	// Test with a custom sort function that sorts by timestamp only (reverse order)
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.timestamp": int64(30000),
			"value":                 75.0,
		},
		{
			"_cardinalhq.name":      "memory.usage",
			"_cardinalhq.timestamp": int64(10000), // Should come last (earliest timestamp)
			"value":                 85.0,
		},
		{
			"_cardinalhq.name":      "disk.usage",
			"_cardinalhq.timestamp": int64(20000),
			"value":                 65.0,
		},
	}

	// Custom sort function: reverse timestamp order (newest first)
	customSortFunc := func(a, b map[string]any) int {
		tsA, tsAOk := a["_cardinalhq.timestamp"].(int64)
		tsB, tsBOk := b["_cardinalhq.timestamp"].(int64)
		if !tsAOk || !tsBOk {
			return 0
		}
		// Reverse order: newer timestamps come first
		if tsA > tsB {
			return -1
		}
		if tsA < tsB {
			return 1
		}
		return 0
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, customSortFunc, 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	require.Len(t, allRows, 3)

	// Verify reverse timestamp ordering: 30000, 20000, 10000
	assert.Equal(t, int64(30000), allRows[0]["_cardinalhq.timestamp"])
	assert.Equal(t, int64(20000), allRows[1]["_cardinalhq.timestamp"])
	assert.Equal(t, int64(10000), allRows[2]["_cardinalhq.timestamp"])
}

func TestMemorySortingReader_TimestampOnlySort(t *testing.T) {
	// Test the built-in TimestampSort function
	inputRows := []Row{
		{
			"_cardinalhq.name":      "memory.usage", // Different names, but should sort by timestamp
			"_cardinalhq.timestamp": int64(30000),
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.timestamp": int64(10000), // Should come first
		},
		{
			"_cardinalhq.name":      "disk.usage",
			"_cardinalhq.timestamp": int64(20000),
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, TimestampSort(), 1000)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := sortingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	require.Len(t, allRows, 3)

	// Verify timestamp ordering: 10000, 20000, 30000
	assert.Equal(t, int64(10000), allRows[0]["_cardinalhq.timestamp"])
	assert.Equal(t, int64(20000), allRows[1]["_cardinalhq.timestamp"])
	assert.Equal(t, int64(30000), allRows[2]["_cardinalhq.timestamp"])
}
