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

func TestPostTranslationSortingReader_SortsByKey(t *testing.T) {
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

	mockReader := newMockAggregatingReader(inputRows)
	sortingReader, err := NewPostTranslationSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		rows := make([]Row, 10)
		n, err := sortingReader.Read(rows)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			allRows = append(allRows, rows[i])
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
		assert.Equal(t, expected.name, allRows[i]["_cardinalhq.name"], "Row %d name mismatch", i)
		assert.Equal(t, expected.tid, allRows[i]["_cardinalhq.tid"], "Row %d TID mismatch", i)
		assert.Equal(t, expected.timestamp, allRows[i]["_cardinalhq.timestamp"], "Row %d timestamp mismatch", i)
	}
}

func TestPostTranslationSortingReader_EmptyInput(t *testing.T) {
	mockReader := newMockAggregatingReader([]Row{})
	sortingReader, err := NewPostTranslationSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	rows := make([]Row, 10)
	n, err := sortingReader.Read(rows)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)
}

func TestPostTranslationSortingReader_MissingFields(t *testing.T) {
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

	mockReader := newMockAggregatingReader(inputRows)
	sortingReader, err := NewPostTranslationSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all results
	var allRows []Row
	for {
		rows := make([]Row, 10)
		n, err := sortingReader.Read(rows)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			allRows = append(allRows, rows[i])
		}
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
