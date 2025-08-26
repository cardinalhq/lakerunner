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

package spillers

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCborSpiller(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)
	require.NotNil(t, spiller)
	require.NotNil(t, spiller.config)
}

func TestCborSpiller_WriteAndReadSpillFile(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)

	// Test data with various types
	testRows := []map[string]any{
		{
			"_cardinalhq.name":      "metric_z",
			"_cardinalhq.tid":       int64(200),
			"_cardinalhq.timestamp": int64(3000),
			"value":                 float64(1.0),
			"tags":                  []string{"tag1", "tag2"},
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(1000),
			"value":                 float64(2.0),
			"data":                  []byte{1, 2, 3},
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(2000),
			"value":                 float64(3.0),
			"nested":                map[string]any{"key": "value"},
		},
	}

	// Key function for sorting by name, tid, timestamp
	keyFunc := func(row map[string]any) any {
		name := row["_cardinalhq.name"].(string)
		tid := row["_cardinalhq.tid"].(int64)
		timestamp := row["_cardinalhq.timestamp"].(int64)
		return fmt.Sprintf("%s_%d_%d", name, tid, timestamp)
	}

	// Write spill file
	tmpDir := os.TempDir()
	spillFile, err := spiller.WriteSpillFile(tmpDir, testRows, keyFunc)
	require.NoError(t, err)
	require.NotNil(t, spillFile)
	require.Equal(t, int64(3), spillFile.RowCount)
	require.NotEmpty(t, spillFile.Path)

	// Verify file exists
	_, err = os.Stat(spillFile.Path)
	require.NoError(t, err)

	// Open spill file for reading
	reader, err := spiller.OpenSpillFile(spillFile, keyFunc)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	// Read all rows back
	var readRows []map[string]any
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		readRows = append(readRows, row)
	}

	// Verify we read the expected number of rows
	require.Len(t, readRows, 3)

	// Verify sorting - should be metric_a (both rows), then metric_z
	assert.Equal(t, "metric_a", readRows[0]["_cardinalhq.name"])
	assert.Equal(t, int64(1000), readRows[0]["_cardinalhq.timestamp"])

	assert.Equal(t, "metric_a", readRows[1]["_cardinalhq.name"])
	assert.Equal(t, int64(2000), readRows[1]["_cardinalhq.timestamp"])

	assert.Equal(t, "metric_z", readRows[2]["_cardinalhq.name"])
	assert.Equal(t, int64(3000), readRows[2]["_cardinalhq.timestamp"])

	// Debug: print actual fields in first few rows
	for i, row := range readRows {
		t.Logf("Row %d fields: %+v", i, row)
	}

	// Verify type preservation - find rows with specific fields after sorting
	var tagsRow, dataRow, nestedRow map[string]any
	for _, row := range readRows {
		if _, ok := row["tags"]; ok {
			tagsRow = row
		}
		if _, ok := row["data"]; ok {
			dataRow = row
		}
		if _, ok := row["nested"]; ok {
			nestedRow = row
		}
	}

	// Verify tags field (was originally in row 0)
	require.NotNil(t, tagsRow, "Should find row with tags field")
	assert.IsType(t, []any{}, tagsRow["tags"]) // []string becomes []any
	assert.Equal(t, []any{"tag1", "tag2"}, tagsRow["tags"])

	// Verify data field (was originally in row 1)
	require.NotNil(t, dataRow, "Should find row with data field")
	assert.IsType(t, []byte{}, dataRow["data"]) // []byte preserved
	assert.Equal(t, []byte{1, 2, 3}, dataRow["data"])

	// Verify nested field (was originally in row 2)
	require.NotNil(t, nestedRow, "Should find row with nested field")
	assert.IsType(t, map[string]any{}, nestedRow["nested"]) // map preserved
	assert.Equal(t, map[string]any{"key": "value"}, nestedRow["nested"])

	// Clean up
	err = spiller.CleanupSpillFile(spillFile)
	require.NoError(t, err)

	// Verify file is removed
	_, err = os.Stat(spillFile.Path)
	require.True(t, os.IsNotExist(err))
}

func TestCborSpiller_EmptyRows(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)

	keyFunc := func(row map[string]any) any {
		return row["key"]
	}

	// Write empty slice
	spillFile, err := spiller.WriteSpillFile(os.TempDir(), []map[string]any{}, keyFunc)
	require.NoError(t, err)
	require.NotNil(t, spillFile)
	require.Equal(t, int64(0), spillFile.RowCount)
	require.Empty(t, spillFile.Path) // No file created for empty data
}

func TestCborSpiller_TypePreservation(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)

	// Test row with various types
	testRow := map[string]any{
		"string_field":  "test_string",
		"int64_field":   int64(9223372036854775807),
		"float64_field": float64(3.14159),
		"bool_field":    true,
		"byte_slice":    []byte{0x01, 0x02, 0x03},
		"float64_slice": []float64{1.1, 2.2, 3.3},
		"mixed_slice":   []any{"string", int64(42), float64(3.14)},
		"nil_field":     nil,
	}

	keyFunc := func(row map[string]any) any {
		return "key"
	}

	// Write and read back
	spillFile, err := spiller.WriteSpillFile(os.TempDir(), []map[string]any{testRow}, keyFunc)
	require.NoError(t, err)
	defer func() { _ = spiller.CleanupSpillFile(spillFile) }()

	reader, err := spiller.OpenSpillFile(spillFile, keyFunc)
	require.NoError(t, err)
	defer reader.Close()

	decoded, err := reader.Next()
	require.NoError(t, err)

	// Verify all types are preserved correctly
	assert.Equal(t, "test_string", decoded["string_field"])
	assert.Equal(t, int64(9223372036854775807), decoded["int64_field"])
	assert.Equal(t, float64(3.14159), decoded["float64_field"])
	assert.Equal(t, true, decoded["bool_field"])
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded["byte_slice"])
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, decoded["float64_slice"]) // Restored from []any
	assert.Equal(t, []any{"string", int64(42), float64(3.14)}, decoded["mixed_slice"])
	assert.Nil(t, decoded["nil_field"])
}

func TestCborSpiller_CleanupNonExistentFile(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)

	// Should not error when cleaning up non-existent file
	spillFile := &SpillFile{
		Path:     "/tmp/nonexistent.cbor",
		RowCount: 0,
	}

	err = spiller.CleanupSpillFile(spillFile)
	require.NoError(t, err)
}

func TestCborSpiller_CleanupEmptyPath(t *testing.T) {
	spiller, err := NewCborSpiller()
	require.NoError(t, err)

	// Should not error when path is empty
	spillFile := &SpillFile{
		Path:     "",
		RowCount: 0,
	}

	err = spiller.CleanupSpillFile(spillFile)
	require.NoError(t, err)
}
