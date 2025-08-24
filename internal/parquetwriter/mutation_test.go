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

package parquetwriter

import (
	"context"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParquetWriterWithMutatedRows verifies that the parquetwriter works correctly
// when rows are mutated after being passed to Write(). This tests the scenario
// where translators modify rows in-place.
func TestParquetWriterWithMutatedRows(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "mutation-test",
		TmpDir:         tmpdir,
		TargetFileSize: 1000,
		OrderBy:        OrderNone,
		BytesPerRecord: 50.0,
	}

	writer, err := NewUnifiedWriter(config)
	require.NoError(t, err)
	defer writer.Abort()

	// Create a row and write it
	row := map[string]any{
		"id":      int64(1),
		"message": "original",
	}

	err = writer.Write(row)
	require.NoError(t, err)

	// Mutate the row after writing (simulating in-place translator modification)
	row["message"] = "mutated"
	row["new_field"] = "added_after_write"

	// Write another row
	row2 := map[string]any{
		"id":      int64(2),
		"message": "second",
	}

	err = writer.Write(row2)
	require.NoError(t, err)

	// Mutate the second row as well
	row2["message"] = "also_mutated"
	row2["another_field"] = int64(42)

	// Close and get results
	results, err := writer.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1, "Expected 1 output file")

	// Read back the file to verify the original data was preserved
	file, err := os.Open(results[0].FileName)
	require.NoError(t, err)
	defer file.Close()
	defer os.Remove(results[0].FileName)

	// Create schema that matches what was actually written
	nodes := map[string]parquet.Node{
		"id":      parquet.Int(64),
		"message": parquet.String(),
	}
	schema := parquet.NewSchema("mutation-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var records []map[string]any
	for {
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)
		n, err := reader.Read(rows)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			require.NoError(t, err)
		}
		records = append(records, rows[0])
	}

	// Verify the data was written with original values, not mutated ones
	require.Len(t, records, 2)

	assert.Equal(t, int64(1), records[0]["id"])
	assert.Equal(t, "original", records[0]["message"]) // Should be original, not "mutated"
	assert.NotContains(t, records[0], "new_field")     // Added field should not be in file

	assert.Equal(t, int64(2), records[1]["id"])
	assert.Equal(t, "second", records[1]["message"])   // Should be original, not "also_mutated"
	assert.NotContains(t, records[1], "another_field") // Added field should not be in file
}

// TestParquetWriterMutationSafety verifies that mutations after Write() don't affect written data.
func TestParquetWriterMutationSafety(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "mutation-safety-test",
		TmpDir:         tmpdir,
		TargetFileSize: 1000,
		OrderBy:        OrderNone,
		BytesPerRecord: 75.0,
	}

	writer, err := NewUnifiedWriter(config)
	require.NoError(t, err)
	defer writer.Abort()

	// Use a consistent schema for all rows to avoid dynamic schema complexity
	row1 := map[string]any{
		"id":      int64(1),
		"message": "first message",
		"status":  "active",
	}

	row2 := map[string]any{
		"id":      int64(2),
		"message": "second message",
		"status":  "inactive",
	}

	row3 := map[string]any{
		"id":      int64(3),
		"message": "third message",
		"status":  "pending",
	}

	// Write all rows
	require.NoError(t, writer.Write(row1))
	require.NoError(t, writer.Write(row2))
	require.NoError(t, writer.Write(row3))

	// Mutate all rows after writing to ensure writer made copies
	row1["id"] = int64(999)
	row1["message"] = "corrupted first"
	row1["status"] = "corrupted"
	row1["evil_field"] = "should not appear"

	row2["id"] = int64(888)
	row2["message"] = "corrupted second"
	row2["status"] = "corrupted"

	row3["id"] = int64(777)
	row3["message"] = "corrupted third"
	row3["status"] = "corrupted"

	// Close and get results
	results, err := writer.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Read back and verify original data is preserved
	file, err := os.Open(results[0].FileName)
	require.NoError(t, err)
	defer file.Close()
	defer os.Remove(results[0].FileName)

	nodes := map[string]parquet.Node{
		"id":      parquet.Int(64),
		"message": parquet.String(),
		"status":  parquet.String(),
	}
	schema := parquet.NewSchema("mutation-safety-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var records []map[string]any
	for {
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)
		n, err := reader.Read(rows)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			require.NoError(t, err)
		}
		records = append(records, rows[0])
	}

	require.Len(t, records, 3)

	// Verify original data is preserved
	expectedData := map[int64]struct {
		message string
		status  string
	}{
		1: {"first message", "active"},
		2: {"second message", "inactive"},
		3: {"third message", "pending"},
	}

	for _, record := range records {
		id := record["id"].(int64)
		expected, exists := expectedData[id]
		require.True(t, exists, "Unexpected ID: %d", id)

		assert.Equal(t, expected.message, record["message"], "Message mismatch for ID %d", id)
		assert.Equal(t, expected.status, record["status"], "Status mismatch for ID %d", id)

		// Verify no evil fields made it in
		assert.NotContains(t, record, "evil_field")
	}
}

// TestOrderedWriterWithMutations tests that ordering works correctly even when
// source rows are mutated after being passed to the writer.
func TestOrderedWriterWithMutations(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "ordered-mutation-test",
		TmpDir:         tmpdir,
		TargetFileSize: 1000,
		OrderBy:        OrderInMemory,
		OrderKeyFunc: func(row map[string]any) any {
			return row["timestamp"].(int64)
		},
		BytesPerRecord: 60.0,
	}

	writer, err := NewUnifiedWriter(config)
	require.NoError(t, err)
	defer writer.Abort()

	// Write rows out of order
	rows := []map[string]any{
		{"timestamp": int64(300), "value": "third"},
		{"timestamp": int64(100), "value": "first"},
		{"timestamp": int64(200), "value": "second"},
	}

	for _, row := range rows {
		require.NoError(t, writer.Write(row))
	}

	// Mutate all source rows after writing
	for i, row := range rows {
		row["timestamp"] = int64(999 + i) // Change sort key
		row["value"] = "corrupted"        // Change data
	}

	// Close and verify
	results, err := writer.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Read back and verify ordering is preserved based on original data
	file, err := os.Open(results[0].FileName)
	require.NoError(t, err)
	defer file.Close()
	defer os.Remove(results[0].FileName)

	nodes := map[string]parquet.Node{
		"timestamp": parquet.Int(64),
		"value":     parquet.String(),
	}
	schema := parquet.NewSchema("ordered-mutation-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var records []map[string]any
	for {
		rowSlice := make([]map[string]any, 1)
		rowSlice[0] = make(map[string]any)
		n, err := reader.Read(rowSlice)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			require.NoError(t, err)
		}
		records = append(records, rowSlice[0])
	}

	require.Len(t, records, 3)

	// Verify records are in timestamp order with original data
	assert.Equal(t, int64(100), records[0]["timestamp"])
	assert.Equal(t, "first", records[0]["value"])

	assert.Equal(t, int64(200), records[1]["timestamp"])
	assert.Equal(t, "second", records[1]["value"])

	assert.Equal(t, int64(300), records[2]["timestamp"])
	assert.Equal(t, "third", records[2]["value"])
}
