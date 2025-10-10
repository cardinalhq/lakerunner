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

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// Helper function to convert []map[string]any to *pipeline.Batch for testing
func testDataToBatch(testRows []map[string]any) *pipeline.Batch {
	batch := pipeline.GetBatch()
	for _, testRow := range testRows {
		row := batch.AddRow()
		for k, v := range testRow {
			row[wkk.NewRowKey(k)] = v
		}
	}
	return batch
}

func TestUnifiedWriter_Basic(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		TmpDir:         tmpdir,
		RecordsPerFile: 20, // Fixed limit for predictable tests
	}
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write some test data
	testRows := []map[string]any{
		{"id": int64(1), "timestamp": int64(1000), "message": "first"},
		{"id": int64(2), "timestamp": int64(2000), "message": "second"},
		{"id": int64(3), "timestamp": int64(3000), "message": "third"},
	}

	batch := testDataToBatch(testRows)
	defer pipeline.ReturnBatch(batch)

	if err := writer.WriteBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Close and get results
	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify results
	if len(results) != 1 {
		t.Errorf("Expected 1 result file, got %d", len(results))
	}

	result := results[0]
	if result.RecordCount != 3 {
		t.Errorf("Expected 3 records, got %d", result.RecordCount)
	}
	if result.FileSize <= 0 {
		t.Errorf("Expected positive file size, got %d", result.FileSize)
	}
	if result.FileName == "" {
		t.Error("Expected non-empty filename")
	}

	// Verify file exists and can be read
	if _, err := os.Stat(result.FileName); os.IsNotExist(err) {
		t.Error("Result file does not exist")
	}

	// Clean up
	_ = os.Remove(result.FileName)
}

func TestUnifiedWriter_WriteBatch(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		TmpDir:         tmpdir,
		RecordsPerFile: 20, // Fixed limit for predictable tests
	}
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Create a pipeline batch
	batch := pipeline.GetBatch()
	defer pipeline.ReturnBatch(batch)

	for i := 0; i < 100; i++ {
		row := batch.AddRow()
		row[wkk.NewRowKey("id")] = int64(i)
		row[wkk.NewRowKey("value")] = "test message"
	}

	// Write batch using new method
	if err := writer.WriteBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify total records
	totalRecords := int64(0)
	for _, result := range results {
		totalRecords += result.RecordCount
		_ = os.Remove(result.FileName)
	}

	if totalRecords != 100 {
		t.Errorf("Expected 100 total records, got %d", totalRecords)
	}
}
