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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// testSplitterSchema creates a schema matching the test batch columns
func testSplitterSchema() *filereader.ReaderSchema {
	schema := filereader.NewReaderSchema()
	// Basic test fields
	schema.AddColumn(wkk.NewRowKey("field1"), wkk.NewRowKey("field1"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("field2"), wkk.NewRowKey("field2"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("group"), wkk.NewRowKey("group"), filereader.DataTypeString, true)
	// String conversion test fields
	schema.AddColumn(wkk.NewRowKey("resource_id"), wkk.NewRowKey("resource_id"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("resource_name"), wkk.NewRowKey("resource_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("attr_value"), wkk.NewRowKey("attr_value"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("regular_field"), wkk.NewRowKey("regular_field"), filereader.DataTypeInt64, true)
	// Fingerprint test fields
	schema.AddColumn(wkk.NewRowKey("message"), wkk.NewRowKey("message"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("resource_service_name"), wkk.NewRowKey("resource_service_name"), filereader.DataTypeString, true)
	// Always-present fields
	schema.AddColumn(wkk.NewRowKey("chq_id"), wkk.NewRowKey("chq_id"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("chq_fingerprint"), wkk.NewRowKey("chq_fingerprint"), filereader.DataTypeInt64, true)
	return schema
}

// mockStatsProvider implements StatsProvider for testing
type mockStatsProvider struct {
	accumulatorFunc func() StatsAccumulator
}

func (p *mockStatsProvider) NewAccumulator() StatsAccumulator {
	if p.accumulatorFunc != nil {
		return p.accumulatorFunc()
	}
	return &mockStatsAccumulator{}
}

// mockStatsAccumulator implements StatsAccumulator for testing
type mockStatsAccumulator struct {
	rows []map[string]any
}

func (a *mockStatsAccumulator) Add(row map[string]any) {
	a.rows = append(a.rows, row)
}

func (a *mockStatsAccumulator) Finalize() any {
	return map[string]any{
		"row_count": len(a.rows),
	}
}

func TestNewFileSplitter(t *testing.T) {
	config := WriterConfig{
		TmpDir:         "/tmp",
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)

	if splitter == nil {
		t.Fatal("NewFileSplitter returned nil")
	}
	if splitter.closed {
		t.Error("Expected splitter to not be closed initially")
	}
	if len(splitter.results) != 0 {
		t.Error("Expected results to be empty initially")
	}
}

func TestFileSplitterWriteBatchRows_NilBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	err := splitter.WriteBatchRows(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil batch")
	}
	if err.Error() != "batch cannot be nil" {
		t.Errorf("Expected 'batch cannot be nil' error, got %q", err.Error())
	}
}

func TestFileSplitterWriteBatchRows_ClosedWriter(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	splitter.closed = true

	ctx := context.Background()
	batch := createTestBatch(t, 1)

	err := splitter.WriteBatchRows(ctx, batch)
	if err != ErrWriterClosed {
		t.Errorf("Expected ErrWriterClosed, got %v", err)
	}
}

func TestFileSplitterWriteBatchRows_EmptyBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createEmptyBatch(t)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Errorf("Expected no error for empty batch, got %v", err)
	}

	// Should not create any files
	if splitter.backend != nil {
		t.Error("Expected no backend for empty batch")
	}
}

func TestFileSplitterWriteBatchRows_SingleBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 3)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Should have created a backend
	if splitter.backend == nil {
		t.Error("Expected backend to be created")
	}
	if splitter.currentRows != 3 {
		t.Errorf("Expected 3 current rows, got %d", splitter.currentRows)
	}

	// Close and check results
	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].RecordCount != 3 {
		t.Errorf("Expected 3 records in result, got %d", results[0].RecordCount)
	}

	// Verify file exists and has content
	if _, err := os.Stat(results[0].FileName); err != nil {
		t.Errorf("Result file does not exist: %v", err)
	}
	if results[0].FileSize <= 0 {
		t.Error("Expected file size to be positive")
	}

	// Clean up
	_ = os.Remove(results[0].FileName)
}

func TestFileSplitterWriteBatchRows_WithStats(t *testing.T) {
	tmpDir := t.TempDir()
	statsProvider := &mockStatsProvider{
		accumulatorFunc: func() StatsAccumulator {
			return &mockStatsAccumulator{}
		},
	}

	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
		StatsProvider:  statsProvider,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 2)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	metadata, ok := results[0].Metadata.(map[string]any)
	if !ok {
		t.Fatal("Expected metadata to be map[string]any")
	}
	if metadata["row_count"] != 2 {
		t.Errorf("Expected row_count to be 2, got %v", metadata["row_count"])
	}

	// Clean up
	_ = os.Remove(results[0].FileName)
}

func TestFileSplitterWriteBatchRows_FileSplittingByRecordCount(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 2, // Split after 2 records
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// First batch - should fit in first file
	batch1 := createTestBatch(t, 2)
	err := splitter.WriteBatchRows(ctx, batch1)
	if err != nil {
		t.Fatalf("WriteBatchRows batch1 failed: %v", err)
	}

	// Second batch - should trigger new file since we'd exceed RecordsPerFile
	batch2 := createTestBatch(t, 1)
	err = splitter.WriteBatchRows(ctx, batch2)
	if err != nil {
		t.Fatalf("WriteBatchRows batch2 failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should have 2 files
	if len(results) != 2 {
		t.Errorf("Expected 2 result files, got %d", len(results))
	}
	if results[0].RecordCount != 2 {
		t.Errorf("Expected first file to have 2 records, got %d", results[0].RecordCount)
	}
	if results[1].RecordCount != 1 {
		t.Errorf("Expected second file to have 1 record, got %d", results[1].RecordCount)
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterWriteBatchRows_UnlimitedFileMode(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: NoRecordLimitPerFile, // Unlimited mode
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Write multiple batches that would normally trigger splits
	// With unlimited mode, all should go into a single file
	totalRecords := 0
	for i := 0; i < 5; i++ {
		batch := createTestBatch(t, 10) // 10 records each batch
		err := splitter.WriteBatchRows(ctx, batch)
		if err != nil {
			t.Fatalf("WriteBatchRows batch %d failed: %v", i, err)
		}
		totalRecords += 10
	}

	// Close and get results
	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should have exactly one file with all records
	if len(results) != 1 {
		t.Errorf("Expected 1 file in unlimited mode, got %d", len(results))
	}
	if results[0].RecordCount != int64(totalRecords) {
		t.Errorf("Expected file to have %d records, got %d", totalRecords, results[0].RecordCount)
	}

	// Clean up
	_ = os.Remove(results[0].FileName)
}

func TestFileSplitterWriteBatchRows_WithGroupKeyFunc(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
		GroupKeyFunc: func(row pipeline.Row) any {
			groupKey := wkk.NewRowKey("group")
			return row[groupKey]
		},
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Create batch with different group values
	batch := createTestBatchWithGroups(t, []string{"A", "A", "B"})
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Check that currentGroup is set to the last row's group
	if splitter.currentGroup != "B" {
		t.Errorf("Expected currentGroup to be 'B', got %v", splitter.currentGroup)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterWriteBatchRows_NoSplitGroups(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 5, // Small limit to trigger splits
		GroupKeyFunc: func(row pipeline.Row) any {
			groupKey := wkk.NewRowKey("group")
			return row[groupKey]
		},
		NoSplitGroups: true,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// First batch: 3 rows of group "A"
	batch1 := createTestBatchWithGroups(t, []string{"A", "A", "A"})
	err := splitter.WriteBatchRows(ctx, batch1)
	if err != nil {
		t.Fatalf("WriteBatchRows batch1 failed: %v", err)
	}

	// Second batch: 7 more rows of group "A" - total 10 rows in group A
	// This exceeds RecordsPerFile (5) but should NOT split because we're still in group A
	batch2 := createTestBatchWithGroups(t, []string{"A", "A", "A", "A", "A", "A", "A"})
	err = splitter.WriteBatchRows(ctx, batch2)
	if err != nil {
		t.Fatalf("WriteBatchRows batch2 failed: %v", err)
	}

	// Third batch: switch to group "B" with 8 rows
	// This should trigger a split because group changed from A to B
	batch3 := createTestBatchWithGroups(t, []string{"B", "B", "B", "B", "B", "B", "B", "B"})
	err = splitter.WriteBatchRows(ctx, batch3)
	if err != nil {
		t.Fatalf("WriteBatchRows batch3 failed: %v", err)
	}

	// Fourth batch: 3 rows of group "C"
	// Should trigger another split because group changed from B to C
	batch4 := createTestBatchWithGroups(t, []string{"C", "C", "C"})
	err = splitter.WriteBatchRows(ctx, batch4)
	if err != nil {
		t.Fatalf("WriteBatchRows batch4 failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should have exactly 3 files (one per group), not split by row count
	if len(results) != 3 {
		t.Errorf("Expected 3 result files (one per group), got %d", len(results))
		for i, result := range results {
			t.Logf("  File %d: %d records", i, result.RecordCount)
		}
		t.FailNow()
	}

	// Verify row counts: 10 + 8 + 3 = 21 total rows
	totalRows := int64(0)
	for i, result := range results {
		totalRows += result.RecordCount
		t.Logf("File %d: %d records", i, result.RecordCount)
	}

	if totalRows != 21 {
		t.Errorf("Expected 21 total rows across all files, got %d", totalRows)
	}

	// First file should have 10 rows (group A - not split despite exceeding RecordsPerFile)
	if results[0].RecordCount != 10 {
		t.Errorf("Expected first file to have 10 rows (group A), got %d", results[0].RecordCount)
	}
	// Second file should have 8 rows (group B)
	if results[1].RecordCount != 8 {
		t.Errorf("Expected second file to have 8 rows (group B), got %d", results[1].RecordCount)
	}
	// Third file should have 3 rows (group C)
	if results[2].RecordCount != 3 {
		t.Errorf("Expected third file to have 3 rows (group C), got %d", results[2].RecordCount)
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterWriteBatchRows_NoSplitGroups_SingleLargeBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 5, // Small limit - but won't trigger splits within a single batch
		GroupKeyFunc: func(row pipeline.Row) any {
			groupKey := wkk.NewRowKey("group")
			return row[groupKey]
		},
		NoSplitGroups: true,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Single large batch with mixed groups: 10xA, 8xB, 3xC (21 total rows)
	// With NoSplitGroups, splits only happen BETWEEN batches when both:
	// 1. RecordsPerFile limit would be exceeded
	// 2. Group changes
	// Since this is a single batch with no previous file, all rows go into one file.
	batch := createTestBatchWithGroups(t, []string{
		"A", "A", "A", "A", "A", "A", "A", "A", "A", "A", // 10 rows of A
		"B", "B", "B", "B", "B", "B", "B", "B", // 8 rows of B
		"C", "C", "C", // 3 rows of C
	})
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should have exactly 1 file with all 21 rows
	// (NoSplitGroups prevents splitting WITHIN a batch)
	if len(results) != 1 {
		t.Errorf("Expected 1 result file, got %d", len(results))
		for i, result := range results {
			t.Logf("  File %d: %d records", i, result.RecordCount)
		}
		t.FailNow()
	}

	// Verify row count: 10 + 8 + 3 = 21 total rows
	if results[0].RecordCount != 21 {
		t.Errorf("Expected file to have 21 rows, got %d", results[0].RecordCount)
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterClose_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// First close
	results1, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should return same results without error
	results2, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}

	if len(results1) != len(results2) {
		t.Error("Multiple Close calls should return same results")
	}

	// Clean up
	for _, result := range results1 {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterAbort(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Get temp file name before abort
	var fileName string
	if splitter.tmpFile != nil {
		fileName = splitter.tmpFile.Name()
	}

	// Abort should clean up
	splitter.Abort()

	// Check that backend and temp file are cleaned up
	if splitter.backend != nil {
		t.Error("Expected backend to be nil after abort")
	}
	if splitter.tmpFile != nil {
		t.Error("Expected tmpFile to be nil after abort")
	}
	if !splitter.closed {
		t.Error("Expected splitter to be closed after abort")
	}

	// File should be removed
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Error("Expected temp file to be removed after abort")
	}
}

func TestFileSplitterAbort_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Multiple aborts should be safe
	splitter.Abort()
	splitter.Abort()
	splitter.Abort()

	// Should still be closed
	if !splitter.closed {
		t.Error("Expected splitter to remain closed after multiple aborts")
	}
}

func TestFileSplitterEmptyFileHandling(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 1,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Write one batch
	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Write empty batch (should not create new file)
	emptyBatch := createEmptyBatch(t)
	err = splitter.WriteBatchRows(ctx, emptyBatch)
	if err != nil {
		t.Fatalf("WriteBatchRows with empty batch failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should only have one file (empty batch shouldn't create a file)
	if len(results) != 1 {
		t.Errorf("Expected 1 result file, got %d", len(results))
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitterTempFileCreation(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check that result file is in the correct directory and has correct prefix
	if len(results) != 1 {
		t.Fatalf("Expected 1 result file, got %d", len(results))
	}

	fileName := results[0].FileName
	if filepath.Dir(fileName) != tmpDir {
		t.Errorf("Expected file to be in %s, got %s", tmpDir, filepath.Dir(fileName))
	}
	baseName := filepath.Base(fileName)
	if !strings.HasSuffix(baseName, ".parquet") {
		t.Errorf("Expected filename to end with '.parquet', got %s", baseName)
	}

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

// TestStringConversionForPrefixedFields tests that fields with configured prefixes
// are converted to strings to avoid schema conflicts
//
// NOTE: String conversion logic has moved to the backend implementations (GoParquetBackend, ArrowBackend).
// The unit tests for internal conversion methods have been removed since they're implementation details.
// The end-to-end batch processing test below verifies the behavior still works correctly.
func TestStringConversionForPrefixedFields(t *testing.T) {
	tmpDir := t.TempDir()

	// Internal conversion methods are now in backends - those unit tests removed

	// Test with default prefixes - REMOVED (methods moved to backends)
	// t.Run("DefaultPrefixes", func(t *testing.T) { ... })

	// Test with custom prefixes - REMOVED (methods moved to backends)
	// t.Run("CustomPrefixes", func(t *testing.T) { ... })

	// Test actual batch processing with mixed types
	t.Run("BatchProcessingWithConversion", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         tmpDir,
			Schema:         testSplitterSchema(),
			RecordsPerFile: 100,
		}

		splitter := NewFileSplitter(config)
		ctx := context.Background()

		// Create a batch with fields that need conversion
		batch := pipeline.GetBatch()

		// First row has resource.id as int64
		row1 := batch.AddRow()
		row1[wkk.NewRowKey("resource_id")] = int64(12345)
		row1[wkk.NewRowKey("resource_name")] = "service-a"
		row1[wkk.NewRowKey("attr_value")] = float64(99.5)
		row1[wkk.NewRowKey("regular_field")] = int64(777)

		// Second row has resource_id as string (simulating type conflict)
		row2 := batch.AddRow()
		row2[wkk.NewRowKey("resource_id")] = "67890"
		row2[wkk.NewRowKey("resource_name")] = "service-b"
		row2[wkk.NewRowKey("attr_value")] = int64(100)
		row2[wkk.NewRowKey("regular_field")] = int64(888)

		// Write batch - should not error despite type mismatch
		err := splitter.WriteBatchRows(ctx, batch)
		require.NoError(t, err, "WriteBatchRows should handle type conversion")

		// Close and verify results
		results, err := splitter.Close(ctx)
		require.NoError(t, err, "Close should succeed")
		require.Len(t, results, 1, "Should have one result file")

		// Clean up
		for _, result := range results {
			_ = os.Remove(result.FileName)
		}

		pipeline.ReturnBatch(batch)
	})
}

// Helper functions

func createTestBatch(t *testing.T, numRows int) *pipeline.Batch {
	t.Helper()
	batch := pipeline.GetBatch()

	for i := range numRows {
		row := batch.AddRow()
		row[wkk.NewRowKey("field1")] = "value" + string(rune('0'+i%10))
		row[wkk.NewRowKey("field2")] = int64(i * 10)
		row[wkk.NewRowKey("timestamp")] = int64(1000000000 + i)
	}

	return batch
}

func createTestBatchWithGroups(t *testing.T, groups []string) *pipeline.Batch {
	t.Helper()
	batch := pipeline.GetBatch()

	for i, group := range groups {
		row := batch.AddRow()
		row[wkk.NewRowKey("field1")] = "value" + string(rune('0'+i%10))
		row[wkk.NewRowKey("field2")] = int64(i * 10)
		row[wkk.NewRowKey("group")] = group
	}

	return batch
}

func createEmptyBatch(t *testing.T) *pipeline.Batch {
	t.Helper()
	return pipeline.GetBatch()
}

func TestFileSplitter_FingerprintStringToInt64Conversion(t *testing.T) {
	tmpDir := t.TempDir()

	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: NoRecordLimitPerFile,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Test 1: Fingerprint as string should be converted to int64
	batch1 := pipeline.GetBatch()
	row1 := batch1.AddRow()
	row1[wkk.NewRowKey("chq_fingerprint")] = "7754623969787599908" // String fingerprint
	row1[wkk.NewRowKey("message")] = "test log message"
	row1[wkk.NewRowKey("resource_service_name")] = "test-service"

	err := splitter.WriteBatchRows(ctx, batch1)
	require.NoError(t, err, "Should handle fingerprint string conversion")

	// Test 2: Fingerprint as int64 should remain int64
	batch2 := pipeline.GetBatch()
	row2 := batch2.AddRow()
	row2[wkk.NewRowKey("chq_fingerprint")] = int64(7754623969787599908) // int64 fingerprint
	row2[wkk.NewRowKey("message")] = "another test message"
	row2[wkk.NewRowKey("resource_service_name")] = "test-service"

	err = splitter.WriteBatchRows(ctx, batch2)
	require.NoError(t, err, "Should handle int64 fingerprint directly")

	// Close and verify results
	results, err := splitter.Close(ctx)
	require.NoError(t, err)
	require.Len(t, results, 1, "Should have one output file")
	require.Equal(t, int64(2), results[0].RecordCount, "Should have 2 records")

	// Clean up
	for _, result := range results {
		_ = os.Remove(result.FileName)
	}
}

func TestFileSplitter_FingerprintConversionErrors(t *testing.T) {
	tmpDir := t.TempDir()

	config := WriterConfig{
		TmpDir:         tmpDir,
		Schema:         testSplitterSchema(),
		RecordsPerFile: NoRecordLimitPerFile,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Test invalid string fingerprint
	batch := pipeline.GetBatch()
	row := batch.AddRow()
	row[wkk.NewRowKey("chq_fingerprint")] = "not-a-number" // Invalid string
	row[wkk.NewRowKey("message")] = "test message"

	err := splitter.WriteBatchRows(ctx, batch)
	require.Error(t, err, "Should error on invalid fingerprint string")
	require.Contains(t, err.Error(), "failed to parse fingerprint")

	splitter.Abort()
}
