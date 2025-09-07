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

package metricsprocessing

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// Test helper to load test data files
func loadTestDataFiles(t *testing.T) ([]filereader.Reader, []*os.File) {
	testDataDir := filepath.Join("..", "..", "testdata", "metrics", "compact-test-0001")
	files, err := os.ReadDir(testDataDir)
	require.NoError(t, err, "Failed to read test data directory")

	var readers []filereader.Reader
	var openFiles []*os.File

	for _, file := range files {
		if filepath.Ext(file.Name()) != ".parquet" {
			continue
		}

		filePath := filepath.Join(testDataDir, file.Name())
		f, err := os.Open(filePath)
		require.NoError(t, err, "Failed to open test file: %s", file.Name())

		stat, err := f.Stat()
		require.NoError(t, err, "Failed to stat test file: %s", file.Name())

		rawReader, err := filereader.NewParquetRawReader(f, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader for: %s", file.Name())

		// Wrap with CookedMetricTranslatingReader for metric files
		reader := filereader.NewCookedMetricTranslatingReader(rawReader)

		readers = append(readers, reader)
		openFiles = append(openFiles, f)
	}

	require.NotEmpty(t, readers, "No test data files found")
	return readers, openFiles
}

// Test compaction with real test data
func TestAggregateMetrics_Compaction_RealData(t *testing.T) {
	ctx := context.Background()

	// Load test data files
	readers, files := loadTestDataFiles(t)
	defer func() {
		for _, f := range files {
			f.Close()
		}
		for _, r := range readers {
			r.Close()
		}
	}()

	// Create merged reader for multiple files
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergedReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err)
	defer mergedReader.Close()

	// Create temp directory for output
	tmpDir := t.TempDir()

	// Create reader stack result
	readerStack := &ReaderStackResult{
		Readers:    readers,
		HeadReader: mergedReader,
		Files:      files,
	}

	// Run aggregation at 10s (same as source) for compaction
	input := ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: 10000, // 10s - same as source for compaction
		TmpDir:            tmpDir,
		RecordsLimit:      100000,
		EstimatedRecords:  50000,
		Action:            "compact",
		InputRecords:      0, // Will be calculated
		InputBytes:        0, // Will be calculated
	}

	// First pass: count input records
	// We need to recreate readers since we'll consume them
	countReaders, countFiles := loadTestDataFiles(t)
	inputRecordCount := int64(0)
	for _, reader := range countReaders {
		testCtx := context.Background()
		for {
			batch, err := reader.Next(testCtx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			inputRecordCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}
	}
	input.InputRecords = inputRecordCount

	// Close count readers
	for _, r := range countReaders {
		r.Close()
	}
	for _, f := range countFiles {
		f.Close()
	}

	// Recreate readers for actual test
	readers, files = loadTestDataFiles(t)
	defer func() {
		for _, f := range files {
			f.Close()
		}
		for _, r := range readers {
			r.Close()
		}
	}()
	mergedReader, err = filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err)
	defer mergedReader.Close()
	readerStack.HeadReader = mergedReader

	// Execute aggregation
	result, err := AggregateMetrics(ctx, input)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify results
	assert.NotEmpty(t, result.RawResults, "Should have output files")
	assert.Greater(t, result.Stats.OutputRecords, int64(0), "Should have output records")
	assert.Greater(t, result.Stats.TotalRows, int64(0), "Should have processed rows")
	assert.Greater(t, result.Stats.BatchCount, 0, "Should have processed batches")

	// For compaction at same frequency, we expect deduplication
	// The output should have fewer or equal records than input
	assert.LessOrEqual(t, result.Stats.OutputRecords, inputRecordCount,
		"Compaction should reduce or maintain record count through deduplication")

	// Verify output files were created
	for _, res := range result.RawResults {
		assert.FileExists(t, res.FileName)
		assert.Greater(t, res.FileSize, int64(0), "Output file should have content")
		assert.Greater(t, res.RecordCount, int64(0), "Output file should have records")
	}

	// Read back one output file to verify content structure
	if len(result.RawResults) > 0 {
		outputFile := result.RawResults[0].FileName
		f, err := os.Open(outputFile)
		require.NoError(t, err)
		defer f.Close()

		stat, err := f.Stat()
		require.NoError(t, err)

		reader, err := filereader.NewParquetRawReader(f, stat.Size(), 1000)
		require.NoError(t, err)
		defer reader.Close()

		// Read first batch to verify structure
		batch, err := reader.Next(ctx)
		require.NoError(t, err)
		require.Greater(t, batch.Len(), 0)

		// Check that key fields exist
		row := batch.Get(0)
		_, hasName := row[wkk.RowKeyCName]
		_, hasTimestamp := row[wkk.RowKeyCTimestamp]
		_, hasTID := row[wkk.RowKeyCTID]

		assert.True(t, hasName, "Output should have metric name")
		assert.True(t, hasTimestamp, "Output should have timestamp")
		assert.True(t, hasTID, "Output should have TID")

		pipeline.ReturnBatch(batch)
	}
}

// Note: We don't need a separate rollup test here since the only difference
// from compaction is the target frequency. The aggregation logic itself is
// tested in the AggregatingMetricsReader tests. The compaction test above
// validates the overall pipeline works correctly.

// Test with empty input
func TestAggregateMetrics_EmptyInput(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	input := ProcessingInput{
		ReaderStack:       nil,
		TargetFrequencyMs: 10000,
		TmpDir:            tmpDir,
		RecordsLimit:      100000,
		EstimatedRecords:  0,
		Action:            "compact",
		InputRecords:      0,
		InputBytes:        0,
	}

	result, err := AggregateMetrics(ctx, input)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.RawResults)
	assert.Equal(t, int64(0), result.Stats.OutputRecords)
	assert.Equal(t, int64(0), result.Stats.TotalRows)
	assert.Equal(t, 0, result.Stats.BatchCount)
}

// Test batch processing with mock reader
func TestAggregateMetrics_BatchProcessing(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a mock reader that returns specific number of batches
	mockReader := &mockBatchReader{
		batchCount:   5,
		rowsPerBatch: 100,
	}

	readerStack := &ReaderStackResult{
		Readers:    []filereader.Reader{mockReader},
		HeadReader: mockReader,
	}

	input := ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: 10000,
		TmpDir:            tmpDir,
		RecordsLimit:      100000,
		EstimatedRecords:  500,
		Action:            "compact",
		InputRecords:      500,
		InputBytes:        50000,
	}

	result, err := AggregateMetrics(ctx, input)
	require.NoError(t, err)
	require.NotNil(t, result)

	// BatchCount is the number of batches from the AggregatingMetricsReader,
	// which aggregates the input batches, so it will be less than the original 5
	assert.Greater(t, result.Stats.BatchCount, 0)
	assert.LessOrEqual(t, result.Stats.BatchCount, 5)
	// TotalRows should match the input (may be slightly different due to aggregation)
	assert.Greater(t, result.Stats.TotalRows, int64(0))
}

// Mock reader for testing batch processing
type mockBatchReader struct {
	batchCount   int
	rowsPerBatch int
	currentBatch int
}

func (m *mockBatchReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if m.currentBatch >= m.batchCount {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()
	for i := 0; i < m.rowsPerBatch; i++ {
		row := batch.AddRow()
		// Add minimal required fields for metrics
		row[wkk.RowKeyCName] = fmt.Sprintf("test.metric.%d", i)
		row[wkk.RowKeyCTID] = int64(i)
		row[wkk.RowKeyCTimestamp] = int64(10000 * (m.currentBatch + 1))

		// Create a sketch for the value
		sketch, _ := ddsketch.NewDefaultDDSketch(0.01)
		_ = sketch.Add(float64(i))
		row[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)

		row[wkk.RowKeyRollupSum] = float64(i)
		row[wkk.RowKeyRollupCount] = 1.0
		row[wkk.RowKeyRollupMin] = float64(i)
		row[wkk.RowKeyRollupMax] = float64(i)
		row[wkk.RowKeyRollupAvg] = float64(i)
	}

	m.currentBatch++
	return batch, nil
}

func (m *mockBatchReader) Close() error {
	return nil
}

func (m *mockBatchReader) TotalRowsReturned() int64 {
	return int64(m.currentBatch * m.rowsPerBatch)
}

// Test error handling during batch processing
func TestAggregateMetrics_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a reader that returns an error
	errorReader := &errorBatchReader{
		errorAfter: 2,
	}

	readerStack := &ReaderStackResult{
		Readers:    []filereader.Reader{errorReader},
		HeadReader: errorReader,
	}

	input := ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: 10000,
		TmpDir:            tmpDir,
		RecordsLimit:      100000,
		EstimatedRecords:  500,
		Action:            "compact",
		InputRecords:      500,
		InputBytes:        50000,
	}

	result, err := AggregateMetrics(ctx, input)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "reading from aggregating reader")
}

// Error reader for testing error handling
type errorBatchReader struct {
	batchCount int
	errorAfter int
}

func (e *errorBatchReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if e.batchCount >= e.errorAfter {
		return nil, fmt.Errorf("simulated read error")
	}

	batch := pipeline.GetBatch()
	row := batch.AddRow()
	row[wkk.RowKeyCName] = "test.metric"
	row[wkk.RowKeyCTID] = int64(1)
	row[wkk.RowKeyCTimestamp] = int64(10000)

	// Create a sketch for the value
	sketch, _ := ddsketch.NewDefaultDDSketch(0.01)
	_ = sketch.Add(1.0)
	row[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)

	row[wkk.RowKeyRollupSum] = 1.0
	row[wkk.RowKeyRollupCount] = 1.0

	e.batchCount++
	return batch, nil
}

func (e *errorBatchReader) Close() error {
	return nil
}

func (e *errorBatchReader) TotalRowsReturned() int64 {
	return int64(e.batchCount)
}

// Benchmark aggregation with real data
func BenchmarkAggregateMetrics_RealData(b *testing.B) {
	ctx := context.Background()

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		// Setup
		readers, files := loadTestDataFiles(&testing.T{})
		keyProvider := GetCurrentMetricSortKeyProvider()
		mergedReader, _ := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		tmpDir := b.TempDir()

		readerStack := &ReaderStackResult{
			Readers:    readers,
			HeadReader: mergedReader,
		}

		input := ProcessingInput{
			ReaderStack:       readerStack,
			TargetFrequencyMs: 10000,
			TmpDir:            tmpDir,
			RecordsLimit:      100000,
			EstimatedRecords:  50000,
			Action:            "compact",
		}

		b.StartTimer()

		// Benchmark the aggregation
		_, err := AggregateMetrics(ctx, input)
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()

		// Cleanup
		mergedReader.Close()
		for _, r := range readers {
			r.Close()
		}
		for _, f := range files {
			f.Close()
		}
	}
}
