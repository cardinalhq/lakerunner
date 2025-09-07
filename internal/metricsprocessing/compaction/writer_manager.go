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

package compaction

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// writerInstance represents a single parquet writer instance
type writerInstance struct {
	writer      *parquetwriter.UnifiedWriter
	recordCount int64
	startTs     int64
	endTs       int64
}

// CompactionWriterManager manages parquet writers for compaction
type CompactionWriterManager struct {
	writers              []*writerInstance
	currentWriter        *writerInstance
	tmpDir               string
	targetRecordsPerFile int64
	maxRecordsPerFile    int64
}

// NewCompactionWriterManager creates a new writer manager for compaction
func NewCompactionWriterManager(tmpDir string, targetRecordsPerFile int64) *CompactionWriterManager {
	return &CompactionWriterManager{
		writers:              make([]*writerInstance, 0),
		tmpDir:               tmpDir,
		targetRecordsPerFile: targetRecordsPerFile,
		maxRecordsPerFile:    targetRecordsPerFile * 2, // Allow up to 2x target before forcing rotation
	}
}

// ProcessReaders processes a stack of readers through the writer manager
func (m *CompactionWriterManager) ProcessReaders(
	ctx context.Context,
	readerStack []filereader.Reader,
	metadata CompactionWorkMetadata,
) error {
	// Use the existing metricsprocessing functions to create the reader pipeline
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	mergeReader, err := filereader.NewMergesortReader(ctx, readerStack, keyProvider, 1000)
	if err != nil {
		return fmt.Errorf("creating mergesort reader: %w", err)
	}
	defer mergeReader.Close()

	// Add aggregation
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(mergeReader, int64(metadata.FrequencyMs), 1000)
	if err != nil {
		return fmt.Errorf("creating aggregating reader: %w", err)
	}
	defer aggregatingReader.Close()

	// Process all rows through the writer manager
	for {
		batch, readErr := aggregatingReader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return fmt.Errorf("reading from pipeline: %w", readErr)
		}

		if batch != nil {
			if err := m.processBatch(ctx, batch); err != nil {
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("processing batch: %w", err)
			}
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	return nil
}

// processBatch processes a batch of rows
func (m *CompactionWriterManager) processBatch(ctx context.Context, batch *pipeline.Batch) error {
	ll := logctx.FromContext(ctx)

	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			ll.Error("Row is nil - skipping", slog.Int("rowIndex", i))
			continue
		}

		// Extract timestamp for tracking
		ts, ok := row[wkk.RowKeyCTimestamp].(int64)
		if !ok {
			ll.Error("_cardinalhq.timestamp field is missing or not int64 - skipping row", slog.Int("rowIndex", i))
			continue
		}

		// Get or create writer
		writer, err := m.getOrCreateWriter(ctx)
		if err != nil {
			return fmt.Errorf("getting writer: %w", err)
		}

		// Create a single-row batch for the writer
		singleBatch := pipeline.GetBatch()
		newRow := singleBatch.AddRow()
		for k, v := range row {
			newRow[k] = v
		}

		// Write the row
		if err := writer.writer.WriteBatch(singleBatch); err != nil {
			pipeline.ReturnBatch(singleBatch)
			return fmt.Errorf("writing row: %w", err)
		}
		pipeline.ReturnBatch(singleBatch)

		// Update writer metadata
		writer.recordCount++
		if writer.startTs == 0 || ts < writer.startTs {
			writer.startTs = ts
		}
		if ts > writer.endTs {
			writer.endTs = ts
		}

		// Check if we should rotate to a new writer
		if writer.recordCount >= m.targetRecordsPerFile {
			m.rotateWriter()
		}
	}

	return nil
}

// getOrCreateWriter gets the current writer or creates a new one
func (m *CompactionWriterManager) getOrCreateWriter(ctx context.Context) (*writerInstance, error) {
	if m.currentWriter != nil && m.currentWriter.recordCount < m.maxRecordsPerFile {
		return m.currentWriter, nil
	}

	// Create new writer
	writer, err := factories.NewMetricsWriter(m.tmpDir, m.targetRecordsPerFile)
	if err != nil {
		return nil, fmt.Errorf("creating metrics writer: %w", err)
	}

	instance := &writerInstance{
		writer: writer,
	}

	m.currentWriter = instance
	m.writers = append(m.writers, instance)

	return instance, nil
}

// rotateWriter forces rotation to a new writer
func (m *CompactionWriterManager) rotateWriter() {
	m.currentWriter = nil
}

// FlushAll closes all writers and returns the results
func (m *CompactionWriterManager) FlushAll(ctx context.Context) ([]metricsprocessing.ProcessingResult, error) {
	ll := logctx.FromContext(ctx)

	var allRawResults []parquetwriter.Result
	totalRecords := int64(0)
	totalBytes := int64(0)

	for i, instance := range m.writers {
		results, err := instance.writer.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("closing writer %d: %w", i, err)
		}

		for _, result := range results {
			allRawResults = append(allRawResults, result)
			totalRecords += result.RecordCount
			totalBytes += result.FileSize
		}
	}

	ll.Info("Flushed all compaction writers",
		slog.Int("writerCount", len(m.writers)),
		slog.Int("fileCount", len(allRawResults)),
		slog.Int64("totalRecords", totalRecords),
		slog.Int64("totalBytes", totalBytes))

	// Return as ProcessingResult format
	result := metricsprocessing.ProcessingResult{
		RawResults: allRawResults,
		Stats: metricsprocessing.ProcessingStats{
			OutputSegments: len(allRawResults),
			OutputRecords:  totalRecords,
			OutputBytes:    totalBytes,
		},
	}

	return []metricsprocessing.ProcessingResult{result}, nil
}

// Close closes all writers and cleans up resources
func (m *CompactionWriterManager) Close(ctx context.Context) error {
	for _, instance := range m.writers {
		if _, err := instance.writer.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
