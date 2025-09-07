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
)

// CompactionWriterManager manages a parquet writer for compaction
type CompactionWriterManager struct {
	writer               *parquetwriter.UnifiedWriter
	tmpDir               string
	targetRecordsPerFile int64
}

// NewCompactionWriterManager creates a new writer manager for compaction
func NewCompactionWriterManager(tmpDir string, targetRecordsPerFile int64) *CompactionWriterManager {
	return &CompactionWriterManager{
		tmpDir:               tmpDir,
		targetRecordsPerFile: targetRecordsPerFile,
	}
}

// ProcessReaders processes a stack of readers through the writer manager
func (m *CompactionWriterManager) ProcessReaders(
	ctx context.Context,
	readerStack []filereader.Reader,
	key CompactionKey,
) error {
	// Use the existing metricsprocessing functions to create the reader pipeline
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	mergeReader, err := filereader.NewMergesortReader(ctx, readerStack, keyProvider, 1000)
	if err != nil {
		return fmt.Errorf("creating mergesort reader: %w", err)
	}
	defer mergeReader.Close()

	// Add aggregation
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(mergeReader, int64(key.FrequencyMs), 1000)
	if err != nil {
		return fmt.Errorf("creating aggregating reader: %w", err)
	}
	defer aggregatingReader.Close()

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
	if err := m.ensureWriter(ctx); err != nil {
		return fmt.Errorf("ensuring writer: %w", err)
	}

	if err := m.writer.WriteBatch(batch); err != nil {
		return fmt.Errorf("writing batch: %w", err)
	}

	return nil
}

// ensureWriter creates the writer if it doesn't exist
func (m *CompactionWriterManager) ensureWriter(_ context.Context) error {
	if m.writer != nil {
		return nil
	}

	// Create writer with the target records per file estimate
	writer, err := factories.NewMetricsWriter(m.tmpDir, m.targetRecordsPerFile)
	if err != nil {
		return fmt.Errorf("creating metrics writer: %w", err)
	}

	m.writer = writer
	return nil
}

// FlushAll closes the writer and returns the results
func (m *CompactionWriterManager) FlushAll(ctx context.Context) (metricsprocessing.ProcessingResult, error) {
	ll := logctx.FromContext(ctx)

	if m.writer == nil {
		// No writer was created, return empty results
		return metricsprocessing.ProcessingResult{}, nil
	}

	// Close the writer and get all the files it created
	results, err := m.writer.Close(ctx)
	if err != nil {
		return metricsprocessing.ProcessingResult{}, fmt.Errorf("closing writer: %w", err)
	}

	// Calculate totals
	var totalRecords int64
	var totalBytes int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalBytes += result.FileSize
	}

	ll.Info("Flushed compaction writer",
		slog.Int("fileCount", len(results)),
		slog.Int64("totalRecords", totalRecords),
		slog.Int64("totalBytes", totalBytes))

	// Clear writer reference
	m.writer = nil

	// Return ProcessingResult
	return metricsprocessing.ProcessingResult{
		RawResults: results,
		Stats: metricsprocessing.ProcessingStats{
			OutputSegments: len(results),
			OutputRecords:  totalRecords,
			OutputBytes:    totalBytes,
		},
	}, nil
}

// Close closes the writer and cleans up resources
func (m *CompactionWriterManager) Close(ctx context.Context) error {
	if m.writer != nil {
		if _, err := m.writer.Close(ctx); err != nil {
			return err
		}
		m.writer = nil
	}
	return nil
}
