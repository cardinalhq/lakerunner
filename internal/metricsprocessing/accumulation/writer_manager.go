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

package accumulation

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

// WriterManager manages parquet writers for accumulation
type WriterManager struct {
	writer               *parquetwriter.UnifiedWriter
	tmpDir               string
	targetRecordsPerFile int64
}

// NewWriterManager creates a new writer manager
func NewWriterManager(tmpDir string, targetRecordsPerFile int64) *WriterManager {
	return &WriterManager{
		tmpDir:               tmpDir,
		targetRecordsPerFile: targetRecordsPerFile,
	}
}

// ProcessReaders processes readers through the writer with a specific target frequency
func (wm *WriterManager) ProcessReaders(ctx context.Context, readers []filereader.Reader, key AccumulationKey, targetFrequency int32) error {
	// Use the existing metricsprocessing functions to create the reader pipeline
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	mergeReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	if err != nil {
		return fmt.Errorf("creating mergesort reader: %w", err)
	}
	defer mergeReader.Close()

	// Add aggregation
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(mergeReader, int64(targetFrequency), 1000)
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
			if err := wm.processBatch(ctx, batch); err != nil {
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
func (wm *WriterManager) processBatch(ctx context.Context, batch *pipeline.Batch) error {
	if err := wm.ensureWriter(ctx); err != nil {
		return fmt.Errorf("ensuring writer: %w", err)
	}

	if err := wm.writer.WriteBatch(batch); err != nil {
		return fmt.Errorf("writing batch: %w", err)
	}

	return nil
}

// ensureWriter creates the writer if it doesn't exist
func (wm *WriterManager) ensureWriter(_ context.Context) error {
	if wm.writer != nil {
		return nil
	}

	// Create writer with the target records per file estimate
	writer, err := factories.NewMetricsWriter(wm.tmpDir, wm.targetRecordsPerFile)
	if err != nil {
		return fmt.Errorf("creating metrics writer: %w", err)
	}

	wm.writer = writer
	return nil
}

// FlushAll flushes all writers and returns the processing result
func (wm *WriterManager) FlushAll(ctx context.Context) (metricsprocessing.ProcessingResult, error) {
	ll := logctx.FromContext(ctx)

	if wm.writer == nil {
		// No writer was created, return empty results
		return metricsprocessing.ProcessingResult{}, nil
	}

	// Close the writer and get all the files it created
	results, err := wm.writer.Close(ctx)
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

	ll.Info("Flushed accumulation writer",
		slog.Int("fileCount", len(results)),
		slog.Int64("totalRecords", totalRecords),
		slog.Int64("totalBytes", totalBytes))

	// Clear writer reference
	wm.writer = nil

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
