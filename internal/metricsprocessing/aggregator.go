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
	"errors"
	"fmt"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// AggregateMetrics is the generic aggregation pipeline for both compaction and rollup
func AggregateMetrics(ctx context.Context, input ProcessingInput) (*ProcessingResult, error) {
	ll := logctx.FromContext(ctx)

	// Validate input
	if input.ReaderStack == nil || len(input.ReaderStack.Readers) == 0 {
		ll.Debug("No files to process, returning empty result")
		return &ProcessingResult{
			Segments: ProcessedSegments{},
			Stats:    ProcessingStats{},
		}, nil
	}

	// Track input metrics
	processingSegmentsIn.Add(ctx, int64(len(input.ReaderStack.Readers)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	processingRecordsIn.Add(ctx, input.InputRecords, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	processingBytesIn.Add(ctx, input.InputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	// Create aggregating reader with target frequency
	ll.Debug("Creating aggregating metrics reader",
		slog.Int("targetFrequencyMs", int(input.TargetFrequencyMs)),
		slog.String("action", input.Action))

	aggregatingReader, err := filereader.NewAggregatingMetricsReader(
		input.ReaderStack.HeadReader,
		int64(input.TargetFrequencyMs),
		1000,
	)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return nil, fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	// Determine records limit for writer
	recordsLimit := input.EstimatedRecords
	if input.RecordsLimit > 0 {
		recordsLimit = input.RecordsLimit
	}

	// Create metrics writer
	writer, err := factories.NewMetricsWriter(input.TmpDir, recordsLimit)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return nil, fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	// Process batches
	totalRows := int64(0)
	batchCount := 0

	for {
		batch, err := aggregatingReader.Next(ctx)
		batchCount++

		if err != nil && !errors.Is(err, io.EOF) {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			ll.Error("Failed to read from aggregating reader", slog.Any("error", err))
			return nil, fmt.Errorf("reading from aggregating reader: %w", err)
		}

		if batch == nil || batch.Len() == 0 {
			pipeline.ReturnBatch(batch)
			break
		}

		totalRows += int64(batch.Len())

		if batch.Len() > 0 {
			if err := writer.WriteBatch(batch); err != nil {
				ll.Error("Failed to write batch", slog.Any("error", err))
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("writing batch: %w", err)
			}
		}

		pipeline.ReturnBatch(batch)

		if errors.Is(err, io.EOF) {
			break
		}
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return nil, fmt.Errorf("finishing writer: %w", err)
	}

	// Calculate output statistics
	outputBytes := int64(0)
	outputRecords := int64(0)
	for _, result := range results {
		outputBytes += result.FileSize
		outputRecords += result.RecordCount
	}

	// Track output metrics
	processingSegmentsOut.Add(ctx, int64(len(results)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	processingRecordsOut.Add(ctx, outputRecords, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	processingBytesOut.Add(ctx, outputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", input.Action),
	))

	// Build result with raw results for the caller to process
	return &ProcessingResult{
		RawResults: results,             // Return raw results for caller to process
		Segments:   ProcessedSegments{}, // Empty until caller processes them
		Stats: ProcessingStats{
			OutputSegments: len(results),
			OutputRecords:  outputRecords,
			OutputBytes:    outputBytes,
			TotalRows:      totalRows,
			BatchCount:     batchCount,
		},
	}, nil
}
