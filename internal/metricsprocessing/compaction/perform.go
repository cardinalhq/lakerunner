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
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// perform executes pure compaction logic on local files
func perform(ctx context.Context, input input) (*result, error) {
	ll := input.Logger

	if len(input.ReaderStack.Readers) == 0 {
		ll.Debug("No files to compact, returning empty result")
		return &result{}, nil
	}

	// Wrap with aggregating reader to merge duplicates during compaction
	ll.Debug("Creating aggregating metrics reader", slog.Int("frequencyMs", int(input.FrequencyMs)))
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(input.ReaderStack.FinalReader, int64(input.FrequencyMs), 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return nil, fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	writer, err := factories.NewMetricsWriter(input.TmpDir, input.RecordsLimit)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return nil, fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

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

		for i := 0; i < batch.Len(); i++ {
			totalRows++
		}

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

	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return nil, fmt.Errorf("finishing writer: %w", err)
	}

	// Calculate sizes
	outputBytes := int64(0)
	for _, result := range results {
		outputBytes += result.FileSize
	}

	return &result{
		Results:     results,
		TotalRows:   totalRows,
		OutputBytes: outputBytes,
	}, nil
}
