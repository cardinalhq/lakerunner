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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricProcessingParams contains the parameters needed for metric processing
type MetricProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.MetricSeg
	FrequencyMs    int32
	MaxRecords     int64
}

// MetricProcessingResult contains the results of metric processing
type MetricProcessingResult struct {
	ProcessedSegments []lrdb.MetricSeg
	Results           []parquetwriter.Result
}

// ProcessMetricsWithAggregation performs the common pattern of:
// 1. Create reader stack from segments
// 2. Create aggregating reader from head reader
// 3. Create metrics writer
// 4. Write from reader to writer
// 5. Close writer and return results
func ProcessMetricsWithAggregation(ctx context.Context, params MetricProcessingParams) (*MetricProcessingResult, error) {
	// Create reader stack
	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx,
		params.TmpDir,
		params.StorageClient,
		params.OrganizationID,
		params.StorageProfile,
		params.ActiveSegments,
	)
	if err != nil {
		return nil, fmt.Errorf("create reader stack: %w", err)
	}
	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	// Create aggregating reader from head reader
	aggReader, err := filereader.NewAggregatingMetricsReader(
		readerStack.HeadReader,
		int64(params.FrequencyMs),
		1000,
	)
	if err != nil {
		return nil, fmt.Errorf("create aggregating reader: %w", err)
	}
	defer aggReader.Close()

	// Create metrics writer
	writer, err := factories.NewMetricsWriter(params.TmpDir, params.MaxRecords)
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	// Write from reader to writer
	if err := WriteFromReader(ctx, aggReader, writer); err != nil {
		writer.Abort()
		return nil, fmt.Errorf("write from reader: %w", err)
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return &MetricProcessingResult{
		ProcessedSegments: readerStack.ProcessedSegments,
		Results:           results,
	}, nil
}

// WriteFromReader writes data from a reader to a writer - shared utility function
func WriteFromReader(ctx context.Context, reader filereader.Reader, writer parquetwriter.ParquetWriter) error {
	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read batch: %w", err)
		}

		if err := writer.WriteBatch(batch); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
	}
	return nil
}
