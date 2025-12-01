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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// metricProcessingParams contains the parameters needed for metric processing
type metricProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.MetricSeg
	FrequencyMs    int32
	MaxRecords     int64
}

// processMetricsWithAggregation performs the common pattern of:
// 1. Create reader stack from segments
// 2. Create aggregating reader from head reader
// 3. Create metrics writer
// 4. Write from reader to writer
// 5. Close writer and return results
func processMetricsWithAggregation(ctx context.Context, params metricProcessingParams) ([]parquetwriter.Result, error) {
	// Create reader stack
	readerStack, err := createMetricReaderStack(
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
	defer readerStack.Close(ctx)

	// Create aggregating reader from head reader
	aggReader, err := filereader.NewAggregatingMetricsReader(
		readerStack.HeadReader,
		int64(params.FrequencyMs),
		1000,
	)
	if err != nil {
		return nil, fmt.Errorf("create aggregating reader: %w", err)
	}
	defer func() { _ = aggReader.Close() }()

	// Get schema from aggregating reader
	schema := aggReader.GetSchema()

	// Create metrics writer
	writer, err := factories.NewMetricsWriter(params.TmpDir, schema, params.MaxRecords)
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	// Write from reader to writer
	if err := writeFromReader(ctx, aggReader, writer); err != nil {
		writer.Abort()
		return nil, fmt.Errorf("write from reader: %w", err)
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return results, nil
}
