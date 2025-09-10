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
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// logProcessingParams contains the parameters needed for log processing
type logProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.LogSeg
	MaxRecords     int64
}

// logProcessingResult contains the results of log processing
type logProcessingResult struct {
	ProcessedSegments []lrdb.LogSeg
	Results           []parquetwriter.Result
}

// processLogsWithSorting performs the common pattern of:
// 1. Create reader stack from segments
// 2. Create logs writer (logs don't need aggregation, just sorting)
// 3. Write from reader to writer
// 4. Close writer and return results
func processLogsWithSorting(ctx context.Context, params logProcessingParams) (*logProcessingResult, error) {
	// Create reader stack
	readerStack, err := createLogReaderStack(
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

	// Create logs writer (no aggregation needed, just sorting)
	writer, err := factories.NewLogsWriter(params.TmpDir, params.MaxRecords)
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	// Write from reader to writer
	if err := writeFromReader(ctx, readerStack.MergedReader, writer); err != nil {
		writer.Abort()
		return nil, fmt.Errorf("write from reader: %w", err)
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return &logProcessingResult{
		ProcessedSegments: readerStack.ProcessedSegments,
		Results:           results,
	}, nil
}
