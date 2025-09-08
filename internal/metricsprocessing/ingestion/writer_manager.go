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

package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// writerKey identifies a unique writer for a specific org/instance/dateint/minute combination
type writerKey struct {
	orgID       string
	instanceNum int16
	dateint     int32
	minute      int
}

// metricWriterManager manages parquet writers for metrics
type metricWriterManager struct {
	writers       map[writerKey]parquetwriter.ParquetWriter
	tmpdir        string
	orgID         string
	instanceNum   int16
	ingestDateint int32
	rpfEstimate   int64
}

func newMetricWriterManager(ctx context.Context, tmpdir, orgID string, instanceNum int16, ingestDateint int32, rpfEstimate int64) *metricWriterManager {
	return &metricWriterManager{
		writers:       make(map[writerKey]parquetwriter.ParquetWriter),
		tmpdir:        tmpdir,
		orgID:         orgID,
		instanceNum:   instanceNum,
		ingestDateint: ingestDateint,
		rpfEstimate:   rpfEstimate,
	}
}

// processBatch efficiently processes an entire batch, grouping rows by org/instance/dateint/minute
func (wm *metricWriterManager) processBatch(ctx context.Context, batch *pipeline.Batch) (processedCount, errorCount int64) {
	ll := logctx.FromContext(ctx)
	// Group rows by writer key to minimize writer lookups and enable batch writes
	batchGroups := make(map[writerKey]*pipeline.Batch)

	// First pass: group rows by minute boundary
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			ll.Error("Row is nil - skipping", slog.Int("rowIndex", i))
			errorCount++
			continue
		}

		// Extract timestamp
		ts, ok := row[wkk.RowKeyCTimestamp].(int64)
		if !ok {
			ll.Error("_cardinalhq.timestamp field is missing or not int64 - skipping row", slog.Int("rowIndex", i))
			errorCount++
			continue
		}

		// Calculate minute boundary
		dateint, minute := wm.timestampToMinuteBoundary(ts)
		// Use the manager's org_id and instance_num for the key
		// (all rows in a batch should belong to the same org/instance)
		key := writerKey{
			orgID:       wm.orgID,
			instanceNum: wm.instanceNum,
			dateint:     dateint,
			minute:      minute,
		}

		// Create or get batch for this group
		if batchGroups[key] == nil {
			batchGroups[key] = pipeline.GetBatch()
		}

		// Add row to the appropriate batch group
		newRow := batchGroups[key].AddRow()
		for k, v := range row {
			newRow[k] = v
		}
	}

	// Second pass: write each grouped batch to its writer
	for key, groupedBatch := range batchGroups {
		writer, err := wm.getWriter(key)
		if err != nil {
			ll.Error("Failed to get writer for batch group",
				slog.Any("key", key),
				slog.String("error", err.Error()))
			errorCount += int64(groupedBatch.Len())
			pipeline.ReturnBatch(groupedBatch)
			continue
		}

		// Write the entire batch efficiently
		if err := writer.WriteBatch(groupedBatch); err != nil {
			ll.Error("Failed to write batch group",
				slog.Any("key", key),
				slog.Int("batchSize", groupedBatch.Len()),
				slog.String("error", err.Error()))
			errorCount += int64(groupedBatch.Len())
		} else {
			processedCount += int64(groupedBatch.Len())
		}

		// Return batch to pool
		pipeline.ReturnBatch(groupedBatch)
	}

	return processedCount, errorCount
}

// timestampToMinuteBoundary converts a timestamp to dateint and minute boundary
func (wm *metricWriterManager) timestampToMinuteBoundary(ts int64) (int32, int) {
	t := time.Unix(ts/1000, (ts%1000)*1000000).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	minute := t.Hour()*60 + t.Minute()
	return dateint, minute
}

// getWriter returns the writer for a specific writer key, creating it if necessary
func (wm *metricWriterManager) getWriter(key writerKey) (parquetwriter.ParquetWriter, error) {
	if writer, exists := wm.writers[key]; exists {
		return writer, nil
	}

	writer, err := factories.NewMetricsWriter(
		wm.tmpdir,
		wm.rpfEstimate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics writer: %w", err)
	}

	wm.writers[key] = writer
	return writer, nil
}

// closeAll closes all writers and returns results
func (wm *metricWriterManager) closeAll(ctx context.Context) ([]parquetwriter.Result, error) {
	var allResults []parquetwriter.Result

	for key, writer := range wm.writers {
		results, err := writer.Close(ctx)
		if err != nil {
			return allResults, fmt.Errorf("failed to close writer %v: %w", key, err)
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}
