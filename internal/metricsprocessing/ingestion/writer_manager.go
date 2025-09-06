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

// metricWriterManager manages parquet writers for metrics
type metricWriterManager struct {
	writers       map[minuteSlotKey]*parquetwriter.UnifiedWriter
	tmpdir        string
	orgID         string
	ingestDateint int32
	rpfEstimate   int64
}

func newMetricWriterManager(ctx context.Context, tmpdir, orgID string, ingestDateint int32, rpfEstimate int64) *metricWriterManager {
	return &metricWriterManager{
		writers:       make(map[minuteSlotKey]*parquetwriter.UnifiedWriter),
		tmpdir:        tmpdir,
		orgID:         orgID,
		ingestDateint: ingestDateint,
		rpfEstimate:   rpfEstimate,
	}
}

// processBatch efficiently processes an entire batch, grouping rows by minute boundary
func (wm *metricWriterManager) processBatch(ctx context.Context, batch *pipeline.Batch) (processedCount, errorCount int64) {
	ll := logctx.FromContext(ctx)
	// Group rows by minute boundary to minimize writer lookups and enable batch writes
	batchGroups := make(map[minuteSlotKey]*pipeline.Batch)

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
		slot := 0
		key := minuteSlotKey{dateint, minute, slot}

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

// getWriter returns the writer for a specific minute boundary, creating it if necessary
func (wm *metricWriterManager) getWriter(key minuteSlotKey) (*parquetwriter.UnifiedWriter, error) {
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
