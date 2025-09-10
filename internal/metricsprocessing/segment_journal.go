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

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// segmentJournalStore defines the interface for logging to segment_journal
type segmentJournalStore interface {
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// logSegmentOperation logs segment operations (compaction or rollup) to segment_journal for debugging
func logSegmentOperation(
	ctx context.Context,
	store segmentJournalStore,
	storageProfile storageprofile.StorageProfile,
	inputSegments, outputSegments []lrdb.MetricSeg,
	results []parquetwriter.Result,
	orgID uuid.UUID,
	collectorName string,
	dateInt int32,
	instanceNum int16,
	recordEstimate int64,
	action int16, // 2 = compact, 3 = rollup
	sourceFrequencyMs, destFrequencyMs int32,
	getHourFromTimestamp func(int64) int16,
) error {
	// Extract source object keys from input segments and calculate time bounds
	sourceObjectKeys := make([]string, len(inputSegments))
	var sourceTotalRecords, sourceTotalSize int64
	var sourceMinTimestamp, sourceMaxTimestamp int64 = int64(^uint64(0) >> 1), 0 // Initialize with max int64 and 0
	for i, seg := range inputSegments {
		sourceObjectKeys[i] = helpers.MakeDBObjectID(orgID, collectorName, dateInt, getHourFromTimestamp(seg.TsRange.Lower.Int64), seg.SegmentID, "metrics")
		sourceTotalRecords += seg.RecordCount
		sourceTotalSize += seg.FileSize

		// Update time bounds
		if seg.TsRange.Lower.Int64 < sourceMinTimestamp {
			sourceMinTimestamp = seg.TsRange.Lower.Int64
		}
		if seg.TsRange.Upper.Int64 > sourceMaxTimestamp {
			sourceMaxTimestamp = seg.TsRange.Upper.Int64
		}
	}

	// Extract destination object keys from results/output segments and calculate dest time bounds
	destObjectKeys := make([]string, len(results))
	var destTotalRecords, destTotalSize int64
	var destMinTimestamp, destMaxTimestamp int64 = int64(^uint64(0) >> 1), 0 // Initialize with max int64 and 0
	for i, result := range results {
		stats, ok := result.Metadata.(factories.MetricsFileStats)
		if !ok {
			return fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}
		// Use the segment ID from the corresponding output segment
		if i < len(outputSegments) {
			destObjectKeys[i] = helpers.MakeDBObjectID(orgID, collectorName, dateInt, getHourFromTimestamp(stats.FirstTS), outputSegments[i].SegmentID, "metrics")
		}
		destTotalRecords += result.RecordCount
		destTotalSize += result.FileSize

		// Update dest time bounds
		if stats.FirstTS < destMinTimestamp {
			destMinTimestamp = stats.FirstTS
		}
		if stats.LastTS > destMaxTimestamp {
			destMaxTimestamp = stats.LastTS
		}
	}

	// Create segment_journal entry
	logParams := lrdb.InsertSegmentJournalParams{
		Signal:             2, // 2 = metrics (based on enum pattern)
		Action:             action,
		OrganizationID:     orgID,
		InstanceNum:        instanceNum,
		Dateint:            dateInt,
		SourceCount:        int32(len(inputSegments)),
		SourceObjectKeys:   sourceObjectKeys,
		SourceTotalRecords: sourceTotalRecords,
		SourceTotalSize:    sourceTotalSize,
		DestCount:          int32(len(results)),
		DestObjectKeys:     destObjectKeys,
		DestTotalRecords:   destTotalRecords,
		DestTotalSize:      destTotalSize,
		RecordEstimate:     recordEstimate,
		Metadata:           make(map[string]any), // Empty metadata for now
		SourceMinTimestamp: sourceMinTimestamp,
		SourceMaxTimestamp: sourceMaxTimestamp,
		DestMinTimestamp:   destMinTimestamp,
		DestMaxTimestamp:   destMaxTimestamp,
		SourceFrequencyMs:  sourceFrequencyMs,
		DestFrequencyMs:    destFrequencyMs,
	}

	return store.InsertSegmentJournal(ctx, logParams)
}
