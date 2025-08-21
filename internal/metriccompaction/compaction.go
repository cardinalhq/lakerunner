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

package metriccompaction

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/tidprocessing"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const targetFileSize = int64(1_100_000)

type WorkResult int

const (
	WorkResultSuccess WorkResult = iota
	WorkResultTryAgainLater
)

func ProcessItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
) (WorkResult, error) {
	if !helpers.IsWantedFrequency(inf.FrequencyMs()) {
		ll.Info("Skipping compaction for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return WorkResultSuccess, nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Info("Starting metric compaction",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int64("workQueueID", inf.ID()))

	t0 := time.Now()
	result, err := doCompactItem(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate)

	if err != nil {
		ll.Info("Metric compaction completed",
			slog.String("result", "error"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
	} else {
		resultStr := "success"
		if result == WorkResultTryAgainLater {
			resultStr = "try_again_later"
		}
		ll.Info("Metric compaction completed",
			slog.String("result", resultStr),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
	}

	return result, err
}

func doCompactItem(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
) (WorkResult, error) {
	st, et, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	const maxRowsLimit = 1000
	totalBatchesProcessed := 0
	totalSegmentsProcessed := 0
	cursorCreatedAt := time.Time{} // Start from beginning (zero time)
	cursorSegmentID := int64(0)    // Start from beginning (zero ID)

	// Loop until we've processed all available segments
	for {
		// Check if context is cancelled before starting next batch
		if ctx.Err() != nil {
			ll.Info("Context cancelled, stopping compaction loop - will retry to continue",
				slog.Int("processedBatches", totalBatchesProcessed),
				slog.Int("processedSegments", totalSegmentsProcessed),
				slog.Any("error", ctx.Err()))
			return WorkResultTryAgainLater, nil
		}

		ll.Info("Querying for metric segments to compact",
			slog.Int("batch", totalBatchesProcessed+1),
			slog.Time("cursorCreatedAt", cursorCreatedAt),
			slog.Int64("cursorSegmentID", cursorSegmentID))

		inRows, err := mdb.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
			OrganizationID:  inf.OrganizationID(),
			Dateint:         inf.Dateint(),
			FrequencyMs:     inf.FrequencyMs(),
			InstanceNum:     inf.InstanceNum(),
			StartTs:         st.Time.UTC().UnixMilli(),
			EndTs:           et.Time.UTC().UnixMilli(),
			MaxFileSize:     targetFileSize * 9 / 10, // Only include files < 90% of target (larger files are fine as-is)
			CursorCreatedAt: cursorCreatedAt,         // Cursor for pagination
			CursorSegmentID: cursorSegmentID,         // Cursor for pagination
			Maxrows:         maxRowsLimit,            // Safety limit for compaction batch
		})
		if err != nil {
			ll.Error("Failed to get current metric segments", slog.Any("error", err))
			return WorkResultTryAgainLater, err
		}

		// No more segments to process
		if len(inRows) == 0 {
			if totalBatchesProcessed == 0 {
				ll.Info("No input rows to compact, skipping work item")
			} else {
				ll.Info("Finished processing all compaction batches",
					slog.Int("totalBatches", totalBatchesProcessed),
					slog.Int("totalSegments", totalSegmentsProcessed))
			}
			return WorkResultSuccess, nil
		}

		ll.Info("Processing compaction batch",
			slog.Int("segmentCount", len(inRows)),
			slog.Int("batch", totalBatchesProcessed+1))

		// Update cursor to last (created_at, segment_id) in this batch to ensure forward progress
		if len(inRows) > 0 {
			lastRow := inRows[len(inRows)-1]
			cursorCreatedAt = lastRow.CreatedAt
			cursorSegmentID = lastRow.SegmentID
		}

		// Check if this batch needs compaction
		if !ShouldCompactMetrics(inRows) {
			ll.Info("No need to compact metrics in this batch", slog.Int("rowCount", len(inRows)))

			// If we didn't hit the limit, we've seen all segments
			if len(inRows) < maxRowsLimit {
				if totalBatchesProcessed == 0 {
					ll.Info("No segments need compaction")
				}
				return WorkResultSuccess, nil
			}
			// Continue to next batch without processing - cursor already advanced
			totalBatchesProcessed++
			continue
		}

		// Process this batch
		err = compactInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, inRows, rpfEstimate)
		if err != nil {
			ll.Error("Failed to compact interval", slog.Any("error", err))
			return WorkResultTryAgainLater, err
		}

		totalBatchesProcessed++
		totalSegmentsProcessed += len(inRows)

		// If we didn't hit the limit, we've processed all available segments
		if len(inRows) < maxRowsLimit {
			ll.Info("Completed all compaction batches",
				slog.Int("totalBatches", totalBatchesProcessed),
				slog.Int("totalSegments", totalSegmentsProcessed))
			return WorkResultSuccess, nil
		}

		// Continue to next batch - cursor already advanced
		ll.Info("Batch completed, checking for more segments",
			slog.Int("processedSegments", len(inRows)),
			slog.Time("nextCursorCreatedAt", cursorCreatedAt),
			slog.Int64("nextCursorSegmentID", cursorSegmentID))
	}
}

func ShouldCompactMetrics(rows []lrdb.MetricSeg) bool {
	if len(rows) < 2 {
		return false
	}

	const smallThreshold = int64(targetFileSize) * 3 / 10

	var totalSize int64
	for _, row := range rows {
		totalSize += row.FileSize
		if row.FileSize > targetFileSize*2 || row.FileSize < smallThreshold {
			return true
		}
	}

	estimatedFileCount := (totalSize + targetFileSize - 1) / targetFileSize
	compact := estimatedFileCount < int64(len(rows))-3 // TODO this feels hacky
	return compact
}

func GetStartEndTimes(rows []lrdb.MetricSeg) (int64, int64) {
	startTs := int64(math.MaxInt64)
	endTs := int64(math.MinInt64)
	for _, row := range rows {
		rowStartTs := row.TsRange.Lower.Int64
		rowEndTs := row.TsRange.Upper.Int64
		startTs = min(startTs, rowStartTs)
		endTs = max(endTs, rowEndTs)
	}
	return startTs, endTs
}

func getIngestDateint(rows []lrdb.MetricSeg) int32 {
	if len(rows) == 0 {
		return 0
	}
	ingest_dateint := int32(0)
	for _, row := range rows {
		ingest_dateint = max(ingest_dateint, row.IngestDateint)
	}
	return ingest_dateint
}

func compactInterval(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	st, _, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	files := make([]string, 0, len(rows))
	for _, row := range rows {
		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		fn, _, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return err
		}
		if is404 {
			ll.Info("S3 object not found, skipping", slog.String("bucket", profile.Bucket), slog.String("objectID", objectID))
			continue
		}

		files = append(files, fn)
	}

	if len(files) == 0 {
		ll.Info("No files to compact, skipping work item")
		return nil
	}

	startTS, endTS, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	merger, err := tidprocessing.NewTIDMerger(tmpdir, files, inf.FrequencyMs(), rpfEstimate, startTS.Time.UTC().UnixMilli(), endTS.Time.UTC().UnixMilli())
	if err != nil {
		ll.Error("Failed to create TIDMerger", slog.Any("error", err))
		return fmt.Errorf("creating TIDMerger: %w", err)
	}

	mergeResult, stats, err := merger.Merge()
	if stats.DatapointsOutOfRange > 0 {
		ll.Warn("Some datapoints were out of range", slog.Int64("count", stats.DatapointsOutOfRange))
	}
	if err != nil {
		ll.Error("Failed to merge files", slog.Any("error", err))
		return fmt.Errorf("merging files: %w", err)
	}
	ll.Info("Merge results", slog.Any("sourceFiles", files), slog.Any("mergeResult", mergeResult), slog.Int64("estimatedRowCount", rpfEstimate))

	startingFileCount := len(files)
	endingFileCount := len(mergeResult)
	ll.Info("Compaction results",
		slog.Int("startingFileCount", startingFileCount),
		slog.Int("endingFileCount", endingFileCount),
		slog.Int("percentFileCountReduction", (startingFileCount-endingFileCount)*100/startingFileCount),
	)

	// Find the starTs and endTs of this new group of files.
	startTs, endTs := GetStartEndTimes(rows)
	ingest_dateint := getIngestDateint(rows)

	// now we need to update the source items to mark them as having been rolled up,
	// add our new file to the database, and remove any previous files for this timebox.
	params := lrdb.ReplaceMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		IngestDateint:  ingest_dateint,
		FrequencyMs:    inf.FrequencyMs(),
		Published:      true,
		Rolledup:       helpers.AllRolledUp(rows),
		CreatedBy:      lrdb.CreatedByCompact,
	}

	for _, row := range rows {
		params.OldRecords = append(params.OldRecords, lrdb.ReplaceMetricSegsOld{
			TidPartition: row.TidPartition,
			SegmentID:    row.SegmentID,
		})
	}

	dateint, hour := helpers.MSToDateintHour(startTs)
	for tidPartition, result := range mergeResult {
		segmentID := s3helper.GenerateID()
		newObjectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, segmentID, "metrics")
		ll.Info("Uploading to S3", slog.String("objectID", newObjectID), slog.String("bucket", profile.Bucket))
		err = s3helper.UploadS3Object(ctx, s3client, profile.Bucket, newObjectID, result.FileName)
		if err != nil {
			ll.Error("Failed to upload new S3 object", slog.String("objectID", newObjectID), slog.Any("error", err))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}
		params.NewRecords = append(params.NewRecords, lrdb.ReplaceMetricSegsNew{
			TidPartition: int16(tidPartition),
			SegmentID:    segmentID,
			StartTs:      startTs,
			EndTs:        endTs,
			RecordCount:  result.RecordCount,
			FileSize:     result.FileSize,
			TidCount:     result.TidCount,
		})
	}

	if err := mdb.ReplaceMetricSegs(ctx, params); err != nil {
		ll.Error("Failed to replace metric segments", slog.Any("error", err))
		return fmt.Errorf("replacing metric segments: %w", err)
	}

	for _, row := range rows {
		rst, _, ok := helpers.RangeBounds(row.TsRange)
		if !ok {
			ll.Error("Invalid time range in row", slog.Any("tsRange", row.TsRange))
			return fmt.Errorf("invalid time range in row: %v", row.TsRange)
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		oid := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		if err := s3helper.ScheduleS3Delete(ctx, mdb, profile.OrganizationID, profile.Bucket, oid); err != nil {
			ll.Error("scheduleS3Delete", slog.String("error", err.Error()))
		}
	}

	return nil
}
