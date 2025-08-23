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

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/tidprocessing"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "rollup-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-rollup-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "rollup"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewRunqueueLoopContext(doneCtx, "metrics", "rollup", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, metricRollupItem, nil)
		},
	}

	rootCmd.AddCommand(cmd)
}

func metricRollupItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	_ any,
) (WorkResult, error) {
	previousFrequency, ok := helpers.RollupSources[inf.FrequencyMs()]
	if !ok {
		ll.Error("Unknown parent frequency, dropping rollup request", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return WorkResultSuccess, nil
	}
	if !helpers.IsWantedFrequency(inf.FrequencyMs()) || !helpers.IsWantedFrequency(previousFrequency) {
		ll.Info("Skipping rollup for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())), slog.Int("previousFrequency", int(previousFrequency)))
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
		return 0, err
	}

	ll.Info("Starting metric rollup",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int("previousFrequencyMs", int(previousFrequency)),
		slog.Int64("workQueueID", inf.ID()),
		slog.Int64("estimatedRowsPerFile", rpfEstimate))

	t0 := time.Now()
	_, err = metricRollupItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, previousFrequency, rpfEstimate)

	if err != nil {
		ll.Info("Metric rollup completed",
			slog.String("result", "error"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
		return WorkResultTryAgainLater, err
	} else {
		ll.Info("Metric rollup completed",
			slog.String("result", "success"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
		return WorkResultSuccess, nil
	}
}

func metricRollupItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	previousFrequency int32,
	rpfEstimate int64,
) (WorkResult, error) {
	st, et, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}
	sourceRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    previousFrequency,
		InstanceNum:    inf.InstanceNum(),
		SlotID:         inf.SlotId(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get previous metric segments", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	if helpers.AllRolledUp(sourceRows) {
		return WorkResultSuccess, nil
	}

	currentRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		InstanceNum:    inf.InstanceNum(),
		SlotID:         inf.SlotId(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get metric segments", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	err = rollupInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, sourceRows, currentRows, rpfEstimate)
	if err != nil {
		ll.Error("Failed to rollup interval", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	return WorkResultSuccess, nil
}

// rollupInterval rolls up the metric segments for a given timebox.
func rollupInterval(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRowsForThisRollup []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	if len(sourceRows) == 0 {
		return nil
	}

	ingest_dateint := int32(0)
	files := make([]string, 0, len(sourceRows))
	for _, row := range sourceRows {
		rst, _, ok := helpers.RangeBounds(row.TsRange)
		rts_dateint, _ := helpers.MSToDateintHour(rst.Int64)
		ingest_dateint = max(ingest_dateint, rts_dateint)
		if !ok {
			ll.Error("Invalid time range in source row", slog.Any("tsRange", row.TsRange))
			return fmt.Errorf("invalid time range in source row: %v", row.TsRange)
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		fn, downloadedSize, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return err
		}
		if is404 {
			ll.Info("S3 object not found, skipping", slog.String("objectID", objectID))
			continue
		}
		ll.Info("Downloaded S3 SOURCE", slog.String("objectID", objectID), slog.String("bucket", profile.Bucket), slog.Int64("rowFileSize", row.FileSize), slog.Int64("s3FileSize", downloadedSize))
		files = append(files, fn)
	}

	if len(files) == 0 {
		ll.Info("No files to roll up, skipping")
		return nil
	}

	startTS, endTS, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	ll.Info("Rolling up files", slog.Int("fileCount", len(files)), slog.Int("frequency", int(inf.FrequencyMs())), slog.Int64("startTS", startTS.Time.UTC().UnixMilli()), slog.Int64("endTS", endTS.Time.UTC().UnixMilli()))
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
	ll.Info("Merge results", slog.Any("sourceFiles", files), slog.Any("mergeResult", mergeResult))

	// now we need to update the source items to mark them as having been rolled up,
	// add our new file to the database, and remove any previous files for this timebox.
	params := lrdb.ReplaceMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		InstanceNum:    inf.InstanceNum(),
		IngestDateint:  ingest_dateint,
		FrequencyMs:    inf.FrequencyMs(),
		Published:      true,
		Rolledup:       false,
		CreatedBy:      lrdb.CreateByRollup,
	}

	for _, row := range existingRowsForThisRollup {
		ll.Info("removing old metric segment", slog.Int("tidPartition", int(row.TidPartition)), slog.Int64("segmentID", row.SegmentID))
		params.OldRecords = append(params.OldRecords, lrdb.ReplaceMetricSegsOld{
			TidPartition: row.TidPartition,
			SegmentID:    row.SegmentID,
		})
	}

	st, et, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())

	// Process each output file from TIDMerger
	for fileIdx, result := range mergeResult {
		// Use the tid_partition from the first source row since all source rows should have the same partition
		tidPartition := sourceRows[0].TidPartition

		// Generate operation ID for tracking this atomic operation
		opID := fmt.Sprintf("rollup_op_%d_%d_%s", time.Now().Unix(), fileIdx, idgen.GenerateShortBase32ID())
		tidLogger := ll.With(slog.String("operationID", opID), slog.Int("tidPartition", int(tidPartition)))

		tidLogger.Info("Starting atomic metric rollup operation",
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize))

		segmentID := s3helper.GenerateID()
		newObjectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, segmentID, "metrics")

		tidLogger.Info("Uploading rolled-up metric file to S3 - point of no return approaching",
			slog.String("newObjectID", newObjectID),
			slog.String("bucket", profile.Bucket),
			slog.Int64("newSegmentID", segmentID))

		err = s3helper.UploadS3Object(ctx, s3client, profile.Bucket, newObjectID, result.FileName)
		if err != nil {
			tidLogger.Error("Atomic operation failed during S3 upload - no changes made",
				slog.Any("error", err),
				slog.String("objectID", newObjectID))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}

		tidLogger.Debug("S3 upload successful, updating database index - CRITICAL SECTION",
			slog.String("uploadedObject", newObjectID))

		// Create params for this single tid_partition
		singleParams := lrdb.ReplaceMetricSegsParams{
			OrganizationID: params.OrganizationID,
			Dateint:        params.Dateint,
			FrequencyMs:    params.FrequencyMs,
			InstanceNum:    params.InstanceNum,
			IngestDateint:  params.IngestDateint,
			Published:      params.Published,
			Rolledup:       params.Rolledup,
			CreatedBy:      params.CreatedBy,
			OldRecords:     params.OldRecords, // Contains all old records, but DB will filter by tid_partition
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					TidPartition: tidPartition,
					SegmentID:    segmentID,
					StartTs:      st.Time.UTC().UnixMilli(),
					EndTs:        et.Time.UTC().UnixMilli(),
					RecordCount:  result.RecordCount,
					FileSize:     result.FileSize,
				},
			},
		}

		if err := mdb.ReplaceMetricSegs(ctx, singleParams); err != nil {
			tidLogger.Error("CRITICAL: Database update failed after S3 upload - file orphaned in S3",
				slog.Any("error", err),
				slog.String("orphanedObject", newObjectID),
				slog.Int64("orphanedSegmentID", segmentID),
				slog.String("bucket", profile.Bucket))

			// Best effort cleanup - try to delete the uploaded file
			tidLogger.Debug("Attempting to cleanup orphaned S3 object")
			if cleanupErr := s3helper.DeleteS3Object(ctx, s3client, profile.Bucket, newObjectID); cleanupErr != nil {
				tidLogger.Error("Failed to cleanup orphaned S3 object - manual cleanup required",
					slog.Any("error", cleanupErr),
					slog.String("objectID", newObjectID),
					slog.String("bucket", profile.Bucket))
			} else {
				tidLogger.Info("Successfully cleaned up orphaned S3 object")
			}
			return fmt.Errorf("replacing metric segments: %w", err)
		}

		tidLogger.Info("ATOMIC OPERATION COMMITTED SUCCESSFULLY - database updated, segments swapped",
			slog.Int64("newSegmentID", segmentID),
			slog.Int64("newRecordCount", result.RecordCount),
			slog.Int64("newFileSize", result.FileSize),
			slog.String("newObjectID", newObjectID))
	}
	ll.Info("Replaced metric segments")

	// mark the input rows as having been rolled up.
	newlyRolled := []lrdb.BatchMarkMetricSegsRolledupParams{}
	for _, row := range sourceRows {
		if row.Rolledup {
			continue
		}
		newlyRolled = append(newlyRolled, lrdb.BatchMarkMetricSegsRolledupParams{
			OrganizationID: row.OrganizationID,
			Dateint:        row.Dateint,
			FrequencyMs:    row.FrequencyMs,
			SegmentID:      row.SegmentID,
			InstanceNum:    row.InstanceNum,
			SlotID:         row.SlotID,
			TidPartition:   row.TidPartition,
		})
	}
	if len(newlyRolled) > 0 {
		result := mdb.BatchMarkMetricSegsRolledup(ctx, newlyRolled)
		result.Exec(func(i int, err error) {
			if err != nil {
				ll.Error("Failed to mark metric segments as rolled up", slog.Int("index", i), slog.Any("error", err), slog.Any("record", newlyRolled[i]))
			}
		})
	}

	if err := queueMetricCompaction(ctx, mdb, qmcFromWorkable(inf)); err != nil {
		return fmt.Errorf("queueing metric compaction: %w", err)
	}
	if err := queueMetricRollup(ctx, mdb, qmcFromWorkable(inf)); err != nil {
		return fmt.Errorf("queueing metric rollup: %w", err)
	}

	// now delete the old files from S3.
	for _, row := range existingRowsForThisRollup {
		rst, _, ok := helpers.RangeBounds(row.TsRange)
		if !ok {
			ll.Error("Invalid time range in existing row", slog.Any("tsRange", row.TsRange))
			continue
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		oid := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		ll.Info("Deleting old S3 object", slog.String("objectID", oid))
		if err := s3helper.ScheduleS3Delete(ctx, mdb, profile.OrganizationID, profile.InstanceNum, profile.Bucket, oid); err != nil {
			ll.Error("scheduleS3Delete", slog.Any("error", err))
		}
	}

	return nil
}

// boxesForRange returns a list of timebox IDs for the given start and end timestamps and frequency.
func boxesForRange(startTs, endTs int64, frequencyMs int32) []int64 {
	if startTs > endTs || frequencyMs <= 0 {
		return []int64{}
	}
	firstBox := startTs / int64(frequencyMs)
	lastBox := endTs / int64(frequencyMs)
	nBoxes := lastBox - firstBox + 1
	boxes := make([]int64, nBoxes)
	for n := range nBoxes {
		boxes[n] = firstBox + n
	}
	return boxes
}
