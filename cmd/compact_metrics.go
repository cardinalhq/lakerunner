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
	"math"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-compact-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "compact"),
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

			loop, err := NewRunqueueLoopContext(doneCtx, "metrics", "compact", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, compactRollupItem)
		},
	}

	rootCmd.AddCommand(cmd)
}

func compactRollupItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
) (WorkResult, error) {
	if !isWantedFrequency(inf.FrequencyMs()) {
		ll.Info("Skipping compaction for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return WorkResultSuccess, nil
	}

	profile, err := sp.Get(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			return WorkResultTryAgainLater, err
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Info("Processing metric compression item", slog.Any("workItem", inf))
	return metricCompactItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate)
}

func metricCompactItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
) (WorkResult, error) {
	st, et, ok := RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	inRows, err := mdb.GetMetricSegs(ctx, lrdb.GetMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get current metric segments", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	if len(inRows) == 0 {
		ll.Info("No input rows to compact, skipping work item")
		return WorkResultSuccess, nil
	}

	if !shouldCompactMetrics(inRows) {
		ll.Info("No need to compact metrics, skipping work item", slog.Int("rowCount", len(inRows)))
		return WorkResultSuccess, nil
	}

	err = compactInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, inRows, rpfEstimate)
	if err != nil {
		ll.Error("Failed to compact interval", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	return WorkResultSuccess, nil
}

func shouldCompactMetrics(rows []lrdb.MetricSeg) bool {
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

func getStartEndTimes(rows []lrdb.MetricSeg) (int64, int64) {
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
	rpfEstimate int64) error {
	st, _, ok := RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	files := make([]string, 0, len(rows))
	for _, row := range rows {
		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		fn, downloadedSize, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return err
		}
		if is404 {
			ll.Info("S3 object not found, skipping", slog.String("bucket", profile.Bucket), slog.String("objectID", objectID))
			continue
		}

		ll.Info("Downloaded S3 SOURCE", slog.String("objectID", objectID), slog.String("bucket", profile.Bucket), slog.Int64("rowFileSize", row.FileSize), slog.Int64("s3FileSize", downloadedSize))
		files = append(files, fn)
	}

	if len(files) == 0 {
		ll.Info("No files to compact, skipping work item")
		return nil
	}

	startTS, endTS, ok := RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	merger, err := NewTIDMerger(tmpdir, files, inf.FrequencyMs(), rpfEstimate, startTS.Time.UTC().UnixMilli(), endTS.Time.UTC().UnixMilli())
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
	startTs, endTs := getStartEndTimes(rows)
	ingest_dateint := getIngestDateint(rows)

	// now we need to update the source items to mark them as having been rolled up,
	// add our new file to the database, and remove any previous files for this timebox.
	params := lrdb.ReplaceMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		IngestDateint:  ingest_dateint,
		InstanceNum:    inf.InstanceNum(),
		FrequencyMs:    inf.FrequencyMs(),
		Published:      true,
		Rolledup:       allRolledUp(rows),
		CreatedBy:      lrdb.CreatedByCompact,
	}

	for _, row := range rows {
		ll.Info("removing old metric segment", slog.Int("tidPartition", int(row.TidPartition)), slog.Int64("segmentID", row.SegmentID))
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
		ll.Info("adding new metric segment", slog.Int("tidPartition", int(tidPartition)), slog.Int64("segmentID", segmentID))
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
	ll.Info("Replaced metric segments")

	for _, row := range rows {
		rst, _, ok := RangeBounds(row.TsRange)
		if !ok {
			ll.Error("Invalid time range in row", slog.Any("tsRange", row.TsRange))
			return fmt.Errorf("invalid time range in row: %v", row.TsRange)
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		oid := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		if err := s3helper.ScheduleS3Delete(ctx, mdb, profile.OrganizationID, profile.InstanceNum, profile.Bucket, oid); err != nil {
			ll.Error("scheduleS3Delete", slog.String("error", err.Error()))
		}
	}

	return nil
}
