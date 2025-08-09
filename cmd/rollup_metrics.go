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

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "rollup-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-rollup-metrics"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "rollup"),
			)
			iter := attribute.NewMergeIterator(&commonAttributes, &addlAttrs)
			attrs := []attribute.KeyValue{}
			for iter.Next() {
				attrs = append(attrs, iter.Attribute())
			}
			commonAttributes = attribute.NewSet(attrs...)

			go diskUsageLoop(doneCtx)

			sp, err := storageprofile.SetupStorageProfiles()
			if err != nil {
				return fmt.Errorf("failed to setup storage profiles: %w", err)
			}

			return RunqueueLoop(doneCtx, sp, "metrics", "rollup", servicename, metricRollupItem)
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
	rpf_estimate int64,
) (WorkResult, error) {
	previousFrequency, ok := rollupSources[inf.FrequencyMs()]
	if !ok {
		ll.Error("Unknown parent frequency, dropping rollup request", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return WorkResultSuccess, nil
	}
	if !isWantedFrequency(inf.FrequencyMs()) || !isWantedFrequency(previousFrequency) {
		ll.Info("Skipping rollup for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())), slog.Int("previousFrequency", int(previousFrequency)))
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
		return 0, err
	}

	ll.Info("Processing rollup item", slog.Int("previousFrequency", int(previousFrequency)), slog.Any("workItem", inf.AsMap()), slog.Int64("estimatedRowsPerFile", rpf_estimate))
	return metricRollupItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, previousFrequency, rpf_estimate)
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
	rpf_estimate int64,
) (WorkResult, error) {
	st, et, ok := RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}
	sourceRows, err := mdb.GetMetricSegs(ctx, lrdb.GetMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    previousFrequency,
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get previous metric segments", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	if allRolledUp(sourceRows) {
		return WorkResultSuccess, nil
	}

	currentRows, err := mdb.GetMetricSegs(ctx, lrdb.GetMetricSegsParams{
		OrganizationID: inf.OrganizationID(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get metric segments", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	err = rollupInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, sourceRows, currentRows, rpf_estimate)
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
	rpf_estimate int64,
) error {
	if len(sourceRows) == 0 {
		return nil
	}

	ingest_dateint := int32(0)
	files := make([]string, 0, len(sourceRows))
	for _, row := range sourceRows {
		rst, _, ok := RangeBounds(row.TsRange)
		rts_dateint, _ := helpers.MSToDateintHour(rst.Int64)
		ingest_dateint = max(ingest_dateint, rts_dateint)
		if !ok {
			ll.Error("Invalid time range in source row", slog.Any("tsRange", row.TsRange))
			return fmt.Errorf("invalid time range in source row: %v", row.TsRange)
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		fn, downloadedSize, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			if s3helper.S3ErrorIs404(err) {
				ll.Info("S3 object not found, skipping", slog.String("objectID", objectID))
				continue
			}
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return err
		}
		ll.Info("Downloaded S3 SOURCE", slog.String("objectID", objectID), slog.String("bucket", profile.Bucket), slog.Int64("rowFileSize", row.FileSize), slog.Int64("s3FileSize", downloadedSize))
		files = append(files, fn)
	}

	if len(files) == 0 {
		ll.Info("No files to roll up, skipping")
		return nil
	}

	startTS, endTS, ok := RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	ll.Info("Rolling up files", slog.Int("fileCount", len(files)), slog.Int("frequency", int(inf.FrequencyMs())), slog.Int64("startTS", startTS.Time.UTC().UnixMilli()), slog.Int64("endTS", endTS.Time.UTC().UnixMilli()))
	merger, err := NewTIDMerger(tmpdir, files, inf.FrequencyMs(), rpf_estimate, startTS.Time.UTC().UnixMilli(), endTS.Time.UTC().UnixMilli())
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
		IngestDateint:  ingest_dateint,
		InstanceNum:    inf.InstanceNum(),
		FrequencyMs:    inf.FrequencyMs(),
		Published:      true,
		Rolledup:       false,
	}

	for _, row := range existingRowsForThisRollup {
		ll.Info("removing old metric segment", slog.Int("tidPartition", int(row.TidPartition)), slog.Int64("segmentID", row.SegmentID))
		params.OldRecords = append(params.OldRecords, lrdb.ReplaceMetricSegsOld{
			TidPartition: row.TidPartition,
			SegmentID:    row.SegmentID,
		})
	}

	st, et, ok := RangeBounds(inf.TsRange())
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
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
			StartTs:      st.Time.UTC().UnixMilli(),
			EndTs:        et.Time.UTC().UnixMilli(),
			RecordCount:  result.RecordCount,
			FileSize:     result.FileSize,
		})
	}

	if err := mdb.ReplaceMetricSegs(ctx, params); err != nil {
		ll.Error("Failed to replace metric segments", slog.Any("error", err))
		return fmt.Errorf("replacing metric segments: %w", err)
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
		rst, _, ok := RangeBounds(row.TsRange)
		if !ok {
			ll.Error("Invalid time range in existing row", slog.Any("tsRange", row.TsRange))
			continue
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		oid := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		ll.Info("Deleting old S3 object", slog.String("objectID", oid))
		if err := s3helper.ScheduleS3Delete(ctx, mdb, profile.OrganizationID, profile.InstanceNum, profile.Bucket, oid); err != nil {
			ll.Error("scheduleS3Delete", slog.String("error", err.Error()))
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
