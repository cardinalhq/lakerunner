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
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionUploadParams contains parameters for uploading compacted metric files.
type CompactionUploadParams struct {
	OrganizationID string
	InstanceNum    int16
	Dateint        int32
	FrequencyMs    int32
	IngestDateint  int32
	CollectorName  string
	Bucket         string
}

// UploadCompactedMetrics uploads compacted metric files to S3 and updates the database,
// replacing old segments with new compacted ones.
func UploadCompactedMetrics(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	oldRows []lrdb.MetricSeg,
	params CompactionUploadParams,
) error {
	startTs, endTs := GetStartEndTimes(oldRows)

	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	// Base parameters for database update
	replaceParams := lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgUUID,
		Dateint:        params.Dateint,
		InstanceNum:    params.InstanceNum,
		IngestDateint:  params.IngestDateint,
		FrequencyMs:    params.FrequencyMs,
		Published:      true,
		Rolledup:       helpers.AllRolledUp(oldRows),
		CreatedBy:      lrdb.CreatedByCompact,
	}

	// Add old records to be replaced
	for _, row := range oldRows {
		replaceParams.OldRecords = append(replaceParams.OldRecords, lrdb.ReplaceMetricSegsOld{
			TidPartition: row.TidPartition,
			SegmentID:    row.SegmentID,
		})
	}

	dateint, hour := helpers.MSToDateintHour(startTs)

	// Process each output file atomically
	for _, file := range results {
		// Generate operation ID for tracking this atomic operation
		opID := fmt.Sprintf("metric_op_%d_%s", time.Now().Unix(), idgen.GenerateShortBase32ID())
		fileLogger := ll.With(slog.String("operationID", opID), slog.String("file", file.FileName))

		fileLogger.Debug("Starting atomic metric compaction operation",
			slog.Int64("recordCount", file.RecordCount),
			slog.Int64("fileSize", file.FileSize))

		segmentID := s3helper.GenerateID()
		newObjectID := helpers.MakeDBObjectID(orgUUID, params.CollectorName, dateint, hour, segmentID, "metrics")

		fileLogger.Debug("Uploading compacted metric file to S3 - point of no return approaching",
			slog.String("newObjectID", newObjectID),
			slog.String("bucket", params.Bucket),
			slog.Int64("newSegmentID", segmentID))

		err := s3helper.UploadS3Object(ctx, s3client, params.Bucket, newObjectID, file.FileName)
		if err != nil {
			fileLogger.Error("Atomic operation failed during S3 upload - no changes made",
				slog.Any("error", err),
				slog.String("objectID", newObjectID))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}

		fileLogger.Debug("S3 upload successful, updating database index - CRITICAL SECTION",
			slog.String("uploadedObject", newObjectID))

		// Create params for this single file
		singleParams := lrdb.ReplaceMetricSegsParams{
			OrganizationID: replaceParams.OrganizationID,
			Dateint:        replaceParams.Dateint,
			FrequencyMs:    replaceParams.FrequencyMs,
			InstanceNum:    replaceParams.InstanceNum,
			IngestDateint:  replaceParams.IngestDateint,
			Published:      replaceParams.Published,
			Rolledup:       replaceParams.Rolledup,
			CreatedBy:      replaceParams.CreatedBy,
			OldRecords:     replaceParams.OldRecords, // Contains all old records
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					TidPartition: 0, // Legacy field, always 0 now
					SegmentID:    segmentID,
					StartTs:      startTs,
					EndTs:        endTs,
					RecordCount:  file.RecordCount,
					FileSize:     file.FileSize,
					TidCount:     1, // Single compacted file
				},
			},
		}

		// Add fingerprints if available in metadata
		if stats, ok := file.Metadata.(factories.MetricsFileStats); ok {
			singleParams.Fingerprints = stats.Fingerprints
		}

		if err := mdb.ReplaceMetricSegs(ctx, singleParams); err != nil {
			fileLogger.Error("CRITICAL: Database update failed after S3 upload - file orphaned in S3",
				slog.Any("error", err),
				slog.String("orphanedObject", newObjectID),
				slog.Int64("orphanedSegmentID", segmentID),
				slog.String("bucket", params.Bucket))

			// Best effort cleanup - try to delete the uploaded file
			if cleanupErr := s3helper.DeleteS3Object(ctx, s3client, params.Bucket, newObjectID); cleanupErr != nil {
				fileLogger.Error("Failed to cleanup orphaned S3 object - manual cleanup required",
					slog.Any("error", cleanupErr),
					slog.String("objectID", newObjectID),
					slog.String("bucket", params.Bucket))
			}
			return fmt.Errorf("replacing metric segments: %w", err)
		}

		fileLogger.Debug("ATOMIC OPERATION COMMITTED SUCCESSFULLY - database updated, segments swapped",
			slog.Int64("newSegmentID", segmentID),
			slog.Int64("newRecordCount", file.RecordCount),
			slog.Int64("newFileSize", file.FileSize),
			slog.String("newObjectID", newObjectID))
	}

	return nil
}

// ScheduleOldFileCleanup schedules deletion of old files after successful compaction.
func ScheduleOldFileCleanup(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	oldRows []lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
) {
	for _, row := range oldRows {
		rst, _, ok := helpers.RangeBounds(row.TsRange)
		if !ok {
			ll.Error("Invalid time range in row", slog.Any("tsRange", row.TsRange))
			continue
		}
		dateint, hour := helpers.MSToDateintHour(rst.Int64)
		oid := helpers.MakeDBObjectID(profile.OrganizationID, profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
		if err := s3helper.ScheduleS3Delete(ctx, mdb, profile.OrganizationID, profile.InstanceNum, profile.Bucket, oid); err != nil {
			ll.Error("scheduleS3Delete", slog.String("error", err.Error()))
		}
	}
}
