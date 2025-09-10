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

// Package metricsprocessing contains shared business logic for metrics ingestion and compaction
// using the new filereader/parquetwriter architecture.
package metricsprocessing

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessedSegment represents a bundle of information about a processed segment
// ready for upload and database insertion. Used in both compaction and rollup.
type ProcessedSegment struct {
	SegmentID    int64                // Pre-generated segment ID for upload and DB
	Result       parquetwriter.Result // File info (path, size, record count)
	StartTs      int64                // Start timestamp from file metadata
	EndTs        int64                // End timestamp from file metadata
	Fingerprints []int64              // Fingerprints from file metadata
	ObjectID     string               // S3 object ID for upload
	Uploaded     bool                 // Track upload status for cleanup
}

// UploadToS3 uploads the segment file to S3 and marks it as uploaded
func (ps *ProcessedSegment) UploadToS3(ctx context.Context, blobclient cloudstorage.Client, bucket string) error {
	if ps.Uploaded {
		return fmt.Errorf("segment %d already uploaded", ps.SegmentID)
	}

	err := blobclient.UploadObject(ctx, bucket, ps.ObjectID, ps.Result.FileName)
	if err != nil {
		return fmt.Errorf("failed to upload segment %d to S3: %w", ps.SegmentID, err)
	}

	ps.Uploaded = true
	return nil
}

// ScheduleCleanupIfUploaded schedules S3 deletion if the segment was uploaded
func (ps *ProcessedSegment) ScheduleCleanupIfUploaded(ctx context.Context, mdb cloudstorage.ObjectCleanupStore, orgID uuid.UUID, instanceNum int16, bucket string) error {
	if !ps.Uploaded {
		return nil // Nothing to clean up
	}

	return cloudstorage.ScheduleS3Delete(ctx, mdb, orgID, instanceNum, bucket, ps.ObjectID)
}

// GetDateintHour returns the dateint and hour from the segment's start timestamp
func (ps *ProcessedSegment) GetDateintHour() (int32, int16) {
	return helpers.MSToDateintHour(ps.StartTs)
}

// GetDateint returns just the dateint from the segment's start timestamp
func (ps *ProcessedSegment) GetDateint() int32 {
	dateint, _ := helpers.MSToDateintHour(ps.StartTs)
	return dateint
}

// ProcessedSegments is a slice of processed segments with helper methods
type ProcessedSegments []*ProcessedSegment

// UploadAll uploads all segments to S3, stopping on first error
func (segments ProcessedSegments) UploadAll(ctx context.Context, blobclient cloudstorage.Client, bucket string) error {
	for _, segment := range segments {
		if err := segment.UploadToS3(ctx, blobclient, bucket); err != nil {
			return err
		}
	}
	return nil
}

// ScheduleCleanupAll schedules cleanup for all uploaded segments
func (segments ProcessedSegments) ScheduleCleanupAll(ctx context.Context, mdb cloudstorage.ObjectCleanupStore, orgID uuid.UUID, instanceNum int16, bucket string) {
	for _, segment := range segments {
		if err := segment.ScheduleCleanupIfUploaded(ctx, mdb, orgID, instanceNum, bucket); err != nil {
			slog.Error("Failed to schedule S3 cleanup for segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.String("objectID", segment.ObjectID),
				slog.Any("error", err))
		}
	}
}

// SegmentIDs extracts just the segment IDs from all segments
func (segments ProcessedSegments) SegmentIDs() []int64 {
	ids := make([]int64, len(segments))
	for i, segment := range segments {
		ids[i] = segment.SegmentID
	}
	return ids
}

// QueueRollupWork queues rollup work for all segments
func (segments ProcessedSegments) QueueRollupWork(ctx context.Context, kafkaProducer fly.Producer, orgID uuid.UUID, instanceNum int16, frequency int32, slotID int32, slotCount int32) error {
	for _, segment := range segments {
		segmentStartTime := time.Unix(segment.StartTs/1000, (segment.StartTs%1000)*1000000)
		if err := queueMetricRollup(ctx, kafkaProducer, orgID, segment.GetDateint(), frequency, instanceNum, slotID, slotCount, segment.SegmentID, segment.Result.RecordCount, segment.Result.FileSize, segmentStartTime); err != nil {
			return fmt.Errorf("queueing rollup work for segment %d: %w", segment.SegmentID, err)
		}
	}
	return nil
}

// UploadParams contains parameters for uploading metric files to S3 and database.
type UploadParams struct {
	OrganizationID string
	InstanceNum    int16
	Dateint        int32
	FrequencyMs    int32
	IngestDateint  int32
	CollectorName  string
	Bucket         string
	CreatedBy      lrdb.CreatedBy
}
