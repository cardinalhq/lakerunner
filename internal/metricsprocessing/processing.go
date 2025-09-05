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
	"math"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
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

// NewProcessedSegment creates a new processed segment bundle from a parquet result
func NewProcessedSegment(ctx context.Context, result parquetwriter.Result, orgID uuid.UUID, collectorName string) (*ProcessedSegment, error) {
	// Extract metadata from the file
	metadata, err := ExtractFileMetadata(ctx, result)
	if err != nil {
		return nil, fmt.Errorf("failed to extract file metadata: %w", err)
	}

	// Generate new segment ID
	segmentID := idgen.DefaultFlakeGenerator.NextID()

	// Construct S3 object ID using actual file timestamps
	dateint, hour := helpers.MSToDateintHour(metadata.StartTs)
	objectID := helpers.MakeDBObjectID(orgID, collectorName, dateint, hour, segmentID, "metrics")

	return &ProcessedSegment{
		SegmentID:    segmentID,
		Result:       result,
		StartTs:      metadata.StartTs,
		EndTs:        metadata.EndTs,
		Fingerprints: metadata.Fingerprints,
		ObjectID:     objectID,
		Uploaded:     false,
	}, nil
}

// UploadToS3 uploads the segment file to S3 and marks it as uploaded
func (ps *ProcessedSegment) UploadToS3(ctx context.Context, s3client *awsclient.S3Client, bucket string) error {
	if ps.Uploaded {
		return fmt.Errorf("segment %d already uploaded", ps.SegmentID)
	}

	err := s3helper.UploadS3Object(ctx, s3client, bucket, ps.ObjectID, ps.Result.FileName)
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

// CreateSegmentsFromResults converts parquet writer results into processed segments
// without uploading them. This is shared between compaction and rollup paths.
func CreateSegmentsFromResults(ctx context.Context, results []parquetwriter.Result, orgID uuid.UUID, collectorName string) (ProcessedSegments, error) {
	segments := make(ProcessedSegments, 0, len(results))
	for _, result := range results {
		segment, err := NewProcessedSegment(ctx, result, orgID, collectorName)
		if err != nil {
			return nil, fmt.Errorf("failed to create processed segment: %w", err)
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

// UploadSegments uploads all provided segments to S3. If an error occurs, the
// returned slice contains only the successfully uploaded segments so callers can
// schedule cleanup for them.
func UploadSegments(ctx context.Context, s3client *awsclient.S3Client, bucket string, segments ProcessedSegments) (ProcessedSegments, error) {
	ll := logctx.FromContext(ctx)

	uploaded := make(ProcessedSegments, 0, len(segments))
	for i, segment := range segments {
		if ctx.Err() != nil {
			ll.Warn("Upload context cancelled", slog.Int("completedUploads", i), slog.Int("totalFiles", len(segments)), slog.Any("error", ctx.Err()))
			return uploaded, ctx.Err()
		}

		if err := segment.UploadToS3(ctx, s3client, bucket); err != nil {
			ll.Error("Failed to upload segment", slog.String("bucket", bucket), slog.String("objectID", segment.ObjectID), slog.Int("completedUploads", i), slog.Any("error", err))
			return uploaded, fmt.Errorf("uploading file %s: %w", segment.ObjectID, err)
		}

		uploaded = append(uploaded, segment)

		ll.Debug("Uploaded segment to S3", slog.String("objectID", segment.ObjectID), slog.Int64("fileSize", segment.Result.FileSize), slog.Int64("recordCount", segment.Result.RecordCount))
	}
	return uploaded, nil
}

// UploadAll uploads all segments to S3, stopping on first error
func (segments ProcessedSegments) UploadAll(ctx context.Context, s3client *awsclient.S3Client, bucket string) error {
	for _, segment := range segments {
		if err := segment.UploadToS3(ctx, s3client, bucket); err != nil {
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

// QueueCompactionWork queues compaction work for all segments
func (segments ProcessedSegments) QueueCompactionWork(ctx context.Context, mdb CompactionWorkQueuer, orgID uuid.UUID, instanceNum int16, frequency int32) error {
	for _, segment := range segments {
		if err := QueueMetricCompaction(ctx, mdb, orgID, segment.GetDateint(), frequency, instanceNum, segment.SegmentID, segment.Result.RecordCount, segment.StartTs, segment.EndTs); err != nil {
			return fmt.Errorf("queueing compaction work for segment %d: %w", segment.SegmentID, err)
		}
	}
	return nil
}

// QueueRollupWork queues rollup work for all segments
func (segments ProcessedSegments) QueueRollupWork(ctx context.Context, mdb RollupWorkQueuer, orgID uuid.UUID, instanceNum int16, frequency int32, slotID int32, slotCount int32) error {
	for _, segment := range segments {
		if err := QueueMetricRollup(ctx, mdb, orgID, segment.GetDateint(), frequency, instanceNum, slotID, slotCount, segment.SegmentID, segment.Result.RecordCount, segment.StartTs); err != nil {
			return fmt.Errorf("queueing rollup work for segment %d: %w", segment.SegmentID, err)
		}
	}
	return nil
}

// GetCurrentMetricSortKeyProvider returns the provider for creating sort keys
// for the current metric sort version. This is the single source of truth for metric sorting.
func GetCurrentMetricSortKeyProvider() filereader.SortKeyProvider {
	// This provider corresponds to lrdb.CurrentMetricSortVersion (SortVersionNameTidTimestamp)
	return &filereader.MetricSortKeyProvider{}
}

// GetStartEndTimes calculates the time range from a set of metric segments.
func GetStartEndTimes(rows []lrdb.MetricSeg) (int64, int64) {
	startTs := int64(math.MaxInt64)
	endTs := int64(math.MinInt64)
	for _, row := range rows {
		rowStartTs := row.TsRange.Lower.Int64
		rowEndTs := row.TsRange.Upper.Int64
		if rowStartTs < startTs {
			startTs = rowStartTs
		}
		if rowEndTs > endTs {
			endTs = rowEndTs
		}
	}
	return startTs, endTs
}

// GetIngestDateint returns the maximum ingest dateint from a set of metric segments.
func GetIngestDateint(rows []lrdb.MetricSeg) int32 {
	if len(rows) == 0 {
		return 0
	}
	ingestDateint := int32(0)
	for _, row := range rows {
		if row.IngestDateint > ingestDateint {
			ingestDateint = row.IngestDateint
		}
	}
	return ingestDateint
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

// UploadMetricResults uploads parquet files to S3 and updates the database with segment records.
// Returns the upload results containing segment IDs and dateints for each uploaded file.
func UploadMetricResults(
	ctx context.Context,
	storageClient cloudstorage.Client,
	mdb lrdb.StoreFull,
	results []parquetwriter.Result,
	params UploadParams,
) ([]UploadResult, error) {
	var uploadResults []UploadResult
	for _, result := range results {
		uploadResult, err := uploadSingleMetricResult(ctx, storageClient, mdb, result, params)
		if err != nil {
			return nil, fmt.Errorf("failed to upload result: %w", err)
		}
		uploadResults = append(uploadResults, uploadResult)
	}
	return uploadResults, nil
}

// UploadResult contains the result of uploading a single metric file
type UploadResult struct {
	SegmentID   int64
	DateInt     int32
	StartTs     int64
	EndTs       int64
	RecordCount int64
}

// uploadSingleMetricResult uploads a single parquet file result to S3 and database.
func uploadSingleMetricResult(
	ctx context.Context,
	storageClient cloudstorage.Client,
	mdb lrdb.StoreFull,
	result parquetwriter.Result,
	params UploadParams,
) (UploadResult, error) {
	ll := logctx.FromContext(ctx)

	// Safety check: should never get empty results from the writer
	if result.RecordCount == 0 {
		ll.Error("Received empty result from writer - this should not happen",
			slog.String("fileName", result.FileName),
			slog.Int64("recordCount", result.RecordCount))
		return UploadResult{}, fmt.Errorf("received empty result file with 0 records")
	}

	// Generate segment ID and object ID
	segmentID := idgen.GenerateID()

	// Extract dateint and hour from actual timestamp data
	filestats, err := ExtractFileMetadata(ctx, result)
	if err != nil {
		return UploadResult{}, fmt.Errorf("failed to extract file metadata: %w", err)
	}

	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return UploadResult{}, fmt.Errorf("invalid organization ID: %w", err)
	}
	objID := helpers.MakeDBObjectID(orgUUID, params.CollectorName, filestats.Dateint, filestats.Hour, segmentID, "metrics")

	// Upload to S3
	if err := storageClient.UploadObject(ctx, params.Bucket, objID, result.FileName); err != nil {
		return UploadResult{}, fmt.Errorf("uploading file to S3: %w", err)
	}

	ll.Debug("Metric segment stats",
		slog.String("objectID", objID),
		slog.Int64("segmentID", segmentID),
		slog.Int("fingerprintCount", len(filestats.Fingerprints)),
		slog.Int64("startTs", filestats.StartTs),
		slog.Int64("endTs", filestats.EndTs),
		slog.Int64("outputFileSize", result.FileSize),
		slog.Int64("recordCount", result.RecordCount))

	// Insert segment record
	err = mdb.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgUUID,
		FrequencyMs:    params.FrequencyMs,
		Dateint:        filestats.Dateint,
		IngestDateint:  params.IngestDateint,
		SegmentID:      segmentID,
		InstanceNum:    params.InstanceNum,
		SlotID:         0,
		SlotCount:      1,
		StartTs:        filestats.StartTs,
		EndTs:          filestats.EndTs,
		RecordCount:    result.RecordCount,
		FileSize:       result.FileSize,
		Published:      true,
		CreatedBy:      params.CreatedBy,
		Fingerprints:   filestats.Fingerprints,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	})
	if err != nil {
		// Clean up uploaded file on database error
		if err2 := storageClient.DeleteObject(ctx, params.Bucket, objID); err2 != nil {
			ll.Error("Failed to delete S3 object after insertion failure", slog.Any("error", err2))
		}
		return UploadResult{}, fmt.Errorf("inserting metric segment: %w", err)
	}

	return UploadResult{
		SegmentID:   segmentID,
		DateInt:     filestats.Dateint,
		StartTs:     filestats.StartTs,
		EndTs:       filestats.EndTs,
		RecordCount: result.RecordCount,
	}, nil
}

// UploadMetricResultsWithProcessedSegments uploads parquet files using ProcessedSegment approach
// and inserts them directly into the database. Returns ProcessedSegments for further use.
func UploadMetricResultsWithProcessedSegments(
	ctx context.Context,
	s3client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	results []parquetwriter.Result,
	params UploadParams,
) (ProcessedSegments, error) {
	ll := logctx.FromContext(ctx)

	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return nil, fmt.Errorf("invalid organization ID: %w", err)
	}

	segments := make(ProcessedSegments, 0, len(results))

	for _, result := range results {
		// Safety check: should never get empty results from the writer
		if result.RecordCount == 0 {
			ll.Error("Received empty result from writer - this should not happen",
				slog.String("fileName", result.FileName),
				slog.Int64("recordCount", result.RecordCount))
			return nil, fmt.Errorf("received empty result file with 0 records")
		}

		segment, err := NewProcessedSegment(ctx, result, orgUUID, params.CollectorName)
		if err != nil {
			return nil, fmt.Errorf("failed to create processed segment: %w", err)
		}

		// Upload to S3
		if err := segment.UploadToS3(ctx, s3client, params.Bucket); err != nil {
			return nil, fmt.Errorf("uploading file to S3: %w", err)
		}

		ll.Debug("Metric segment stats",
			slog.String("objectID", segment.ObjectID),
			slog.Int64("segmentID", segment.SegmentID),
			slog.Int("fingerprintCount", len(segment.Fingerprints)),
			slog.Int64("startTs", segment.StartTs),
			slog.Int64("endTs", segment.EndTs),
			slog.Int64("outputFileSize", segment.Result.FileSize),
			slog.Int64("recordCount", segment.Result.RecordCount))

		// Insert segment record
		err = mdb.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgUUID,
			FrequencyMs:    params.FrequencyMs,
			Dateint:        segment.GetDateint(),
			IngestDateint:  params.IngestDateint,
			SegmentID:      segment.SegmentID,
			InstanceNum:    params.InstanceNum,
			SlotID:         0,
			SlotCount:      1,
			StartTs:        segment.StartTs,
			EndTs:          segment.EndTs,
			RecordCount:    segment.Result.RecordCount,
			FileSize:       segment.Result.FileSize,
			Published:      true,
			CreatedBy:      params.CreatedBy,
			Fingerprints:   segment.Fingerprints,
			SortVersion:    lrdb.CurrentMetricSortVersion,
		})
		if err != nil {
			// Clean up uploaded file on database error
			if err2 := segment.ScheduleCleanupIfUploaded(ctx, mdb, orgUUID, params.InstanceNum, params.Bucket); err2 != nil {
				ll.Error("Failed to schedule S3 cleanup after insertion failure", slog.Any("error", err2))
			}
			return nil, fmt.Errorf("inserting metric segment: %w", err)
		}

		segments = append(segments, segment)
	}

	return segments, nil
}
