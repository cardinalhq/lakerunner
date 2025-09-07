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

package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionWorkMetadata holds the basic metadata needed for compaction work
type CompactionWorkMetadata struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
	IngestDateint  int32
}

// coordinateWithKafkaOffset performs compaction with atomic Kafka offset updates
func coordinateWithKafkaOffset(
	ctx context.Context,
	mdb CompactionStore,
	tmpdir string,
	metadata CompactionWorkMetadata,
	profile storageprofile.StorageProfile,
	blobclient cloudstorage.Client,
	oldRows []lrdb.MetricSeg,
	estimatedTargetRecords int64,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Calculate IngestDateint from segments if not already set
	if metadata.IngestDateint == 0 {
		metadata.IngestDateint = metricsprocessing.GetIngestDateint(oldRows)
	}

	readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpdir, blobclient, metadata.OrganizationID, profile, oldRows)
	if err != nil {
		return err
	}
	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	inputBytes := int64(0)
	inputRecords := int64(0)
	for _, row := range oldRows {
		inputBytes += row.FileSize
		inputRecords += row.RecordCount
	}

	processingInput := metricsprocessing.ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: metadata.FrequencyMs,
		TmpDir:            tmpdir,
		RecordsLimit:      estimatedTargetRecords * 2,
		EstimatedRecords:  estimatedTargetRecords,
		Action:            "compact",
		InputRecords:      inputRecords,
		InputBytes:        inputBytes,
	}

	processingResult, err := metricsprocessing.AggregateMetrics(ctx, processingInput)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	if processingResult.Stats.OutputSegments == 0 {
		ll.Warn("Produced 0 output files from aggregating reader")
		return nil
	}

	// Upload files to S3 with deadline
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer s3Cancel()

	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, processingResult.RawResults, metadata.OrganizationID, profile.CollectorName)
	if err != nil {
		return fmt.Errorf("failed to create processed segments: %w", err)
	}

	uploadedSegments, err := metricsprocessing.UploadSegments(s3Ctx, blobclient, profile.Bucket, segments)
	if err != nil {
		if len(uploadedSegments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(uploadedSegments)),
				slog.Any("error", err))
			uploadedSegments.ScheduleCleanupAll(ctx, mdb, metadata.OrganizationID, metadata.InstanceNum, profile.Bucket)
		}
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	segments = uploadedSegments

	oldRecords := make([]lrdb.CompactMetricSegsOld, len(oldRows))
	for i, row := range oldRows {
		oldRecords[i] = lrdb.CompactMetricSegsOld{
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
		}
	}

	newRecords := make([]lrdb.CompactMetricSegsNew, len(segments))
	for i, segment := range segments {
		newRecords[i] = lrdb.CompactMetricSegsNew{
			SegmentID:    segment.SegmentID,
			StartTs:      segment.StartTs,
			EndTs:        segment.EndTs,
			RecordCount:  segment.Result.RecordCount,
			FileSize:     segment.Result.FileSize,
			Fingerprints: segment.Fingerprints,
		}
	}

	var kafkaOffsets []lrdb.KafkaOffsetUpdate
	if kafkaOffset != nil {
		kafkaOffsets = []lrdb.KafkaOffsetUpdate{*kafkaOffset}
	}

	// Use the new atomic transaction method
	err = mdb.CompactMetricSegsWithKafkaOffsets(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: metadata.OrganizationID,
		Dateint:        metadata.Dateint,
		InstanceNum:    metadata.InstanceNum,
		SlotID:         0, // compaction is terminal, so we can just write to a single slot here.
		SlotCount:      1,
		IngestDateint:  metadata.IngestDateint,
		FrequencyMs:    metadata.FrequencyMs,
		CreatedBy:      lrdb.CreatedByCompact,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
	}, kafkaOffsets)

	if err != nil {
		return fmt.Errorf("failed to compact metric segments with kafka offsets: %w", err)
	}

	var outputRecords, outputBytes int64
	for _, segment := range segments {
		outputRecords += segment.Result.RecordCount
		outputBytes += segment.Result.FileSize
	}

	ll.Info("Compaction complete with Kafka offset update",
		slog.Int("inputSegments", len(oldRows)),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("outputRecords", outputRecords),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", outputBytes),
		slog.Int64("targetRecords", estimatedTargetRecords))

	return nil
}
