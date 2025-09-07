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

package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessAccumulatedRollup processes all accumulated rollup work
func ProcessAccumulatedRollup(
	ctx context.Context,
	manager *RollupManager,
	db rollupStore,
	sp storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	consumerGroup string,
	topic string,
) error {
	ll := logctx.FromContext(ctx)

	if !manager.HasWork() {
		ll.Debug("No rollup work to process")
		// Still need to update Kafka offsets if any
		return updateKafkaOffsets(ctx, db, manager, consumerGroup, topic)
	}

	startTime := time.Now()
	ll.Info("Starting accumulated rollup processing")

	// Process each accumulator
	for key, accumulator := range manager.GetAccumulators() {
		work := accumulator.GetWork()
		if len(work) == 0 {
			continue
		}

		ll.Debug("Processing rollup accumulator",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)),
			slog.Int("sourceFrequencyMs", int(key.SourceFrequency)),
			slog.Int("slotID", int(key.SlotID)),
			slog.Int("workCount", len(work)))

		// Get target frequency for this rollup
		targetFrequency, exists := config.GetTargetRollupFrequency(key.SourceFrequency)
		if !exists {
			ll.Error("Invalid source frequency for rollup",
				slog.Int("frequencyMs", int(key.SourceFrequency)))
			continue
		}

		// Get storage profile from first work item
		if len(work) == 0 {
			continue
		}
		profile := work[0].Profile

		// Create blob client for this org/instance
		blobclient, err := cloudstorage.NewClient(ctx, cmgr, profile)
		if err != nil {
			ll.Error("Failed to create storage client",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to create storage client: %w", err)
		}

		// Process the accumulator
		if err := FlushRollupAccumulator(ctx, accumulator, db, blobclient, manager.GetTmpDir(), targetFrequency); err != nil {
			ll.Error("Failed to flush rollup accumulator",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to flush rollup accumulator: %w", err)
		}
	}

	// Update Kafka offsets after successful processing
	if err := updateKafkaOffsets(ctx, db, manager, consumerGroup, topic); err != nil {
		return fmt.Errorf("failed to update Kafka offsets: %w", err)
	}

	ll.Info("Completed accumulated rollup processing",
		slog.Duration("duration", time.Since(startTime)))

	// Reset the manager for the next accumulation cycle
	if err := manager.Reset(); err != nil {
		ll.Error("Failed to reset rollup manager", slog.Any("error", err))
		// Don't fail the overall operation for reset errors
	}

	return nil
}

// updateKafkaOffsets updates Kafka offsets in the database
func updateKafkaOffsets(ctx context.Context, db rollupStore, manager *RollupManager, consumerGroup, topic string) error {
	offsetUpdates := manager.GetKafkaOffsetUpdates(consumerGroup, topic)
	for _, update := range offsetUpdates {
		if err := db.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       update.ConsumerGroup,
			Topic:               update.Topic,
			Partition:           update.Partition,
			LastProcessedOffset: update.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset for partition %d: %w", update.Partition, err)
		}
	}
	return nil
}

// AddRollupWorkFromKafka processes a Kafka message and adds it to the accumulator
func AddRollupWorkFromKafka(
	ctx context.Context,
	notification *messages.MetricSegmentNotificationMessage,
	manager *RollupManager,
	db rollupStore,
	sp storageprofile.StorageProfileProvider,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Check if this frequency should generate rollup work
	if !config.IsRollupSourceFrequency(notification.FrequencyMs) {
		ll.Debug("Frequency not eligible for rollup",
			slog.Int("frequencyMs", int(notification.FrequencyMs)))
		return nil
	}

	// Fetch the actual segment
	segment, err := db.GetMetricSegByPrimaryKey(ctx, lrdb.GetMetricSegByPrimaryKeyParams{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    notification.FrequencyMs,
		SegmentID:      notification.SegmentID,
		InstanceNum:    notification.InstanceNum,
		SlotID:         notification.SlotID,
		SlotCount:      notification.SlotCount,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			ll.Warn("Segment not found for rollup",
				slog.Int64("segmentID", notification.SegmentID))
			return nil
		}
		return fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if segment is already rolled up (optional - could add a rolled_up flag)
	// For now, we'll process all segments

	// Get storage profile
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, notification.OrganizationID, notification.InstanceNum)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Create rollup key
	key := RollupKey{
		OrganizationID:  notification.OrganizationID,
		Dateint:         notification.DateInt,
		SourceFrequency: notification.FrequencyMs,
		InstanceNum:     notification.InstanceNum,
		SlotID:          notification.SlotID,
		SlotCount:       notification.SlotCount,
	}

	// Add to accumulator
	manager.AddRollupWork(ctx, segment, profile, key, kafkaOffset)

	ll.Debug("Added rollup work to accumulator",
		slog.Int64("segmentID", notification.SegmentID),
		slog.String("organizationID", notification.OrganizationID.String()))

	return nil
}

// FlushRollupAccumulator processes a single accumulator and performs rollup
func FlushRollupAccumulator(
	ctx context.Context,
	acc *RollupAccumulator,
	db rollupStore,
	blobclient cloudstorage.Client,
	tmpDir string,
	targetFrequency int32,
) error {

	work := acc.GetWork()
	if len(work) == 0 {
		return nil
	}

	// Always create a fresh writer manager for each flush cycle
	// to avoid reusing closed writers
	if acc.writerManager != nil {
		// Clean up any previous writer manager
		_ = acc.writerManager.Close(ctx)
	}
	acc.writerManager = NewRollupWriterManager(tmpDir, acc.rpfEstimate)

	// Collect source segments
	sourceSegments := make([]lrdb.MetricSeg, len(work))
	for i, w := range work {
		sourceSegments[i] = w.Segment
	}

	// Use first work item's profile (all should be same for this key)
	profile := work[0].Profile

	// Create reader stack from segments
	readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpDir, blobclient, acc.key.OrganizationID, profile, sourceSegments)
	if err != nil {
		return fmt.Errorf("creating reader stack: %w", err)
	}
	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	// Process through writer manager for rollup
	if err := acc.writerManager.ProcessRollup(ctx, readerStack.Readers, acc.key, targetFrequency); err != nil {
		return fmt.Errorf("processing rollup: %w", err)
	}

	// Flush all writers and get results
	result, err := acc.writerManager.FlushAll(ctx)
	if err != nil {
		return fmt.Errorf("flushing writers: %w", err)
	}

	// Upload results and update database
	if err := uploadRollupResults(ctx, db, blobclient, acc.key, sourceSegments, result, profile, targetFrequency); err != nil {
		return fmt.Errorf("uploading rollup results: %w", err)
	}

	// Clear writer manager reference - it's already closed from FlushAll
	acc.writerManager = nil

	return nil
}

// uploadRollupResults handles uploading rolled-up segments and database updates
func uploadRollupResults(
	ctx context.Context,
	db rollupStore,
	blobclient cloudstorage.Client,
	key RollupKey,
	sourceSegments []lrdb.MetricSeg,
	result metricsprocessing.ProcessingResult,
	profile storageprofile.StorageProfile,
	targetFrequency int32,
) error {
	ll := logctx.FromContext(ctx)

	if len(result.RawResults) == 0 {
		ll.Warn("No output files from rollup",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)))
		return nil
	}

	// Collect source segment IDs
	sourceSegmentIDs := make([]int64, len(sourceSegments))
	for i, seg := range sourceSegments {
		sourceSegmentIDs[i] = seg.SegmentID
	}

	// Upload segments
	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, result.RawResults, key.OrganizationID, profile.CollectorName)
	if err != nil {
		return fmt.Errorf("creating segments: %w", err)
	}

	uploadedSegments, err := metricsprocessing.UploadSegments(ctx, blobclient, profile.Bucket, segments)
	if err != nil {
		if len(uploadedSegments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(uploadedSegments)))
			uploadedSegments.ScheduleCleanupAll(ctx, db, key.OrganizationID, key.InstanceNum, profile.Bucket)
		}
		return fmt.Errorf("failed to upload rolled-up files: %w", err)
	}

	// Prepare rollup records
	newRecords := make([]lrdb.RollupNewRecord, len(uploadedSegments))
	for i, segment := range uploadedSegments {
		newRecords[i] = lrdb.RollupNewRecord{
			SegmentID:    segment.SegmentID,
			StartTs:      segment.StartTs,
			EndTs:        segment.EndTs,
			RecordCount:  segment.Result.RecordCount,
			FileSize:     segment.Result.FileSize,
			Fingerprints: segment.Fingerprints,
		}
	}

	// Create rollup parameters
	sourceParams := lrdb.RollupSourceParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.Dateint,
		FrequencyMs:    key.SourceFrequency,
		InstanceNum:    key.InstanceNum,
	}

	targetParams := lrdb.RollupTargetParams{
		OrganizationID: key.OrganizationID,
		Dateint:        CalculateTargetDateint(key.Dateint, targetFrequency),
		FrequencyMs:    targetFrequency,
		InstanceNum:    key.InstanceNum,
		SlotID:         key.SlotID,
		SlotCount:      key.SlotCount,
		IngestDateint:  GetIngestDateint(sourceSegments),
		SortVersion:    lrdb.CurrentMetricSortVersion,
	}

	// Get Kafka offsets for this batch
	// Note: In a real implementation, we'd track which offsets correspond to which segments
	// For now, we'll use the atomic transaction without offset tracking
	// TODO: Implement RollupMetricSegsWithKafkaOffsets when available
	err = db.RollupMetricSegs(ctx, sourceParams, targetParams, sourceSegmentIDs, newRecords)
	if err != nil {
		return fmt.Errorf("failed to rollup metric segments: %w", err)
	}

	var outputRecords, outputBytes int64
	for _, segment := range uploadedSegments {
		outputRecords += segment.Result.RecordCount
		outputBytes += segment.Result.FileSize
	}

	ll.Info("Rollup complete",
		slog.Int("sourceSegments", len(sourceSegments)),
		slog.Int("targetSegments", len(uploadedSegments)),
		slog.Int("sourceFrequencyMs", int(key.SourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
		slog.Int64("outputRecords", outputRecords),
		slog.Int64("outputBytes", outputBytes))

	return nil
}

// CalculateTargetDateint calculates the target dateint for rollup
func CalculateTargetDateint(sourceDateint int32, targetFrequencyMs int32) int32 {
	// For now, keep the same dateint
	// In the future, we might want to adjust based on frequency boundaries
	return sourceDateint
}

// GetIngestDateint gets the current dateint for when we ingested this rollup
func GetIngestDateint(sourceSegments []lrdb.MetricSeg) int32 {
	// Use current dateint for when we added this data to our index
	now := time.Now()
	return int32(now.Year()*10000 + int(now.Month())*100 + now.Day())
}
