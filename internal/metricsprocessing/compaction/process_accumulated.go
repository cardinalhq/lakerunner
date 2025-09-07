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
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessAccumulatedCompaction processes all accumulated compaction work
func ProcessAccumulatedCompaction(
	ctx context.Context,
	manager *CompactionManager,
	db CompactionStore,
	sp storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	consumerGroup string,
	topic string,
) error {
	ll := logctx.FromContext(ctx)

	// Always reset at the end, regardless of success or failure
	defer func() {
		if err := manager.Reset(); err != nil {
			ll.Error("Failed to reset compaction manager", slog.Any("error", err))
		}
	}()

	if !manager.HasWork() {
		ll.Debug("No compaction work to process")
		// Still need to update Kafka offsets if any
		return updateKafkaOffsets(ctx, db, manager, consumerGroup, topic)
	}

	startTime := time.Now()
	ll.Info("Starting accumulated compaction processing")

	// Process each accumulator
	for key, accumulator := range manager.GetAccumulators() {
		work := accumulator.GetWork()
		if len(work) == 0 {
			continue
		}

		ll.Debug("Processing compaction accumulator",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)),
			slog.Int("frequencyMs", int(key.FrequencyMs)),
			slog.Int("workCount", len(work)))

		// Get storage profile from first work item
		profile := work[0].Profile

		// Create blob client for this org/instance
		blobclient, err := cloudstorage.NewClient(ctx, cmgr, profile)
		if err != nil {
			ll.Error("Failed to create storage client",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to create storage client: %w", err)
		}

		// Process the accumulator (uses cached RPF estimate)
		if err := FlushAccumulator(ctx, accumulator, db, blobclient, manager.GetTmpDir()); err != nil {
			ll.Error("Failed to flush accumulator",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to flush accumulator: %w", err)
		}
	}

	// Update Kafka offsets after successful processing
	if err := updateKafkaOffsets(ctx, db, manager, consumerGroup, topic); err != nil {
		return fmt.Errorf("failed to update Kafka offsets: %w", err)
	}

	ll.Info("Completed accumulated compaction processing",
		slog.Duration("duration", time.Since(startTime)))

	// Note: Reset is called in the defer at the beginning of this function
	return nil
}

// updateKafkaOffsets updates Kafka offsets in the database
func updateKafkaOffsets(ctx context.Context, db CompactionStore, manager *CompactionManager, consumerGroup, topic string) error {
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

// AddCompactionWorkFromKafka processes a Kafka message and adds it to the accumulator
func AddCompactionWorkFromKafka(
	ctx context.Context,
	notification *messages.MetricSegmentNotificationMessage,
	manager *CompactionManager,
	db CompactionStore,
	sp storageprofile.StorageProfileProvider,
	cfg Config,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Check if we should process this frequency
	if !helpers.IsWantedFrequency(int32(notification.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency",
			slog.Int("frequencyMs", int(notification.FrequencyMs)))
		return nil
	}

	// Get RPF estimate
	rpfEstimate := db.GetMetricEstimate(ctx, notification.OrganizationID, int32(notification.FrequencyMs))

	// Check if segment is already large enough (80% threshold)
	fileSizeThreshold := cfg.TargetFileSizeBytes * 80 / 100
	if notification.FileSize >= fileSizeThreshold || notification.RecordCount >= rpfEstimate {
		ll.Info("Segment already well-sized, marking as compacted",
			slog.Int64("segmentID", notification.SegmentID),
			slog.Int64("fileSize", notification.FileSize),
			slog.Int64("recordCount", notification.RecordCount))

		// Mark as compacted directly
		err := db.SetSingleMetricSegCompacted(ctx, lrdb.SetSingleMetricSegCompactedParams{
			OrganizationID: notification.OrganizationID,
			Dateint:        notification.DateInt,
			FrequencyMs:    notification.FrequencyMs,
			SegmentID:      notification.SegmentID,
			InstanceNum:    notification.InstanceNum,
			SlotID:         notification.SlotID,
			SlotCount:      notification.SlotCount,
		})
		if err != nil {
			return fmt.Errorf("failed to mark segment as compacted: %w", err)
		}
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
			ll.Warn("Segment not found for compaction",
				slog.Int64("segmentID", notification.SegmentID))
			return nil
		}
		return fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if already compacted
	if segment.Compacted {
		ll.Info("Segment already compacted",
			slog.Int64("segmentID", segment.SegmentID))
		return nil
	}

	// Get storage profile
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, notification.OrganizationID, notification.InstanceNum)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Add to accumulator
	manager.AddCompactionWork(ctx, notification, []lrdb.MetricSeg{segment}, profile, kafkaOffset)

	return nil
}
