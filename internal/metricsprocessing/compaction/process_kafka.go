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
	"os"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessKafkaMessage processes a single metric segment notification from Kafka
func ProcessKafkaMessage(
	ctx context.Context,
	notification *messages.MetricSegmentNotificationMessage,
	db CompactionStore,
	sp storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg Config,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Generate batch ID for tracking
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(
		slog.String("batchID", batchID),
		slog.String("organizationID", notification.OrganizationID.String()),
		slog.Int("dateint", int(notification.DateInt)),
		slog.Int("frequencyMs", int(notification.FrequencyMs)),
		slog.Int64("segmentID", notification.SegmentID),
		slog.Int("slotID", int(notification.SlotID)),
		slog.Int("slotCount", int(notification.SlotCount)),
		slog.Int64("recordCount", notification.RecordCount),
		slog.Int64("fileSize", notification.FileSize),
	)
	ctx = logctx.WithLogger(ctx, ll)

	// Check if this is a wanted frequency
	if !helpers.IsWantedFrequency(int32(notification.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency")
		// Still need to update Kafka offset to mark as processed
		if kafkaOffset != nil {
			return updateKafkaOffset(ctx, db, kafkaOffset)
		}
		return nil
	}

	// Get RPF estimate for this org/frequency to determine if we should skip compaction
	rpfEstimate := db.GetMetricEstimate(ctx, notification.OrganizationID, int32(notification.FrequencyMs))
	if rpfEstimate == 0 {
		rpfEstimate = 40_000 // Default fallback
	}

	// Short-circuit check: Skip compaction for segments that are already at or near target size
	// This avoids unnecessary compaction of segments that are already well-sized
	// Check if segment is already large enough to skip compaction (80% of target)
	fileSizeThreshold := cfg.TargetFileSizeBytes * 80 / 100 // 80% of target size

	if notification.FileSize >= fileSizeThreshold || notification.RecordCount >= rpfEstimate {
		ll.Info("Short-circuiting compaction for already-optimized segment",
			slog.Int64("segmentID", notification.SegmentID),
			slog.Int64("fileSize", notification.FileSize),
			slog.Int64("fileSizeThreshold", fileSizeThreshold),
			slog.Int64("recordCount", notification.RecordCount),
			slog.Int64("rpfEstimate", rpfEstimate))

		// Mark the segment as compacted using single-segment update with full primary key
		// We do NOT update Kafka offset here - if we replay, we'll make the same decision
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
			ll.Error("Failed to mark already-optimized segment as compacted",
				slog.Int64("segmentID", notification.SegmentID),
				slog.Any("error", err))
			return fmt.Errorf("failed to mark segment as compacted: %w", err)
		}

		// NOTE: We intentionally do NOT update the Kafka offset here
		// This allows replay safety - if we replay this message, we'll make the same decision
		return nil
	}

	// Get storage profile
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, notification.OrganizationID, notification.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Create blob client
	blobclient, err := cloudstorage.NewClient(ctx, cmgr, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Create temporary directory
	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	ll.Info("Processing metric compaction from Kafka notification")

	// Fetch the single segment specified in the notification
	segment, err := fetchSegment(ctx, db, notification)
	if err != nil {
		ll.Error("Failed to fetch segment", slog.Any("error", err))
		return fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if segment exists
	if segment == nil {
		ll.Warn("Segment not found for compaction")
		// Still need to update Kafka offset to avoid reprocessing
		if kafkaOffset != nil {
			return updateKafkaOffset(ctx, db, kafkaOffset)
		}
		return nil
	}

	// Check if already compacted
	if segment.Compacted {
		ll.Info("Segment already compacted, nothing to do",
			slog.Int64("segmentID", segment.SegmentID))
		if kafkaOffset != nil {
			return updateKafkaOffset(ctx, db, kafkaOffset)
		}
		return nil
	}

	// Create a slice with the single segment for processing
	validSegments := []lrdb.MetricSeg{*segment}

	// Create metadata for the compaction work
	metadata := CompactionWorkMetadata{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    int32(notification.FrequencyMs),
		InstanceNum:    notification.InstanceNum,
	}

	// Perform the actual compaction
	err = performCompaction(ctx, db, tmpdir, profile, blobclient, validSegments, metadata, kafkaOffset)
	if err != nil {
		ll.Error("Failed to perform compaction", slog.Any("error", err))
		return fmt.Errorf("compaction failed: %w", err)
	}

	ll.Info("Successfully completed compaction from Kafka notification",
		slog.Int("segmentsCompacted", len(validSegments)))

	return nil
}

// fetchSegment retrieves a single segment using the primary key fields from the notification
func fetchSegment(ctx context.Context, db CompactionStore, notification *messages.MetricSegmentNotificationMessage) (*lrdb.MetricSeg, error) {
	// Use the single-row fetcher with all primary key fields
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
		// Check if it's a not found error
		if err == sql.ErrNoRows {
			// Segment not found
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch segment: %w", err)
	}

	return &segment, nil
}

// updateKafkaOffset updates the Kafka offset in the database
func updateKafkaOffset(ctx context.Context, db CompactionStore, kafkaOffset *lrdb.KafkaOffsetUpdate) error {
	// Update the Kafka offset to mark this message as processed
	return db.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
		ConsumerGroup:       kafkaOffset.ConsumerGroup,
		Topic:               kafkaOffset.Topic,
		Partition:           kafkaOffset.Partition,
		LastProcessedOffset: kafkaOffset.Offset,
	})
}

// performCompaction performs the actual compaction work
func performCompaction(
	ctx context.Context,
	db CompactionStore,
	tmpdir string,
	profile storageprofile.StorageProfile,
	blobclient cloudstorage.Client,
	segments []lrdb.MetricSeg,
	metadata CompactionWorkMetadata,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Get the metric estimate for this organization and frequency
	estimatedTarget := db.GetMetricEstimate(ctx, metadata.OrganizationID, metadata.FrequencyMs)

	// Use the existing coordinate function to perform the actual compaction
	// For a single segment, this will just mark it as compacted
	// For multiple segments (shouldn't happen with Kafka), it would merge them
	err := coordinate(ctx, db, tmpdir, metadata, profile, blobclient, segments, estimatedTarget)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Update Kafka offset after successful compaction
	if kafkaOffset != nil {
		err = updateKafkaOffset(ctx, db, kafkaOffset)
		if err != nil {
			// This is a critical error - we've done the work but can't record the offset
			// The message will be reprocessed, but the compaction check will prevent duplicate work
			ll.Error("Failed to update Kafka offset after successful compaction",
				slog.Any("error", err),
				slog.String("topic", kafkaOffset.Topic),
				slog.Int("partition", int(kafkaOffset.Partition)),
				slog.Int64("offset", kafkaOffset.Offset))
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
	}

	return nil
}
