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

	ll = ll.With(
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

	if !helpers.IsWantedFrequency(int32(notification.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency")
		return nil
	}

	// We do not need a fallback, GetMetricEstimate will return a default for us if needed.
	rpfEstimate := db.GetMetricEstimate(ctx, notification.OrganizationID, int32(notification.FrequencyMs))

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

		// If we replay, we will make the same decision again, which is idempotent.
		// The kafka offset will be updated when we process a batch of segments that includes this one.
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

	segment, err := fetchSegment(ctx, db, notification)
	if err != nil {
		ll.Error("Failed to fetch segment", slog.Any("error", err))
		return fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if segment exists.  We will not update the offset here, that
	// will still happen after our processing batch selection and windows close.
	if segment == nil {
		ll.Warn("Segment not found for compaction")
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

	validSegments := []lrdb.MetricSeg{*segment}

	metadata := CompactionWorkMetadata{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    int32(notification.FrequencyMs),
		InstanceNum:    notification.InstanceNum,
		IngestDateint:  segment.IngestDateint,
	}

	estimatedTarget := db.GetMetricEstimate(ctx, metadata.OrganizationID, metadata.FrequencyMs)

	err = coordinateWithKafkaOffset(ctx, db, tmpdir, metadata, profile, blobclient, validSegments, estimatedTarget, kafkaOffset)
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
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch segment: %w", err)
	}

	return &segment, nil
}

// updateKafkaOffset updates the Kafka offset in the database
func updateKafkaOffset(ctx context.Context, db CompactionStore, kafkaOffset *lrdb.KafkaOffsetUpdate) error {
	return db.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
		ConsumerGroup:       kafkaOffset.ConsumerGroup,
		Topic:               kafkaOffset.Topic,
		Partition:           kafkaOffset.Partition,
		LastProcessedOffset: kafkaOffset.Offset,
	})
}
