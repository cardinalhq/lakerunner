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
	// This should be done in the same transaction as the compaction results
	// For now, this is a placeholder
	return nil
}

// KafkaCompactionMetadata extends CompactionWorkMetadata with Kafka-specific fields
type KafkaCompactionMetadata struct {
	CompactionWorkMetadata
	SlotID    int32
	SlotCount int32
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
	// This would call the existing compaction logic
	// For now, this is a placeholder that needs to be implemented
	// It should:
	// 1. Download and merge the segments
	// 2. Create compacted output
	// 3. Upload to storage
	// 4. Update database with results and Kafka offset in same transaction
	return fmt.Errorf("performCompaction not yet implemented")
}
