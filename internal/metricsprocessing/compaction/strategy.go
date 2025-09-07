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

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/accumulation"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionStrategy implements the Strategy interface for compaction
type CompactionStrategy struct {
	Config Config
}

// NewCompactionStrategy creates a new compaction strategy
func NewCompactionStrategy(cfg Config) *CompactionStrategy {
	return &CompactionStrategy{
		Config: cfg,
	}
}

// GetAccumulationKey returns the accumulation key for a notification
func (s *CompactionStrategy) GetAccumulationKey(notification *messages.MetricSegmentNotificationMessage) accumulation.AccumulationKey {
	return accumulation.AccumulationKey{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    notification.FrequencyMs,
		InstanceNum:    notification.InstanceNum,
		SlotID:         0, // Compaction doesn't use slots
		SlotCount:      1, // Compaction is terminal, single slot
	}
}

// ShouldProcess determines if we should process this notification
func (s *CompactionStrategy) ShouldProcess(notification *messages.MetricSegmentNotificationMessage) bool {
	// Check if we should process this frequency
	return helpers.IsWantedFrequency(notification.FrequencyMs)
}

// IsAlreadyOptimized checks if the segment is already well-sized
func (s *CompactionStrategy) IsAlreadyOptimized(notification *messages.MetricSegmentNotificationMessage, segment lrdb.MetricSeg, rpfEstimate int64, targetFileSize int64) bool {
	ll := logctx.FromContext(context.Background())

	// Check if already compacted
	if segment.Compacted {
		ll.Info("Segment already compacted",
			slog.Int64("segmentID", segment.SegmentID))
		return true
	}

	// Check if segment is already large enough (80% threshold)
	fileSizeThreshold := targetFileSize * 80 / 100
	if notification.FileSize >= fileSizeThreshold || notification.RecordCount >= rpfEstimate {
		ll.Info("Segment already well-sized, marking as compacted",
			slog.Int64("segmentID", notification.SegmentID),
			slog.Int64("fileSize", notification.FileSize),
			slog.Int64("recordCount", notification.RecordCount))

		// Note: We can't mark as compacted here since we don't have the Store
		// The caller should handle this case
		return true
	}

	return false
}

// GetTargetFrequency returns the target frequency (same as source for compaction)
func (s *CompactionStrategy) GetTargetFrequency(key accumulation.AccumulationKey) int32 {
	return key.FrequencyMs
}

// MarkSegmentsProcessed marks segments as compacted in the database
func (s *CompactionStrategy) MarkSegmentsProcessed(ctx context.Context, db accumulation.Store, params accumulation.ProcessedParams) error {
	// Collect old segment records
	var oldRecords []lrdb.CompactMetricSegsOld
	for _, seg := range params.OldRecords {
		oldRecords = append(oldRecords, lrdb.CompactMetricSegsOld{
			SegmentID: seg.SegmentID,
			SlotID:    seg.SlotID,
		})
	}

	// Prepare new records
	newRecords := make([]lrdb.CompactMetricSegsNew, len(params.NewRecords))
	for i, seg := range params.NewRecords {
		newRecords[i] = lrdb.CompactMetricSegsNew{
			SegmentID:    seg.SegmentID,
			StartTs:      seg.TsRange.Lower.Int64,
			EndTs:        seg.TsRange.Upper.Int64,
			RecordCount:  seg.RecordCount,
			FileSize:     seg.FileSize,
			Fingerprints: seg.Fingerprints,
		}
	}

	// Update database atomically - use current dateint for when we added this data to our index
	now := time.Now()
	currentDateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	err := db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: params.Key.OrganizationID,
		Dateint:        params.Key.Dateint,
		InstanceNum:    params.Key.InstanceNum,
		SlotID:         0, // compaction is terminal, single slot
		SlotCount:      1,
		IngestDateint:  currentDateint,
		FrequencyMs:    params.Key.FrequencyMs,
		CreatedBy:      lrdb.CreatedByCompact,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
	})

	return err
}

// GetSourceSegments fetches the source segments for compaction
func (s *CompactionStrategy) GetSourceSegments(ctx context.Context, db accumulation.Store, notification *messages.MetricSegmentNotificationMessage) ([]lrdb.MetricSeg, error) {
	ll := logctx.FromContext(ctx)

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
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if already compacted
	if segment.Compacted {
		return nil, nil
	}

	// Check if we should mark as compacted directly
	rpfEstimate := db.GetMetricEstimate(ctx, notification.OrganizationID, notification.FrequencyMs)
	if s.IsAlreadyOptimized(notification, segment, rpfEstimate, s.Config.TargetFileSizeBytes) {
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
			return nil, fmt.Errorf("failed to mark segment as compacted: %w", err)
		}
		return nil, nil
	}

	return []lrdb.MetricSeg{segment}, nil
}

// GetOutputTopics returns the output Kafka topics for compacted segments
func (s *CompactionStrategy) GetOutputTopics(key accumulation.AccumulationKey) []string {
	// Compaction doesn't produce Kafka output - it updates the same segments
	return nil
}

// GetConsumerGroup returns the consumer group name
func (s *CompactionStrategy) GetConsumerGroup() string {
	return "lakerunner.compact.metrics"
}

// GetSourceTopic returns the source Kafka topic
func (s *CompactionStrategy) GetSourceTopic() string {
	return "lakerunner.segments.metrics.compact"
}

// GetMaxAccumulationTime returns the max accumulation time
func (s *CompactionStrategy) GetMaxAccumulationTime() string {
	return s.Config.MaxAccumulationTime
}

// GetTargetFileSizeBytes returns the target file size in bytes
func (s *CompactionStrategy) GetTargetFileSizeBytes() int64 {
	return s.Config.TargetFileSizeBytes
}
