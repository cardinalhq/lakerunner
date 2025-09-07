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
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/accumulation"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Config holds configuration for rollup
type Config struct {
	TargetFileSizeBytes int64  // Target file size in bytes for rollup
	MaxAccumulationTime string // Maximum time to accumulate work before flushing
}

// RollupStrategy implements the Strategy interface for rollup
type RollupStrategy struct {
	Config Config
}

// NewRollupStrategy creates a new rollup strategy
func NewRollupStrategy(cfg Config) *RollupStrategy {
	return &RollupStrategy{
		Config: cfg,
	}
}

// GetAccumulationKey returns the accumulation key for a notification
func (s *RollupStrategy) GetAccumulationKey(notification *messages.MetricSegmentNotificationMessage) accumulation.AccumulationKey {
	return accumulation.AccumulationKey{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    notification.FrequencyMs, // Source frequency - target will be calculated
		InstanceNum:    notification.InstanceNum,
		SlotID:         notification.SlotID,
		SlotCount:      notification.SlotCount,
	}
}

// ShouldProcess determines if we should process this notification
func (s *RollupStrategy) ShouldProcess(notification *messages.MetricSegmentNotificationMessage) bool {
	// Check if this frequency should generate rollup work
	return config.IsRollupSourceFrequency(notification.FrequencyMs)
}

// IsAlreadyOptimized checks if the segment is already rolled up
func (s *RollupStrategy) IsAlreadyOptimized(notification *messages.MetricSegmentNotificationMessage, segment lrdb.MetricSeg, rpfEstimate int64, targetFileSize int64) bool {
	return segment.Rolledup
}

// GetTargetFrequency returns the target frequency for rollup
func (s *RollupStrategy) GetTargetFrequency(key accumulation.AccumulationKey) int32 {
	targetFrequency, exists := config.GetTargetRollupFrequency(key.FrequencyMs)
	if !exists {
		ll := logctx.FromContext(context.Background())
		ll.Error("Invalid source frequency for rollup",
			slog.Int("frequencyMs", int(key.FrequencyMs)))
		return key.FrequencyMs // Fallback to source frequency
	}
	return targetFrequency
}

// MarkSegmentsProcessed marks segments as rolled up in the database
func (s *RollupStrategy) MarkSegmentsProcessed(ctx context.Context, db accumulation.Store, params accumulation.ProcessedParams) error {
	// Collect source segment IDs
	sourceSegmentIDs := make([]int64, len(params.OldRecords))
	for i, seg := range params.OldRecords {
		sourceSegmentIDs[i] = seg.SegmentID
	}

	// Prepare rollup records
	newRecords := make([]lrdb.RollupNewRecord, len(params.NewRecords))
	for i, seg := range params.NewRecords {
		newRecords[i] = lrdb.RollupNewRecord{
			SegmentID:    seg.SegmentID,
			StartTs:      seg.TsRange.Lower.Int64,
			EndTs:        seg.TsRange.Upper.Int64,
			RecordCount:  seg.RecordCount,
			FileSize:     seg.FileSize,
			Fingerprints: seg.Fingerprints,
		}
	}

	// Get target frequency
	targetFrequency := s.GetTargetFrequency(params.Key)

	// Create rollup parameters
	sourceParams := lrdb.RollupSourceParams{
		OrganizationID: params.Key.OrganizationID,
		Dateint:        params.Key.Dateint,
		FrequencyMs:    params.Key.FrequencyMs,
		InstanceNum:    params.Key.InstanceNum,
	}

	targetDateint := params.Key.Dateint

	// Get current dateint for when we ingested this rollup
	now := time.Now()
	ingestDateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	targetParams := lrdb.RollupTargetParams{
		OrganizationID: params.Key.OrganizationID,
		Dateint:        targetDateint,
		FrequencyMs:    targetFrequency,
		InstanceNum:    params.Key.InstanceNum,
		SlotID:         params.Key.SlotID,
		SlotCount:      params.Key.SlotCount,
		IngestDateint:  ingestDateint,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	}

	err := db.RollupMetricSegs(ctx, sourceParams, targetParams, sourceSegmentIDs, newRecords)
	return err
}

// GetSourceSegments fetches the source segments for rollup
func (s *RollupStrategy) GetSourceSegments(ctx context.Context, db accumulation.Store, notification *messages.MetricSegmentNotificationMessage) ([]lrdb.MetricSeg, error) {
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
			ll.Warn("Segment not found for rollup",
				slog.Int64("segmentID", notification.SegmentID))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch segment: %w", err)
	}

	// Check if segment is already rolled up (optional - could add a rolled_up flag)
	// For now, we'll process all segments

	return []lrdb.MetricSeg{segment}, nil
}

// GetOutputTopics returns the output Kafka topics for rolled-up segments
func (s *RollupStrategy) GetOutputTopics(key accumulation.AccumulationKey) []string {
	// Rollup produces output to metric-segments topic for the target frequency
	return []string{"metric-segments"}
}

// GetConsumerGroup returns the consumer group name
func (s *RollupStrategy) GetConsumerGroup() string {
	return "rollup-metrics"
}

// GetSourceTopic returns the source Kafka topic
func (s *RollupStrategy) GetSourceTopic() string {
	return "metric-segments"
}

// GetMaxAccumulationTime returns the max accumulation time
func (s *RollupStrategy) GetMaxAccumulationTime() string {
	return s.Config.MaxAccumulationTime
}

// GetTargetFileSizeBytes returns the target file size in bytes
func (s *RollupStrategy) GetTargetFileSizeBytes() int64 {
	return s.Config.TargetFileSizeBytes
}
