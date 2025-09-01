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

package metricsprocessing

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// RollupWorkQueuer defines the interface for queuing metric rollup work
type RollupWorkQueuer interface {
	PutMetricRollupWork(ctx context.Context, arg lrdb.PutMetricRollupWorkParams) error
}

// calculateWindowCloseTime calculates when a rollup window can safely be processed
func calculateWindowCloseTime(startTs int64, targetFrequency int32) time.Time {
	// Calculate which rollup window this segment belongs to
	windowStartMs := (startTs / int64(targetFrequency)) * int64(targetFrequency)
	windowEndMs := windowStartMs + int64(targetFrequency)
	windowEnd := time.UnixMilli(windowEndMs)

	// Add grace period based on target frequency
	var gracePeriod time.Duration
	switch targetFrequency {
	case 60_000: // 1min rollups
		gracePeriod = 2 * time.Minute
	case 300_000: // 5min rollups
		gracePeriod = 3 * time.Minute
	case 1_200_000: // 20min rollups
		gracePeriod = 5 * time.Minute
	case 3_600_000: // 1hr rollups
		gracePeriod = 10 * time.Minute
	default:
		gracePeriod = 2 * time.Minute // Default fallback
	}

	return windowEnd.Add(gracePeriod)
}

// QueueMetricRollup queues rollup work for a specific segment at the next frequency level
func QueueMetricRollup(ctx context.Context, mdb RollupWorkQueuer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, slotID int32, slotCount int32, segmentID int64, recordCount int64, startTs int64) error {
	nextFrequency, exists := RollupTo[frequencyMs]
	if !exists {
		return nil
	}

	priority := GetFrequencyPriority(nextFrequency)

	// Calculate rollup group: segment start time divided by target rollup frequency
	rollupGroup := startTs / int64(nextFrequency)

	// Calculate when this rollup window can safely be processed
	windowCloseTs := calculateWindowCloseTime(startTs, nextFrequency)

	err := mdb.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: organizationID,
		Dateint:        dateint,
		FrequencyMs:    int64(frequencyMs),
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		SegmentID:      segmentID,
		RecordCount:    recordCount,
		RollupGroup:    rollupGroup,
		Priority:       priority,
		WindowCloseTs:  windowCloseTs,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric rollup work: %w", err)
	}

	return nil
}
