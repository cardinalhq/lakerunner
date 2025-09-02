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
	MrqQueueWork(ctx context.Context, arg lrdb.MrqQueueWorkParams) error
}

// QueueMetricRollup queues rollup work for a specific segment at the next frequency level
func QueueMetricRollup(ctx context.Context, mdb RollupWorkQueuer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, slotID int32, slotCount int32, segmentID int64, recordCount int64, startTs int64) error {
	nextFrequency, exists := RollupTo[frequencyMs]
	if !exists {
		return nil
	}

	priority := int32(0)

	// Calculate rollup group: segment start time divided by target rollup frequency
	rollupGroup := startTs / int64(nextFrequency)

	// Calculate when the rollup work should become eligible
	eligibleAt := calculateRollupEligibleTime(frequencyMs, startTs, nextFrequency)

	err := mdb.MrqQueueWork(ctx, lrdb.MrqQueueWorkParams{
		OrganizationID: organizationID,
		Dateint:        dateint,
		FrequencyMs:    frequencyMs,
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		SegmentID:      segmentID,
		RecordCount:    recordCount,
		RollupGroup:    rollupGroup,
		Priority:       priority,
		EligibleAt:     eligibleAt,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric rollup work: %w", err)
	}

	return nil
}

// calculateRollupEligibleTime determines when rollup work should become eligible based on frequency
func calculateRollupEligibleTime(currentFrequencyMs int32, startTs int64, nextFrequencyMs int32) time.Time {
	now := time.Now()

	switch currentFrequencyMs {
	case 10_000: // 10s rollups
		// Schedule 120s past the end of the current 60s interval
		currentMinute := now.Truncate(time.Minute)
		nextMinute := currentMinute.Add(time.Minute)
		return nextMinute.Add(2 * time.Minute) // 120s delay

	case 60_000: // 60s rollups (1m)
		// Schedule 2m past the end of the next 5m interval
		current5Min := now.Truncate(5 * time.Minute)
		next5Min := current5Min.Add(5 * time.Minute)
		return next5Min.Add(2 * time.Minute) // 2m delay

	default: // 5m+ rollups
		// Schedule 10m past the end of the respective next-level window
		windowDuration := time.Duration(nextFrequencyMs) * time.Millisecond
		currentWindow := now.Truncate(windowDuration)
		nextWindow := currentWindow.Add(windowDuration)
		return nextWindow.Add(10 * time.Minute) // 10m delay
	}
}
