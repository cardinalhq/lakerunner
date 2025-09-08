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
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// RollupWorkQueuer defines the interface for queuing metric rollup work
type RollupWorkQueuer interface {
	// Placeholder for future interface requirements
}

// QueueMetricRollup sends rollup work notification to Kafka for a specific segment
func QueueMetricRollup(ctx context.Context, kafkaProducer fly.Producer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, slotID int32, slotCount int32, segmentID int64, recordCount int64, fileSize int64, segmentStartTime time.Time) error {
	ll := logctx.FromContext(ctx)

	// Map source frequencies to target frequencies
	var targetFrequencyMs int32
	switch frequencyMs {
	case 10_000:
		targetFrequencyMs = 60_000 // 10s -> 1m
	case 60_000:
		targetFrequencyMs = 300_000 // 1m -> 5m
	case 300_000:
		targetFrequencyMs = 1_200_000 // 5m -> 20m
	case 1_200_000:
		targetFrequencyMs = 3_600_000 // 20m -> 1h
	default:
		// Not a rollup source frequency
		return nil
	}

	// Create rollup notification message
	notification := messages.MetricRollupMessage{
		Version:           1,
		OrganizationID:    organizationID,
		DateInt:           dateint,
		SourceFrequencyMs: frequencyMs,
		TargetFrequencyMs: targetFrequencyMs,
		SegmentID:         segmentID,
		InstanceNum:       instanceNum,
		SlotID:            slotID,
		SlotCount:         slotCount,
		Records:           recordCount,
		FileSize:          fileSize,
		SegmentStartTime:  segmentStartTime,
		QueuedAt:          time.Now(),
	}

	// Marshal the message
	msgBytes, err := notification.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal rollup notification: %w", err)
	}

	// Send to Kafka rollup topic
	rollupTopic := "lakerunner.segments.metrics.rollup"
	// Use dateint and frequency for key instead of segment ID to group rollup intervals
	if err := kafkaProducer.Send(ctx, rollupTopic, fly.Message{
		Key:   fmt.Appendf(nil, "%s-%d-%d-%d-%d", organizationID.String(), dateint, frequencyMs, instanceNum, slotID),
		Value: msgBytes,
	}); err != nil {
		return fmt.Errorf("failed to send rollup notification to Kafka: %w", err)
	}

	ll.Debug("Sent rollup notification to Kafka",
		slog.String("organizationID", organizationID.String()),
		slog.Int("dateint", int(dateint)),
		slog.Int("frequencyMs", int(frequencyMs)),
		slog.Int64("segmentID", segmentID))

	return nil
}
