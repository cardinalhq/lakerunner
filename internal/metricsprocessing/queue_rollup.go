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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// queueMetricRollup sends rollup work notification to Kafka for a specific segment
func queueMetricRollup(ctx context.Context, kafkaProducer fly.Producer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, segmentID int64, recordCount int64, fileSize int64, segmentStartTime time.Time, topicRegistry *config.TopicRegistry) error {
	ll := logctx.FromContext(ctx)

	// Check if this frequency needs rollup and get target frequency
	targetFrequencyMs, exists := config.GetTargetRollupFrequency(frequencyMs)
	if !exists {
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

	// Send to Kafka boxer topic (which will bundle and forward to rollup topic)
	rollupTopic := topicRegistry.GetTopic(config.TopicBoxerMetricsRollup)
	// Use dateint and target frequency for key to group rollup intervals by their target frequency
	if err := kafkaProducer.Send(ctx, rollupTopic, fly.Message{
		Key:   fmt.Appendf(nil, "%s-%d-%d-%d", organizationID.String(), dateint, targetFrequencyMs, instanceNum),
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
