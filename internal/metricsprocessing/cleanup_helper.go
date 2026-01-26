// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CleanupStore defines the interface for stores that support offset cleanup
type CleanupStore interface {
	CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error)
}

// offsetCleanupBuffer is the number of offsets to keep behind the committed offset
// to handle potential replays during partition rebalancing
const offsetCleanupBuffer = 10000

// CleanupCommittedOffsets removes old offset tracking records after successful Kafka commit.
// This is a helper function for consumers that don't use CommonConsumer.
func CleanupCommittedOffsets(ctx context.Context, store CleanupStore, topic, consumerGroup string, messages []fly.ConsumedMessage) {
	ll := logctx.FromContext(ctx)

	// Group messages by partition to find max offset per partition
	maxOffsetPerPartition := make(map[int32]int64)
	for _, msg := range messages {
		partition := int32(msg.Partition)
		if currentMax, exists := maxOffsetPerPartition[partition]; !exists || msg.Offset > currentMax {
			maxOffsetPerPartition[partition] = msg.Offset
		}
	}

	// Cleanup old offset tracking records for each partition
	for partition, maxOffset := range maxOffsetPerPartition {
		// Apply safety buffer to handle potential replays during rebalancing
		// We keep offsetCleanupBuffer offsets behind to ensure deduplication works
		// even if messages are replayed after a partition moves to another consumer
		safeCleanupOffset := maxOffset - offsetCleanupBuffer
		if safeCleanupOffset < 0 {
			// Don't cleanup anything if we haven't processed enough messages yet
			continue
		}

		params := lrdb.CleanupKafkaOffsetsParams{
			ConsumerGroup: consumerGroup,
			Topic:         topic,
			PartitionID:   partition,
			MaxOffset:     safeCleanupOffset,
		}

		if rowsDeleted, err := store.CleanupKafkaOffsets(ctx, params); err != nil {
			ll.Error("Failed to cleanup old Kafka offset tracking records",
				slog.Any("error", err),
				slog.String("topic", topic),
				slog.String("consumerGroup", consumerGroup),
				slog.Int("partition", int(partition)),
				slog.Int64("maxOffset", maxOffset))
		} else if rowsDeleted > 0 {
			ll.Debug("Cleaned up old Kafka offset tracking records",
				slog.String("topic", topic),
				slog.String("consumerGroup", consumerGroup),
				slog.Int("partition", int(partition)),
				slog.Int64("committedOffset", maxOffset),
				slog.Int64("cleanupOffset", safeCleanupOffset),
				slog.Int64("rowsDeleted", rowsDeleted))
		}
	}
}
