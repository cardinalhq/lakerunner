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

package fly

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// OffsetSkipChecker periodically checks for skip offsets and applies them.
// This allows consumers to skip forward without shutdown.
type OffsetSkipChecker struct {
	store         *lrdb.Store
	adminClient   *AdminClient
	consumerGroup string
	topic         string
	checkInterval time.Duration

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

// NewOffsetSkipChecker creates a new offset skip checker.
func NewOffsetSkipChecker(store *lrdb.Store, adminClient *AdminClient, consumerGroup, topic string, checkInterval time.Duration) *OffsetSkipChecker {
	if checkInterval == 0 {
		checkInterval = 30 * time.Second
	}
	return &OffsetSkipChecker{
		store:         store,
		adminClient:   adminClient,
		consumerGroup: consumerGroup,
		topic:         topic,
		checkInterval: checkInterval,
		stopCh:        make(chan struct{}),
	}
}

// Start begins periodic checking for skip offsets.
func (c *OffsetSkipChecker) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.mu.Unlock()

	// Do an initial check
	c.checkAndApplySkips(ctx)

	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkAndApplySkips(ctx)
		}
	}
}

// Stop stops the periodic checking.
func (c *OffsetSkipChecker) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		close(c.stopCh)
		c.running = false
	}
}

// CheckOnce performs a single check for skip offsets and applies them.
// Returns true if any skips were applied.
func (c *OffsetSkipChecker) CheckOnce(ctx context.Context) (bool, error) {
	return c.checkAndApplySkips(ctx), nil
}

func (c *OffsetSkipChecker) checkAndApplySkips(ctx context.Context) bool {
	skips, err := c.store.GetKafkaOffsetSkips(ctx, lrdb.GetKafkaOffsetSkipsParams{
		ConsumerGroup: c.consumerGroup,
		Topic:         c.topic,
	})
	if err != nil {
		slog.Warn("Failed to check for offset skips",
			slog.String("consumerGroup", c.consumerGroup),
			slog.String("topic", c.topic),
			slog.Any("error", err))
		return false
	}

	if len(skips) == 0 {
		return false
	}

	slog.Info("Found offset skip entries",
		slog.String("consumerGroup", c.consumerGroup),
		slog.String("topic", c.topic),
		slog.Int("count", len(skips)))

	// Build offset list for committing
	offsets := make([]PartitionOffset, 0, len(skips))
	for _, skip := range skips {
		offsets = append(offsets, PartitionOffset{
			Partition: int(skip.PartitionID),
			Offset:    skip.SkipToOffset,
		})
		slog.Info("Applying offset skip",
			slog.String("consumerGroup", c.consumerGroup),
			slog.String("topic", c.topic),
			slog.Int("partition", int(skip.PartitionID)),
			slog.Int64("skipToOffset", skip.SkipToOffset))
	}

	// Commit the offsets to Kafka
	if err := c.adminClient.CommitConsumerGroupOffsets(ctx, c.consumerGroup, c.topic, offsets); err != nil {
		slog.Error("Failed to commit skip offsets",
			slog.String("consumerGroup", c.consumerGroup),
			slog.String("topic", c.topic),
			slog.Any("error", err))
		return false
	}

	// Delete the skip entries after successful commit
	// Include skip_to_offset in the delete to avoid race with concurrent flush requests
	for _, skip := range skips {
		if err := c.store.DeleteKafkaOffsetSkip(ctx, lrdb.DeleteKafkaOffsetSkipParams{
			ConsumerGroup: c.consumerGroup,
			Topic:         c.topic,
			PartitionID:   skip.PartitionID,
			SkipToOffset:  skip.SkipToOffset,
		}); err != nil {
			slog.Warn("Failed to delete skip entry after applying",
				slog.String("consumerGroup", c.consumerGroup),
				slog.String("topic", c.topic),
				slog.Int("partition", int(skip.PartitionID)),
				slog.Any("error", err))
		}
	}

	slog.Info("Successfully applied offset skips",
		slog.String("consumerGroup", c.consumerGroup),
		slog.String("topic", c.topic),
		slog.Int("count", len(skips)))

	return true
}
