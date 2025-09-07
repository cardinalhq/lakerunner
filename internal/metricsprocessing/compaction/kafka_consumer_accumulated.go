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
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// KafkaAccumulatedCompactionConsumer handles consuming and accumulating metric segment notifications from Kafka
type KafkaAccumulatedCompactionConsumer struct {
	consumer            fly.Consumer
	manager             *Manager
	compactionManager   *CompactionManager
	config              *config.Config
	consumerGroup       string
	topic               string
	lastFlushTime       time.Time
	maxAccumulationTime time.Duration
}

// NewKafkaAccumulatedCompactionConsumer creates a new accumulator-based Kafka compaction consumer
func NewKafkaAccumulatedCompactionConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	manager *Manager,
	maxAccumulationTime time.Duration,
) (*KafkaAccumulatedCompactionConsumer, error) {
	consumerGroup := "lakerunner.compact.metrics"
	topic := "lakerunner.segments.metrics.compact"

	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Create compaction manager with centralized temp directory management
	compactionManager, err := NewCompactionManager(maxAccumulationTime, manager.GetDB())
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create compaction manager: %w", err)
	}

	return &KafkaAccumulatedCompactionConsumer{
		consumer:            consumer,
		manager:             manager,
		compactionManager:   compactionManager,
		config:              cfg,
		consumerGroup:       consumerGroup,
		topic:               topic,
		lastFlushTime:       time.Now(),
		maxAccumulationTime: maxAccumulationTime,
	}, nil
}

// Run starts consuming messages from Kafka and processing them in accumulated batches
func (k *KafkaAccumulatedCompactionConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting accumulated Kafka compaction consumer",
		slog.Duration("maxAccumulationTime", k.maxAccumulationTime))

	// Create a ticker for periodic flushing
	flushTicker := time.NewTicker(k.maxAccumulationTime / 2)
	defer flushTicker.Stop()

	// Channel to signal flush requests
	flushChan := make(chan struct{}, 1)

	// Start a goroutine to handle periodic flush checks
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-flushTicker.C:
				if k.compactionManager.ShouldFlush() {
					select {
					case flushChan <- struct{}{}:
					default:
						// Flush already pending
					}
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining work before exiting
			if err := k.flushAccumulated(ctx); err != nil {
				ll.Error("Failed to flush accumulated work on shutdown", slog.Any("error", err))
			}
			return ctx.Err()

		case <-flushChan:
			// Time-based flush
			if err := k.flushAccumulated(ctx); err != nil {
				ll.Error("Failed to flush accumulated compaction work", slog.Any("error", err))
			}

		default:
			// Continue consuming messages
		}

		// Consume messages with timeout to allow periodic flush checks
		consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := k.consumer.Consume(consumeCtx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
			ll := logctx.FromContext(ctx).With(slog.String("batchID", idgen.GenerateShortBase32ID()))
			ctx = logctx.WithLogger(ctx, ll)

			if len(kafkaMessages) == 0 {
				return nil
			}

			ll.Debug("Processing Kafka message batch",
				slog.Int("messageCount", len(kafkaMessages)))

			// Add each message to the accumulator
			for _, kafkaMsg := range kafkaMessages {
				var notification messages.MetricSegmentNotificationMessage
				if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
					ll.Error("Failed to unmarshal metric segment notification",
						slog.Any("error", err),
						slog.Int("partition", kafkaMsg.Partition),
						slog.Int64("offset", kafkaMsg.Offset))
					continue // Skip malformed messages
				}

				// Create Kafka offset update
				kafkaOffset := &lrdb.KafkaOffsetUpdate{
					ConsumerGroup: k.consumerGroup,
					Topic:         k.topic,
					Partition:     int32(kafkaMsg.Partition),
					Offset:        kafkaMsg.Offset,
				}

				// Add to accumulator
				if err := AddCompactionWorkFromKafka(
					ctx,
					&notification,
					k.compactionManager,
					k.manager.GetDB(),
					k.manager.GetStorageProfileProvider(),
					k.manager.GetConfig(),
					kafkaOffset,
				); err != nil {
					ll.Error("Failed to add compaction work to accumulator",
						slog.Any("error", err),
						slog.Int64("segmentID", notification.SegmentID))
					return err // Return error to prevent commit
				}
			}

			// Check if we should flush based on accumulation state
			if k.compactionManager.ShouldFlush() {
				if err := k.flushAccumulated(ctx); err != nil {
					return fmt.Errorf("failed to flush accumulated work: %w", err)
				}
			}

			return nil // Success - messages added to accumulator
		})
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Timeout is expected for periodic flush checks
				continue
			}
			if err == context.Canceled {
				return ctx.Err()
			}
			ll.Error("Failed to consume from Kafka", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// flushAccumulated processes all accumulated compaction work
func (k *KafkaAccumulatedCompactionConsumer) flushAccumulated(ctx context.Context) error {
	ll := logctx.FromContext(ctx)

	if !k.compactionManager.HasWork() {
		ll.Debug("No accumulated work to flush")
		return nil
	}

	ll.Info("Flushing accumulated compaction work")

	err := ProcessAccumulatedCompaction(
		ctx,
		k.compactionManager,
		k.manager.GetDB(),
		k.manager.GetStorageProfileProvider(),
		k.manager.GetCloudManager(),
		k.consumerGroup,
		k.topic,
	)

	if err != nil {
		return fmt.Errorf("failed to process accumulated compaction: %w", err)
	}

	k.lastFlushTime = time.Now()
	return nil
}

// Close closes the Kafka consumer and cleans up resources
func (k *KafkaAccumulatedCompactionConsumer) Close() error {
	// Flush any remaining work
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := k.flushAccumulated(ctx); err != nil {
		logctx.FromContext(ctx).Error("Failed to flush remaining work during shutdown", slog.Any("error", err))
	}

	// Close the compaction manager
	if err := k.compactionManager.Close(); err != nil {
		logctx.FromContext(ctx).Error("Failed to close compaction manager", slog.Any("error", err))
	}

	// Close the Kafka consumer
	if k.consumer != nil {
		return k.consumer.Close()
	}
	return nil
}
