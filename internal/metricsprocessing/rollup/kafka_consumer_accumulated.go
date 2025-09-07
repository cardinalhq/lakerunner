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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// PartitionState tracks the offset state for a Kafka partition
type PartitionState struct {
	lastSeenOffset  int64
	lastKnownOffset int64 // From DB
	needsDBCheck    bool
}

// KafkaAccumulatedRollupConsumer handles consuming and accumulating metric rollup notifications from Kafka
type KafkaAccumulatedRollupConsumer struct {
	consumer        fly.Consumer
	rollupManager   *RollupManager
	db              rollupStore
	sp              storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	config          *config.Config
	consumerGroup   string
	topic           string
	lastFlushTime   time.Time
	partitionStates map[int]*PartitionState
	stateMutex      sync.RWMutex
}

// NewKafkaAccumulatedRollupConsumer creates a new accumulator-based Kafka rollup consumer
func NewKafkaAccumulatedRollupConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	db rollupStore,
	sp storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*KafkaAccumulatedRollupConsumer, error) {
	consumerGroup := "lakerunner.rollup.metrics"
	topic := "lakerunner.segments.metrics.rollup"

	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Create rollup manager with centralized temp directory management
	rollupManager, err := NewRollupManager(db)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create rollup manager: %w", err)
	}

	return &KafkaAccumulatedRollupConsumer{
		consumer:        consumer,
		rollupManager:   rollupManager,
		db:              db,
		sp:              sp,
		cmgr:            cmgr,
		config:          cfg,
		consumerGroup:   consumerGroup,
		topic:           topic,
		lastFlushTime:   time.Now(),
		partitionStates: make(map[int]*PartitionState),
	}, nil
}

// Run starts consuming messages from Kafka and processing them in accumulated batches
func (k *KafkaAccumulatedRollupConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting accumulated Kafka rollup consumer")

	// Create tickers for periodic flushing based on different frequencies
	// We'll check every 30 seconds for any accumulators that need flushing
	flushTicker := time.NewTicker(30 * time.Second)
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
				if k.rollupManager.ShouldFlush() {
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
				ll.Error("Failed to flush accumulated rollup work", slog.Any("error", err))
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
					ll.Error("Failed to unmarshal metric rollup notification",
						slog.Any("error", err),
						slog.Int("partition", kafkaMsg.Partition),
						slog.Int64("offset", kafkaMsg.Offset))
					continue // Skip malformed messages
				}

				// Check if we should process this message based on offset tracking
				shouldProcess, err := k.shouldProcessMessage(ctx, kafkaMsg.Partition, kafkaMsg.Offset)
				if err != nil {
					ll.Error("Failed to check if message should be processed", slog.Any("error", err))
					return err
				}

				if !shouldProcess {
					ll.Debug("Skipping already processed message",
						slog.Int("partition", kafkaMsg.Partition),
						slog.Int64("offset", kafkaMsg.Offset),
						slog.Int64("segmentID", notification.SegmentID))
					continue
				}

				// Create Kafka offset update
				kafkaOffset := &lrdb.KafkaOffsetUpdate{
					ConsumerGroup: k.consumerGroup,
					Topic:         k.topic,
					Partition:     int32(kafkaMsg.Partition),
					Offset:        kafkaMsg.Offset,
				}

				// Add to accumulator
				if err := AddRollupWorkFromKafka(
					ctx,
					&notification,
					k.rollupManager,
					k.db,
					k.sp,
					kafkaOffset,
				); err != nil {
					ll.Error("Failed to add rollup work to accumulator",
						slog.Any("error", err),
						slog.Int64("segmentID", notification.SegmentID))
					return err // Return error to prevent commit
				}
			}

			// Check if we should flush based on accumulation state
			if k.rollupManager.ShouldFlush() {
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

// flushAccumulated processes all accumulated rollup work
func (k *KafkaAccumulatedRollupConsumer) flushAccumulated(ctx context.Context) error {
	ll := logctx.FromContext(ctx)

	if !k.rollupManager.HasWork() {
		ll.Debug("No accumulated work to flush")
		return nil
	}

	ll.Info("Flushing accumulated rollup work")

	err := ProcessAccumulatedRollup(
		ctx,
		k.rollupManager,
		k.db,
		k.sp,
		k.cmgr,
		k.consumerGroup,
		k.topic,
	)

	if err != nil {
		return fmt.Errorf("failed to process accumulated rollup: %w", err)
	}

	k.lastFlushTime = time.Now()
	return nil
}

// shouldProcessMessage checks if a message should be processed based on offset tracking
func (k *KafkaAccumulatedRollupConsumer) shouldProcessMessage(ctx context.Context, partition int, offset int64) (bool, error) {
	ll := logctx.FromContext(ctx)
	k.stateMutex.Lock()
	defer k.stateMutex.Unlock()

	state := k.partitionStates[partition]
	if state == nil {
		// First time seeing this partition - check DB
		state = &PartitionState{needsDBCheck: true}
		k.partitionStates[partition] = state
	}

	if state.needsDBCheck || offset != state.lastSeenOffset+1 {
		// Either first time or gap detected - check DB
		dbOffset, err := k.db.KafkaJournalGetLastProcessed(ctx, lrdb.KafkaJournalGetLastProcessedParams{
			ConsumerGroup: k.consumerGroup,
			Topic:         k.topic,
			Partition:     int32(partition),
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// No previous offset recorded - this is the first message
				state.lastKnownOffset = -1
			} else {
				return false, fmt.Errorf("failed to get last processed offset: %w", err)
			}
		} else {
			state.lastKnownOffset = dbOffset
		}

		state.needsDBCheck = false
		ll.Debug("Checked DB for partition offset",
			slog.Int("partition", partition),
			slog.Int64("db_offset", state.lastKnownOffset),
			slog.Int64("message_offset", offset))

		if offset <= state.lastKnownOffset {
			state.lastSeenOffset = offset
			return false, nil // Already processed
		}
	}

	state.lastSeenOffset = offset
	return true, nil
}

// Close closes the Kafka consumer and cleans up resources
func (k *KafkaAccumulatedRollupConsumer) Close() error {
	// Flush any remaining work
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := k.flushAccumulated(ctx); err != nil {
		logctx.FromContext(ctx).Error("Failed to flush remaining work during shutdown", slog.Any("error", err))
	}

	// Close the rollup manager
	if err := k.rollupManager.Close(); err != nil {
		logctx.FromContext(ctx).Error("Failed to close rollup manager", slog.Any("error", err))
	}

	// Close the Kafka consumer
	if k.consumer != nil {
		return k.consumer.Close()
	}
	return nil
}
