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
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/accumulation"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// PartitionState tracks the offset state for a Kafka partition
type PartitionState struct {
	lastSeenOffset  int64
	lastKnownOffset int64 // From DB
	needsDBCheck    bool
}

// KafkaAccumulatedCompactionConsumer handles consuming and accumulating metric segment notifications from Kafka
type KafkaAccumulatedCompactionConsumer struct {
	consumer            fly.Consumer
	accManager          *accumulation.Manager
	strategy            *CompactionStrategy
	store               CompactionStore
	sp                  storageprofile.StorageProfileProvider
	cmgr                cloudstorage.ClientProvider
	config              *config.Config
	consumerGroup       string
	topic               string
	lastFlushTime       time.Time
	maxAccumulationTime time.Duration
	partitionStates     map[int]*PartitionState
	stateMutex          sync.RWMutex
}

// NewKafkaAccumulatedCompactionConsumer creates a new accumulator-based Kafka compaction consumer
func NewKafkaAccumulatedCompactionConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	manager *Manager,
	maxAccumulationTime time.Duration,
) (*KafkaAccumulatedCompactionConsumer, error) {
	// Create strategy
	compactionConfig := manager.GetConfig()
	compactionConfig.MaxAccumulationTime = maxAccumulationTime.String()
	strategy := NewCompactionStrategy(compactionConfig)

	// Create shared accumulation manager
	accManager, err := accumulation.NewManager(
		strategy,
		manager.GetDB(),
		maxAccumulationTime,
		compactionConfig.TargetFileSizeBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulation manager: %w", err)
	}

	consumerGroup := strategy.GetConsumerGroup()
	topic := strategy.GetSourceTopic()

	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		accManager.Close()
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &KafkaAccumulatedCompactionConsumer{
		consumer:            consumer,
		accManager:          accManager,
		strategy:            strategy,
		store:               manager.GetDB(),
		sp:                  manager.GetStorageProfileProvider(),
		cmgr:                manager.GetCloudManager(),
		config:              cfg,
		consumerGroup:       consumerGroup,
		topic:               topic,
		lastFlushTime:       time.Now(),
		maxAccumulationTime: maxAccumulationTime,
		partitionStates:     make(map[int]*PartitionState),
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
				if k.accManager.ShouldFlush() {
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

		// Consume messages - Kafka has built-in polling timeout (~500ms)
		err := k.consumer.Consume(ctx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
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

				// Add to accumulator using the shared framework
				if err := k.accManager.AddWorkFromKafka(ctx, &notification, k.sp, kafkaOffset); err != nil {
					// Log warning and skip this message - don't fail the whole batch
					ll.Warn("Skipping segment due to compaction work error",
						slog.Any("error", err),
						slog.Int64("segmentID", notification.SegmentID),
						slog.String("organizationID", notification.OrganizationID.String()))
					continue // Skip this message but continue processing others
				}
			}

			// Check if we should flush based on accumulation state
			if k.accManager.ShouldFlush() {
				if err := k.flushAccumulated(ctx); err != nil {
					// Log error but don't fail - we'll retry the flush next time
					ll.Error("Failed to flush accumulated work, will retry on next batch",
						slog.Any("error", err))
					// Don't return error - we still want to commit the Kafka offsets
					// The data remains in the accumulator and will be retried on the next flush
				}
			}

			return nil // Success - messages added to accumulator
		})

		if err != nil {
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

	// Always reset at the end, regardless of success or failure
	defer func() {
		if err := k.accManager.Reset(); err != nil {
			ll.Error("Failed to reset accumulation manager", slog.Any("error", err))
		}
	}()

	if !k.accManager.HasWork() {
		ll.Debug("No accumulated work to flush")
		// Still need to update Kafka offsets if any
		return k.updateKafkaOffsets(ctx)
	}

	startTime := time.Now()
	ll.Info("Starting accumulated compaction processing")

	// Process each accumulator
	for key, accumulator := range k.accManager.GetAccumulators() {
		work := accumulator.GetWork()
		if len(work) == 0 {
			continue
		}

		ll.Debug("Processing compaction accumulator",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)),
			slog.Int("frequencyMs", int(key.FrequencyMs)),
			slog.Int("workCount", len(work)))

		// Get storage profile from first work item
		profile := work[0].Profile

		// Create blob client for this org/instance
		blobclient, err := cloudstorage.NewClient(ctx, k.cmgr, profile)
		if err != nil {
			ll.Error("Failed to create storage client",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to create storage client: %w", err)
		}

		// Process the accumulator using shared framework
		if err := accumulation.FlushAccumulator(ctx, accumulator, k.store, blobclient, k.accManager.GetTmpDir(), k.strategy, nil); err != nil {
			ll.Error("Failed to flush accumulator",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Any("error", err))
			return fmt.Errorf("failed to flush accumulator: %w", err)
		}
	}

	// Update Kafka offsets after successful processing
	if err := k.updateKafkaOffsets(ctx); err != nil {
		return fmt.Errorf("failed to update Kafka offsets: %w", err)
	}

	ll.Info("Completed accumulated compaction processing",
		slog.Duration("duration", time.Since(startTime)))

	k.lastFlushTime = time.Now()
	return nil
}

// updateKafkaOffsets updates Kafka offsets in the database
func (k *KafkaAccumulatedCompactionConsumer) updateKafkaOffsets(ctx context.Context) error {
	offsetUpdates := k.accManager.GetKafkaOffsetUpdates(k.consumerGroup, k.topic)
	for _, update := range offsetUpdates {
		if err := k.store.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       update.ConsumerGroup,
			Topic:               update.Topic,
			Partition:           update.Partition,
			LastProcessedOffset: update.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset for partition %d: %w", update.Partition, err)
		}
	}
	return nil
}

// shouldProcessMessage checks if a message should be processed based on offset tracking
func (k *KafkaAccumulatedCompactionConsumer) shouldProcessMessage(ctx context.Context, partition int, offset int64) (bool, error) {
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
		dbOffset, err := k.store.KafkaJournalGetLastProcessed(ctx, lrdb.KafkaJournalGetLastProcessedParams{
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
func (k *KafkaAccumulatedCompactionConsumer) Close() error {
	// Flush any remaining work
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := k.flushAccumulated(ctx); err != nil {
		logctx.FromContext(ctx).Error("Failed to flush remaining work during shutdown", slog.Any("error", err))
	}

	// Close the accumulation manager
	if err := k.accManager.Close(); err != nil {
		logctx.FromContext(ctx).Error("Failed to close accumulation manager", slog.Any("error", err))
	}

	// Close the Kafka consumer
	if k.consumer != nil {
		return k.consumer.Close()
	}
	return nil
}
