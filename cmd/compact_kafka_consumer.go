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

package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/compaction"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// KafkaCompactionConsumer handles consuming metric segment notifications from Kafka for compaction
type KafkaCompactionConsumer struct {
	consumer        fly.Consumer
	manager         *compaction.Manager
	kafkaJournalDB  KafkaJournalDB
	config          *config.Config
	consumerGroup   string
	topic           string
	partitionStates map[int]*PartitionState
	stateMutex      sync.RWMutex
}

// NewKafkaCompactionConsumer creates a new Kafka-based compaction consumer
func NewKafkaCompactionConsumer(ctx context.Context, factory *fly.Factory, cfg *config.Config, manager *compaction.Manager, kafkaJournalDB KafkaJournalDB) (*KafkaCompactionConsumer, error) {
	if !factory.IsEnabled() {
		return nil, fmt.Errorf("Kafka is not enabled for compaction")
	}

	// Create consumer for metric compaction topic
	consumerGroup := "lakerunner.compact.metrics"
	topic := "lakerunner.segments.metrics.compact"

	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &KafkaCompactionConsumer{
		consumer:        consumer,
		manager:         manager,
		kafkaJournalDB:  kafkaJournalDB,
		config:          cfg,
		consumerGroup:   consumerGroup,
		topic:           topic,
		partitionStates: make(map[int]*PartitionState),
	}, nil
}

// Run starts consuming messages from Kafka and processing them for compaction
func (k *KafkaCompactionConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting Kafka compaction consumer")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Consume messages with metadata for proper offset tracking
		err := k.consumer.Consume(ctx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
			if len(kafkaMessages) == 0 {
				return nil
			}

			// Process each message individually for now
			// TODO: Later we can batch multiple messages for efficiency
			for _, kafkaMsg := range kafkaMessages {
				// Parse the message
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

				// Process the compaction work
				if err := k.processCompactionWork(ctx, &notification, kafkaMsg); err != nil {
					ll.Error("Failed to process compaction work",
						slog.Any("error", err),
						slog.Int64("segmentID", notification.SegmentID))
					return err // Return error to prevent commit
				}

				// Log lag metrics
				lag := time.Since(notification.QueuedAt).Seconds()
				if compactionLag != nil {
					compactionLag.Record(ctx, lag,
						metric.WithAttributeSet(commonAttributes),
						metric.WithAttributes(
							attribute.String("signal", "metrics"),
						))
				}
			}

			return nil // Success - consumer will commit offsets
		})

		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return ctx.Err()
			}
			ll.Error("Failed to consume from Kafka", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// processCompactionWork processes a single compaction work item from Kafka
func (k *KafkaCompactionConsumer) processCompactionWork(ctx context.Context, notification *messages.MetricSegmentNotificationMessage, kafkaMsg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	ll.Info("Processing compaction work from Kafka",
		slog.String("organizationID", notification.OrganizationID.String()),
		slog.Int("dateint", int(notification.DateInt)),
		slog.Int("frequencyMs", int(notification.FrequencyMs)),
		slog.Int64("segmentID", notification.SegmentID),
		slog.Int("slotID", int(notification.SlotID)),
		slog.Int("slotCount", int(notification.SlotCount)))

	// Prepare Kafka offset for the compaction processing
	kafkaOffset := &lrdb.KafkaOffsetUpdate{
		ConsumerGroup: k.consumerGroup,
		Topic:         k.topic,
		Partition:     int32(kafkaMsg.Partition),
		Offset:        kafkaMsg.Offset,
	}

	// Process the Kafka message using the new Kafka-specific processing function
	err := compaction.ProcessKafkaMessage(ctx, notification, k.manager.GetDB(), k.manager.GetStorageProfileProvider(), k.manager.GetCloudManager(), kafkaOffset)
	if err != nil {
		ll.Error("Failed to process compaction from Kafka",
			slog.Any("error", err),
			slog.Int64("segmentID", notification.SegmentID))
		return fmt.Errorf("failed to process compaction: %w", err)
	}

	ll.Debug("Successfully processed compaction work from Kafka",
		slog.Int64("segmentID", notification.SegmentID))

	return nil
}

// shouldProcessMessage checks if a message should be processed based on offset tracking
func (k *KafkaCompactionConsumer) shouldProcessMessage(ctx context.Context, partition int, offset int64) (bool, error) {
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
		dbOffset, err := k.kafkaJournalDB.KafkaJournalGetLastProcessed(ctx, lrdb.KafkaJournalGetLastProcessedParams{
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

// Close closes the Kafka consumer
func (k *KafkaCompactionConsumer) Close() error {
	if k.consumer != nil {
		return k.consumer.Close()
	}
	return nil
}

// Define compactionLag metric if not already defined
var compactionLag metric.Float64Histogram

func init() {
	// Initialize compaction lag metric
	if meter != nil {
		var err error
		compactionLag, err = meter.Float64Histogram(
			"lakerunner.compaction.lag",
			metric.WithDescription("Time between message queuing and processing for compaction"),
			metric.WithUnit("s"),
		)
		if err != nil {
			slog.Error("Failed to create compaction lag metric", slog.Any("error", err))
		}
	}
}
