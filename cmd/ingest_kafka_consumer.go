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
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/logsprocessing/logsingestion"
	metricsingestion "github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// PartitionState tracks the offset state for a Kafka partition
type PartitionState struct {
	lastSeenOffset  int64
	lastKnownOffset int64 // From DB
	needsDBCheck    bool
}

// KafkaIngestConsumer handles consuming object storage notifications from Kafka and processing them
type KafkaIngestConsumer struct {
	consumer        *fly.ObjStoreNotificationConsumer
	loop            *IngestLoopContext
	kafkaJournalDB  KafkaJournalDB
	signal          string
	config          *config.Config
	consumerGroup   string
	topic           string
	partitionStates map[int]*PartitionState
	stateMutex      sync.RWMutex
}

// NewKafkaIngestConsumer creates a new Kafka-based ingest consumer
func NewKafkaIngestConsumer(factory *fly.Factory, cfg *config.Config, signal string, groupID string) (*KafkaIngestConsumer, error) {
	if !factory.IsEnabled() {
		return nil, fmt.Errorf("Kafka is not enabled for ingestion")
	}

	// Logger will be retrieved from context using logctx.FromContext()
	// Create a temporary logger for consumer creation
	tempLogger := slog.Default().With("component", "kafka_ingest_consumer", "signal", signal)

	consumer, err := fly.NewObjStoreNotificationConsumer(factory, signal, groupID, tempLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	loop, err := NewIngestLoopContext(context.Background(), signal)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create ingest loop context: %w", err)
	}

	// Determine topic based on signal
	topic := fmt.Sprintf("lakerunner.objstore.ingest.%s", signal)

	return &KafkaIngestConsumer{
		consumer:        consumer,
		loop:            loop,
		kafkaJournalDB:  loop.mdb, // Pass the full mdb as the KafkaJournalDB interface
		signal:          signal,
		config:          cfg,
		consumerGroup:   groupID,
		topic:           topic,
		partitionStates: make(map[int]*PartitionState),
	}, nil
}

// Run starts consuming messages from Kafka and processing them
func (k *KafkaIngestConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting Kafka ingest consumer", slog.String("signal", k.signal))

	// Note: batchSize is not currently used since we're using a handler-based approach
	// that consumes continuously rather than fetching a specific batch size

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use a handler-based approach for consuming messages with Kafka metadata
		err := k.consumer.ConsumeWithMetadata(ctx, func(ctx context.Context, notifications []*messages.ObjStoreNotificationMessage, kafkaMessages []fly.ConsumedMessage) error {
			if len(notifications) == 0 {
				return nil
			}

			// Process each notification individually since they are unrelated
			for i, notif := range notifications {
				if i >= len(kafkaMessages) {
					ll.Error("Mismatch between notifications and Kafka messages")
					return fmt.Errorf("mismatch between notifications and Kafka messages")
				}

				kafkaMsg := kafkaMessages[i]

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
						slog.String("objectID", notif.ObjectID))
					continue
				}

				if err := k.processItem(ctx, notif); err != nil {
					ll.Error("Failed to process item", slog.Any("error", err))
					return err // Return error to prevent commit
				}
			}

			return nil // Success - consumer will commit automatically
		})

		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				// Expected when context is cancelled
				return ctx.Err()
			}
			ll.Error("Failed to consume from Kafka", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// processItem processes a single Kafka notification
func (k *KafkaIngestConsumer) processItem(ctx context.Context, notif *messages.ObjStoreNotificationMessage) error {
	ll := logctx.FromContext(ctx)
	// Convert notification to IngestItem
	item := ingest.IngestItem{
		OrganizationID: notif.OrganizationID,
		InstanceNum:    notif.InstanceNum,
		Bucket:         notif.Bucket,
		ObjectID:       notif.ObjectID,
		Signal:         k.signal,
		FileSize:       notif.FileSize,
		QueuedAt:       notif.QueuedAt,
	}

	// Log lag metrics
	lag := time.Since(item.QueuedAt).Seconds()
	inqueueLag.Record(ctx, lag,
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("signal", item.Signal),
		))

	// Create temporary directory for processing
	tmpdir, err := os.MkdirTemp("", "kafka-ingest-")
	if err != nil {
		return fmt.Errorf("creating tmpdir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to clean up tmpdir", slog.Any("error", err))
		}
	}()

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	itemLogger := ll.With(
		slog.String("organizationID", item.OrganizationID.String()),
		slog.Int("instanceNum", int(item.InstanceNum)),
		slog.String("bucket", item.Bucket),
		slog.String("objectID", item.ObjectID))

	// Create new context with the item-specific logger
	ctxWithItemLogger := logctx.WithLogger(ctx, itemLogger)

	// Get RPF estimate for this specific item
	var rpfEstimate int64
	switch k.signal {
	case "metrics":
		rpfEstimate = k.loop.metricEstimator.Get(item.OrganizationID, item.InstanceNum, 10_000)
	case "logs":
		rpfEstimate = k.loop.logEstimator.Get(item.OrganizationID, item.InstanceNum)
	default:
		rpfEstimate = 40_000
	}

	// Process based on signal type - single item
	var processErr error
	switch k.signal {
	case "metrics":
		processErr = metricsingestion.ProcessBatch(ctxWithItemLogger, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, []ingest.IngestItem{item}, ingestDateint, rpfEstimate, k.loop.exemplarProcessor, k.config.Metrics.Ingestion)
	case "logs":
		processErr = logsingestion.ProcessBatch(ctxWithItemLogger, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, item, ingestDateint, rpfEstimate)
	case "traces":
		processErr = traceIngestBatch(ctxWithItemLogger, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, item, ingestDateint, rpfEstimate, k.loop)
	default:
		processErr = fmt.Errorf("unsupported signal type: %s", k.signal)
	}

	if processErr != nil {
		return fmt.Errorf("failed to process item: %w", processErr)
	}

	// Record successful processing
	ll.Debug("Successfully processed item from Kafka",
		slog.String("objectID", item.ObjectID))

	return nil
}

// Close closes the Kafka consumer and loop context
func (k *KafkaIngestConsumer) Close() error {
	var firstErr error

	if k.consumer != nil {
		if err := k.consumer.Close(); err != nil {
			firstErr = err
		}
	}

	if k.loop != nil {
		if err := k.loop.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// shouldProcessMessage checks if a message should be processed based on offset tracking
func (k *KafkaIngestConsumer) shouldProcessMessage(ctx context.Context, partition int, offset int64) (bool, error) {
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
