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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// KafkaIngestConsumer handles consuming object storage notifications from Kafka and processing them
type KafkaIngestConsumer struct {
	consumer *fly.ObjStoreNotificationConsumer
	loop     *IngestLoopContext
	signal   string
	logger   *slog.Logger
}

// NewKafkaIngestConsumer creates a new Kafka-based ingest consumer
func NewKafkaIngestConsumer(signal string, groupID string) (*KafkaIngestConsumer, error) {
	factory := fly.NewFactoryFromEnv()
	if !factory.IsEnabledForIngestion() {
		return nil, fmt.Errorf("Kafka is not enabled for ingestion")
	}

	logger := slog.Default().With("component", "kafka_ingest_consumer", "signal", signal)

	consumer, err := fly.NewObjStoreNotificationConsumer(factory, signal, groupID, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	loop, err := NewIngestLoopContext(context.Background(), signal)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create ingest loop context: %w", err)
	}

	return &KafkaIngestConsumer{
		consumer: consumer,
		loop:     loop,
		signal:   signal,
		logger:   logger,
	}, nil
}

// Run starts consuming messages from Kafka and processing them
func (k *KafkaIngestConsumer) Run(ctx context.Context) error {
	k.logger.Info("Starting Kafka ingest consumer", slog.String("signal", k.signal))

	// Note: batchSize is not currently used since we're using a handler-based approach
	// that consumes continuously rather than fetching a specific batch size
	_ = helpers.GetBatchSizeForSignal(k.signal)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use a handler-based approach for consuming messages
		err := k.consumer.Consume(ctx, func(ctx context.Context, notifications []*messages.ObjStoreNotificationMessage) error {
			if len(notifications) == 0 {
				return nil
			}

			// Process the notifications
			if err := k.processBatch(ctx, notifications); err != nil {
				k.logger.Error("Failed to process batch", slog.Any("error", err))
				return err // Return error to prevent commit
			}

			return nil // Success - consumer will commit automatically
		})

		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				// Expected when context is cancelled
				return ctx.Err()
			}
			k.logger.Error("Failed to consume from Kafka", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// processBatch converts Kafka notifications to inqueue items and processes them
func (k *KafkaIngestConsumer) processBatch(ctx context.Context, notifications []*messages.ObjStoreNotificationMessage) error {
	if len(notifications) == 0 {
		return nil
	}

	// Group notifications by organization and instance for efficient batching
	grouped := make(map[string][]*messages.ObjStoreNotificationMessage)
	for _, notif := range notifications {
		key := fmt.Sprintf("%s-%d", notif.OrganizationID.String(), notif.InstanceNum)
		grouped[key] = append(grouped[key], notif)
	}

	// Process each group separately
	for groupKey, groupNotifications := range grouped {
		// Convert notifications to Inqueue items
		items := make([]lrdb.Inqueue, len(groupNotifications))
		now := time.Now()
		for i, notif := range groupNotifications {
			items[i] = lrdb.Inqueue{
				ID:             uuid.New(), // Generate ID for database
				QueueTs:        notif.QueuedAt,
				Priority:       10, // Default priority
				OrganizationID: notif.OrganizationID,
				CollectorName:  "", // Will be looked up based on bucket/instance/org
				InstanceNum:    notif.InstanceNum,
				Bucket:         notif.Bucket,
				ObjectID:       notif.ObjectID,
				Signal:         k.signal, // Use the consumer's signal
				FileSize:       notif.FileSize,
				Tries:          0,
				ClaimedBy:      myInstanceID,
				ClaimedAt:      &now,
			}
		}

		// Log lag metrics
		for _, item := range items {
			lag := time.Since(item.QueueTs).Seconds()
			inqueueLag.Record(ctx, lag,
				metric.WithAttributeSet(commonAttributes),
				metric.WithAttributes(
					attribute.String("signal", item.Signal),
				))
		}

		// Get RPF estimate for this group
		var rpfEstimate int64
		switch k.signal {
		case "metrics":
			rpfEstimate = k.loop.metricEstimator.Get(items[0].OrganizationID, items[0].InstanceNum, 10_000)
		case "logs":
			rpfEstimate = k.loop.logEstimator.Get(items[0].OrganizationID, items[0].InstanceNum)
		default:
			rpfEstimate = 40_000
		}

		ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

		// Create temporary directory for processing
		tmpdir, err := os.MkdirTemp("", "kafka-ingest-")
		if err != nil {
			return fmt.Errorf("creating tmpdir: %w", err)
		}
		defer func() {
			if err := os.RemoveAll(tmpdir); err != nil {
				k.logger.Error("Failed to clean up tmpdir", slog.Any("error", err))
			}
		}()

		ll := k.logger.With(
			slog.String("group", groupKey),
			slog.Int("batchSize", len(items)),
			slog.String("organizationID", items[0].OrganizationID.String()),
			slog.Int("instanceNum", int(items[0].InstanceNum)))

		// Process based on signal type
		var processErr error
		switch k.signal {
		case "metrics":
			processErr = ingestion.ProcessBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
				k.loop.awsmanager, items, ingestDateint, rpfEstimate, k.loop.exemplarProcessor)
		case "logs":
			processErr = processLogsBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
				k.loop.awsmanager, items, ingestDateint, rpfEstimate, k.loop)
		case "traces":
			processErr = processTracesBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
				k.loop.awsmanager, items, ingestDateint, rpfEstimate, k.loop)
		default:
			processErr = fmt.Errorf("unsupported signal type: %s", k.signal)
		}

		if processErr != nil {
			return fmt.Errorf("failed to process batch for group %s: %w", groupKey, processErr)
		}

		// Record successful processing
		// Note: itemsProcessed metric should be defined in the parent cmd package
		// For now, just log the success
		k.logger.Info("Successfully processed batch from Kafka",
			slog.String("group", groupKey),
			slog.Int("items", len(items)))
	}

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

// Helper functions to handle signal-specific processing
// These are stubs that should be implemented based on your actual processing logic

func processLogsBatch(ctx context.Context, ll *slog.Logger, tmpdir string,
	sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue,
	ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// This should be implemented based on your actual logs processing logic
	// For now, returning an error to indicate it needs implementation
	return fmt.Errorf("logs processing not yet implemented for Kafka consumer")
}

func processTracesBatch(ctx context.Context, ll *slog.Logger, tmpdir string,
	sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue,
	ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// This should be implemented based on your actual traces processing logic
	// For now, returning an error to indicate it needs implementation
	return fmt.Errorf("traces processing not yet implemented for Kafka consumer")
}
