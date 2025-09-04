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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// KafkaIngestConsumer handles consuming object storage notifications from Kafka and processing them
type KafkaIngestConsumer struct {
	consumer *fly.ObjStoreNotificationConsumer
	loop     *IngestLoopContext
	signal   string
	logger   *slog.Logger
	config   *config.Config
}

// NewKafkaIngestConsumer creates a new Kafka-based ingest consumer
func NewKafkaIngestConsumer(factory *fly.Factory, cfg *config.Config, signal string, groupID string) (*KafkaIngestConsumer, error) {
	if !factory.IsEnabled() {
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
		config:   cfg,
	}, nil
}

// Run starts consuming messages from Kafka and processing them
func (k *KafkaIngestConsumer) Run(ctx context.Context) error {
	k.logger.Info("Starting Kafka ingest consumer", slog.String("signal", k.signal))

	// Note: batchSize is not currently used since we're using a handler-based approach
	// that consumes continuously rather than fetching a specific batch size

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

			// Process each notification individually since they are unrelated
			for _, notif := range notifications {
				if err := k.processItem(ctx, notif); err != nil {
					k.logger.Error("Failed to process item", slog.Any("error", err))
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
			k.logger.Error("Failed to consume from Kafka", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// processItem processes a single Kafka notification
func (k *KafkaIngestConsumer) processItem(ctx context.Context, notif *messages.ObjStoreNotificationMessage) error {
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
			k.logger.Error("Failed to clean up tmpdir", slog.Any("error", err))
		}
	}()

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	ll := k.logger.With(
		slog.String("organizationID", item.OrganizationID.String()),
		slog.Int("instanceNum", int(item.InstanceNum)),
		slog.String("bucket", item.Bucket),
		slog.String("objectID", item.ObjectID))

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
		processErr = ingestion.ProcessBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, []ingest.IngestItem{item}, ingestDateint, rpfEstimate, k.loop.exemplarProcessor, k.config.Metrics.Ingestion)
	case "logs":
		processErr = processLogsBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, item, ingestDateint, rpfEstimate, k.loop)
	case "traces":
		processErr = processTracesBatch(ctx, ll, tmpdir, k.loop.sp, k.loop.mdb,
			k.loop.awsmanager, item, ingestDateint, rpfEstimate, k.loop)
	default:
		processErr = fmt.Errorf("unsupported signal type: %s", k.signal)
	}

	if processErr != nil {
		return fmt.Errorf("failed to process item: %w", processErr)
	}

	// Record successful processing
	k.logger.Debug("Successfully processed item from Kafka",
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

// Helper functions to handle signal-specific processing
// These are stubs that should be implemented based on your actual processing logic

func processLogsBatch(ctx context.Context, ll *slog.Logger, tmpdir string,
	sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, item ingest.IngestItem,
	ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// Call the existing logIngestBatch function from ingest_logs.go
	return logIngestBatch(ctx, ll, tmpdir, sp, mdb, awsmanager, item, ingest_dateint, rpfEstimate, loop)
}

func processTracesBatch(ctx context.Context, ll *slog.Logger, tmpdir string,
	sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, item ingest.IngestItem,
	ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// Call the existing traceIngestBatch function from ingest_traces_cmd.go
	return traceIngestBatch(ctx, ll, tmpdir, sp, mdb, awsmanager, item, ingest_dateint, rpfEstimate, loop)
}
