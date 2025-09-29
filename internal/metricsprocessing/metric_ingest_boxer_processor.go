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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestBoxerProcessor implements the Processor interface for boxing metric ingestion bundles
type MetricIngestBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
	config        *config.Config
}

// newMetricIngestBoxerProcessor creates a new metric ingestion boxer processor instance
func newMetricIngestBoxerProcessor(
	cfg *config.Config,
	kafkaProducer fly.Producer, store BoxerStore) *MetricIngestBoxerProcessor {
	return &MetricIngestBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
		config:        cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the ingestion processing topic
func (b *MetricIngestBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.IngestKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Create a MetricIngestBundle to send to the ingestion processing topic
	bundle := &messages.MetricIngestBundle{
		Version:  1,
		Messages: make([]*messages.ObjStoreNotificationMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to bundle format
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			continue // Skip non-ObjStoreNotificationMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing ingestion bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	bundleBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal metric ingest bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: bundleBytes,
	}

	// Send bundle to ingestion processing topic
	ingestionTopic := b.config.TopicRegistry.GetTopic(config.TopicSegmentsMetricsIngest)
	if err := b.kafkaProducer.Send(ctx, ingestionTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send bundle to processing topic: %w", err)
	}

	// Persist Kafka offsets to prevent duplicate processing on restart
	for _, offset := range kafkaOffsets {
		if len(offset.Offsets) == 0 {
			continue // Skip empty offset arrays
		}

		err := b.store.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
			ConsumerGroup: offset.ConsumerGroup,
			Topic:         offset.Topic,
			PartitionID:   offset.PartitionID,
			Offsets:       offset.Offsets,
			CreatedAt:     nil, // Use default (now())
		})
		if err != nil {
			return fmt.Errorf("failed to persist kafka offsets: %w", err)
		}
	}

	ll.Info("Successfully sent ingestion bundle to processing topic",
		slog.String("topic", ingestionTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the target file size limit for metric ingestion batching
func (b *MetricIngestBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return config.TargetFileSize
}
