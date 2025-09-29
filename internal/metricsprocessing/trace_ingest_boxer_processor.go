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

// TraceIngestBoxerProcessor implements the Processor interface for boxing trace ingestion bundles
type TraceIngestBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
	config        *config.Config
}

// newTraceIngestBoxerProcessor creates a new trace ingestion boxer processor instance
func newTraceIngestBoxerProcessor(
	ctx context.Context,
	cfg *config.Config,
	kafkaProducer fly.Producer, store BoxerStore) *TraceIngestBoxerProcessor {
	return &TraceIngestBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
		config:        cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the ingestion processing topic
func (b *TraceIngestBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.IngestKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Create a TraceIngestBundle to send to the ingestion processing topic
	bundle := &messages.TraceIngestBundle{
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

	ll.Info("Boxing trace ingestion bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	bundleBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal trace ingest bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: bundleBytes,
	}

	// Send bundle to ingestion processing topic
	ingestionTopic := b.config.TopicRegistry.GetTopic(config.TopicSegmentsTracesIngest)
	if err := b.kafkaProducer.Send(ctx, ingestionTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send bundle to processing topic: %w", err)
	}

	ll.Info("Successfully sent trace ingestion bundle to processing topic",
		slog.String("topic", ingestionTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the target file size limit for trace ingestion batching
func (b *TraceIngestBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return config.TargetFileSize
}
