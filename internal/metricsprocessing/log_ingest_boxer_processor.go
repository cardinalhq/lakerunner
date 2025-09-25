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

// LogIngestBoxerProcessor implements the Processor interface for boxing log ingestion bundles
type LogIngestBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
	config        *config.Config
}

// newLogIngestBoxerProcessor creates a new log ingestion boxer processor instance
func newLogIngestBoxerProcessor(
	ctx context.Context,
	cfg *config.Config,
	kafkaProducer fly.Producer, store BoxerStore) *LogIngestBoxerProcessor {
	return &LogIngestBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
		config:        cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the ingestion processing topic
func (b *LogIngestBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.IngestKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Create a LogIngestBundle to send to the ingestion processing topic
	bundle := &messages.LogIngestBundle{
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

	ll.Info("Boxing log ingestion bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	bundleBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal log ingest bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: bundleBytes,
	}

	// Send bundle to ingestion processing topic
	ingestionTopic := b.config.TopicRegistry.GetTopic(config.TopicSegmentsLogsIngest)
	if err := b.kafkaProducer.Send(ctx, ingestionTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send bundle to processing topic: %w", err)
	}

	ll.Info("Successfully sent log ingestion bundle to processing topic",
		slog.String("topic", ingestionTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	// Note: Offset tracking happens automatically when segments are inserted
	// No manual offset commit needed here

	return nil
}

// GetTargetRecordCount returns the estimated record count for log ingestion
func (b *LogIngestBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return b.store.GetLogEstimate(ctx, groupingKey.OrganizationID)
}
