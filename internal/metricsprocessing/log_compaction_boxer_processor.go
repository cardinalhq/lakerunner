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
	"runtime"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// LogCompactionBoxerProcessor implements boxing for log compaction messages
type LogCompactionBoxerProcessor struct {
	store         LogCompactionStore
	kafkaProducer fly.Producer
	config        *config.Config
}

// newLogCompactionBoxerProcessor creates a new log compaction boxer processor
func newLogCompactionBoxerProcessor(
	cfg *config.Config,
	producer fly.Producer,
	store LogCompactionStore,
) *LogCompactionBoxerProcessor {
	return &LogCompactionBoxerProcessor{
		store:         store,
		kafkaProducer: producer,
		config:        cfg,
	}
}

// Process implements the processor interface for log compaction boxing
func (p *LogCompactionBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.LogCompactionKey], kafkaCommitData *KafkaCommitData) error {
	defer runtime.GC() // TODO find a way to not need this

	ll := logctx.FromContext(ctx)

	ll.Info("Processing log compaction boxer group",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)))

	// Create bundle
	bundle := messages.LogCompactionBundle{
		Version:  1,
		Messages: make([]*messages.LogCompactionMessage, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to log compaction messages
	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.LogCompactionMessage)
		if !ok {
			return fmt.Errorf("expected LogCompactionMessage, got %T", accMsg.Message)
		}
		bundle.Messages[i] = msg
	}

	// Marshal bundle
	bundleBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal log compaction bundle: %w", err)
	}

	// Create Kafka message key for proper partitioning
	kafkaMessage := fly.Message{
		Key:   []byte(fmt.Sprintf("%s-%d-%d", group.Key.OrganizationID.String(), group.Key.DateInt, group.Key.InstanceNum)),
		Value: bundleBytes,
	}

	// Send to compaction topic
	compactionTopic := p.config.TopicRegistry.GetTopic(config.TopicSegmentsLogsCompact)
	if err := p.kafkaProducer.Send(ctx, compactionTopic, kafkaMessage); err != nil {
		return fmt.Errorf("failed to send log compaction bundle to Kafka: %w", err)
	}

	ll.Info("Successfully sent log compaction bundle",
		slog.String("topic", compactionTopic),
		slog.Int("bundleSize", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the target record count for a grouping key
func (p *LogCompactionBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.LogCompactionKey) int64 {
	return p.store.GetLogEstimate(ctx, groupingKey.OrganizationID)
}
