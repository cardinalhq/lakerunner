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

// MetricRollupBoxerProcessor implements the Processor interface for boxing metric rollup bundles
type MetricRollupBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
	config        *config.Config
}

// newMetricBoxerProcessor creates a new metric boxer processor instance
func newMetricBoxerProcessor(
	_ context.Context,
	cfg *config.Config,
	kafkaProducer fly.Producer, store BoxerStore) *MetricRollupBoxerProcessor {
	return &MetricRollupBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
		config:        cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the rollup topic
func (b *MetricRollupBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.RollupKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Create a MetricRollupBundle to send to the rollup topic
	bundle := &messages.MetricRollupBundle{
		Version:  1,
		Messages: make([]*messages.MetricRollupMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to MetricRollupMessage format
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricRollupMessage)
		if !ok {
			continue // Skip non-MetricRollupMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing rollup bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("sourceFrequencyMs", int(group.Key.SourceFrequencyMs)),
		slog.Int("targetFrequencyMs", int(group.Key.TargetFrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int64("truncatedTimebox", group.Key.TruncatedTimebox),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	msgBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal rollup bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: msgBytes,
	}

	// Send to rollup topic
	rollupTopic := b.config.TopicRegistry.GetTopic(config.TopicSegmentsMetricsRollup)
	if err := b.kafkaProducer.Send(ctx, rollupTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send rollup bundle to rollup topic: %w", err)
	}

	ll.Info("Successfully sent rollup bundle to rollup topic",
		slog.String("topic", rollupTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the estimated record count for the target frequency
func (b *MetricRollupBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.RollupKey) int64 {
	return b.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.TargetFrequencyMs)
}
