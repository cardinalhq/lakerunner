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
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricCompactionBoxerProcessor implements the Processor interface for boxing metric compaction bundles
type MetricCompactionBoxerProcessor struct {
	store  BoxerStore
	config *config.Config
}

// newMetricCompactionBoxerProcessor creates a new metric compaction boxer processor instance
func newMetricCompactionBoxerProcessor(cfg *config.Config, store BoxerStore) *MetricCompactionBoxerProcessor {
	return &MetricCompactionBoxerProcessor{
		store:  store,
		config: cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the compaction topic
func (b *MetricCompactionBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.CompactionKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Create a MetricCompactionBundle to send to the compaction topic
	bundle := &messages.MetricCompactionBundle{
		Version:  1,
		Messages: make([]*messages.MetricCompactionMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to MetricCompactionMessage format
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricCompactionMessage)
		if !ok {
			continue // Skip non-MetricCompactionMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing compaction bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("frequencyMs", int(group.Key.FrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	msgBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal compaction bundle: %w", err)
	}

	// Enqueue bundle to work queue
	workID, err := workqueue.AddBundle(ctx, b.store, config.BoxerTaskCompactMetrics, group.Key.OrganizationID, group.Key.InstanceNum, msgBytes)
	if err != nil {
		return fmt.Errorf("failed to enqueue bundle to work queue: %w", err)
	}

	ll.Info("Successfully enqueued metric compaction bundle to work queue",
		slog.Int64("workID", workID),
		slog.Int("bundledMessages", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the estimated record count for the frequency
func (b *MetricCompactionBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return b.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.FrequencyMs)
}

// ShouldEmitImmediately returns false - metric compaction always uses normal grouping.
func (b *MetricCompactionBoxerProcessor) ShouldEmitImmediately(msg *messages.MetricCompactionMessage) bool {
	return false
}
