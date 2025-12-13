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

// TraceCompactionBoxerProcessor implements the Processor interface for boxing trace compaction bundles
type TraceCompactionBoxerProcessor struct {
	store  BoxerStore
	config *config.Config
}

// newTraceCompactionBoxerProcessor creates a new trace compaction boxer processor instance
func newTraceCompactionBoxerProcessor(cfg *config.Config, store BoxerStore) *TraceCompactionBoxerProcessor {
	return &TraceCompactionBoxerProcessor{
		store:  store,
		config: cfg,
	}
}

// Process implements the Processor interface and sends the bundle to the compaction topic
func (b *TraceCompactionBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.TraceCompactionKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	bundle := &messages.TraceCompactionBundle{
		Version:  1,
		Messages: make([]*messages.TraceCompactionMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			continue // Skip non-TraceCompactionMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing compaction bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	msgBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal compaction bundle: %w", err)
	}

	// Enqueue bundle to work queue
	workID, err := workqueue.AddBundle(ctx, b.store, config.BoxerTaskCompactTraces, group.Key.OrganizationID, group.Key.InstanceNum, msgBytes)
	if err != nil {
		return fmt.Errorf("failed to enqueue bundle to work queue: %w", err)
	}

	ll.Info("Successfully enqueued trace compaction bundle to work queue",
		slog.Int64("workID", workID),
		slog.Int("bundledMessages", len(bundle.Messages)))

	return nil
}

// GetTargetRecordCount returns the estimated record count for traces
func (b *TraceCompactionBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.TraceCompactionKey) int64 {
	return b.store.GetTraceEstimate(ctx, groupingKey.OrganizationID)
}

// ShouldEmitImmediately returns false - trace compaction always uses normal grouping.
func (b *TraceCompactionBoxerProcessor) ShouldEmitImmediately(msg *messages.TraceCompactionMessage) bool {
	return false
}
