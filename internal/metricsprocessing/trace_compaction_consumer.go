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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// TraceCompactionConsumer consumes TraceCompactionBundle messages from boxer
type TraceCompactionConsumer struct {
	consumer  fly.Consumer
	store     TraceCompactionStore
	processor *TraceCompactionProcessor
	cfg       *config.Config
}

// NewTraceCompactionConsumer creates a consumer that processes TraceCompactionBundle messages from boxer
func NewTraceCompactionConsumer(
	ctx context.Context,
	cfg *config.Config,
	factory *fly.Factory,
	store TraceCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*TraceCompactionConsumer, error) {
	processor := NewTraceCompactionProcessor(store, storageProvider, cmgr, cfg)

	consumer, err := factory.CreateConsumer(config.DefaultTopicRegistry().GetTopic(config.TopicSegmentsTracesCompact), config.DefaultTopicRegistry().GetConsumerGroup(config.TopicSegmentsTracesCompact))
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &TraceCompactionConsumer{
		consumer:  consumer,
		store:     store,
		processor: processor,
		cfg:       cfg,
	}, nil
}

func (c *TraceCompactionConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting trace compaction consumer (bundle mode)")

	handler := func(handlerCtx context.Context, msgs []fly.ConsumedMessage) error {
		for _, msg := range msgs {
			if err := c.processMessage(handlerCtx, msg); err != nil {
				ll.Error("Error processing message", slog.Any("error", err))
				return err // Return error to prevent committing bad batch
			}
		}
		return nil
	}

	return c.consumer.Consume(ctx, handler)
}

func (c *TraceCompactionConsumer) processMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	var bundle messages.TraceCompactionBundle
	if err := bundle.Unmarshal(msg.Value); err != nil {
		ll.Info("Dropping message that failed to unmarshal as bundle", slog.Any("error", err))
		return nil // Don't fail the batch, just skip this message
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Dropping empty message bundle")
		return nil
	}

	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.TraceCompactionKey)

	ll.Info("Processing compaction bundle",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.DateInt)),
		slog.Int("instanceNum", int(key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	if err := c.processor.ProcessBundle(ctx, key, bundle.Messages, int32(msg.Partition), msg.Offset); err != nil {
		return fmt.Errorf("failed to process bundle: %w", err)
	}

	return nil
}

func (c *TraceCompactionConsumer) Close() error {
	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}
