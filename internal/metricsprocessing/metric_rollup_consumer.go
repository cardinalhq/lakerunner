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
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// Rollup accumulation times to avoid import cycle with config package
var rollupAccumulationTimes = map[int32]time.Duration{
	10_000:    90 * time.Second,  // 10s->60s: wait max 90 seconds
	60_000:    200 * time.Second, // 60s->300s: wait max 200 seconds
	300_000:   5 * time.Minute,   // 5m->20m: wait max 5 minutes
	1_200_000: 5 * time.Minute,   // 20m->1h: wait max 5 minutes
}

// MetricRollupConsumer consumes MetricRollupBundle messages from boxer
type MetricRollupConsumer struct {
	consumer  fly.Consumer
	store     MetricRollupStore
	processor *MetricRollupProcessor
	cfg       *config.Config
}

// NewMetricRollupConsumer creates a consumer that processes MetricRollupBundle messages from boxer
func NewMetricRollupConsumer(
	ctx context.Context,
	cfg *config.Config,
	factory *fly.Factory,
	store MetricRollupStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricRollupConsumer, error) {
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	processor := newMetricRollupProcessor(cfg, store, storageProvider, cmgr, producer)

	consumer, err := factory.CreateConsumer(cfg.TopicRegistry.GetTopic(config.TopicSegmentsMetricsRollup), cfg.TopicRegistry.GetConsumerGroup(config.TopicSegmentsMetricsRollup))
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &MetricRollupConsumer{
		consumer:  consumer,
		store:     store,
		processor: processor,
		cfg:       cfg,
	}, nil
}

func (c *MetricRollupConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting metric rollup consumer (bundle mode)")

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

func (c *MetricRollupConsumer) processMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	var bundle messages.MetricRollupBundle
	if err := bundle.Unmarshal(msg.Value); err != nil {
		ll.Info("Dropping message that failed to unmarshal as bundle", slog.Any("error", err))
		return nil
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Dropping empty message bundle")
		return nil
	}

	firstMsg := bundle.Messages[0]
	ll.Info("Processing rollup bundle",
		slog.String("organizationID", firstMsg.OrganizationID.String()),
		slog.Int("dateint", int(firstMsg.DateInt)),
		slog.Int("sourceFrequencyMs", int(firstMsg.SourceFrequencyMs)),
		slog.Int("targetFrequencyMs", int(firstMsg.TargetFrequencyMs)),
		slog.Int("instanceNum", int(firstMsg.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	if err := c.processor.ProcessBundle(ctx, &bundle, int32(msg.Partition), msg.Offset); err != nil {
		return fmt.Errorf("failed to process bundle: %w", err)
	}

	return nil
}

// Close stops the consumer
func (c *MetricRollupConsumer) Close() error {
	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}
