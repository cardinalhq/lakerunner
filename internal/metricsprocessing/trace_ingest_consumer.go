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

// TraceIngestConsumer handles trace ingest bundles from boxer
type TraceIngestConsumer struct {
	consumer      fly.Consumer
	store         TraceIngestStore
	processor     *TraceIngestProcessor
	cfg           *config.Config
	topic         string
	consumerGroup string
}

// NewTraceIngestConsumer creates a new trace ingest consumer that processes bundles from boxer
func NewTraceIngestConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store TraceIngestStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*TraceIngestConsumer, error) {
	kafkaProducer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	processor := newTraceIngestProcessor(cfg, store, storageProvider, cmgr, kafkaProducer)

	topic := cfg.TopicRegistry.GetTopic(config.TopicSegmentsTracesIngest)
	consumerGroup := cfg.TopicRegistry.GetConsumerGroup(config.TopicSegmentsTracesIngest)
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &TraceIngestConsumer{
		consumer:      consumer,
		store:         store,
		processor:     processor,
		cfg:           cfg,
		topic:         topic,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *TraceIngestConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting trace ingest consumer", "topic", c.topic, "consumerGroup", c.consumerGroup)

	handler := func(ctx context.Context, msgs []fly.ConsumedMessage) error {
		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg); err != nil {
				return err
			}
		}
		return nil
	}

	return c.consumer.Consume(ctx, handler)
}

func (c *TraceIngestConsumer) processMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	var bundle messages.TraceIngestBundle
	if err := bundle.Unmarshal(msg.Value); err != nil {
		ll.Info("Dropping message that failed to unmarshal as bundle", slog.Any("error", err))
		return nil // Don't fail the batch, just skip this message
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Dropping empty message bundle")
		return nil
	}

	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.IngestKey)

	ll.Info("Processing trace ingest bundle",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Process the bundle using the ProcessBundle method
	return c.processor.ProcessBundle(ctx, key, bundle.Messages, int32(msg.Partition), msg.Offset)
}

func (c *TraceIngestConsumer) Close() error {
	return c.consumer.Close()
}
