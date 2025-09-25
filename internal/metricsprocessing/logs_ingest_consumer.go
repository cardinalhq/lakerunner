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

// LogIngestConsumer handles log ingest bundles from boxer
type LogIngestConsumer struct {
	consumer      fly.Consumer
	store         LogIngestStore
	processor     *LogIngestProcessor
	cfg           *config.Config
	topic         string
	consumerGroup string
}

// NewLogIngestConsumer creates a new log ingest consumer that processes bundles from boxer
func NewLogIngestConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store LogIngestStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*LogIngestConsumer, error) {
	kafkaProducer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	processor := newLogIngestProcessor(cfg, store, storageProvider, cmgr, kafkaProducer)

	topic := cfg.TopicRegistry.GetTopic(config.TopicSegmentsLogsIngest)
	consumerGroup := cfg.TopicRegistry.GetConsumerGroup(config.TopicSegmentsLogsIngest)
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &LogIngestConsumer{
		consumer:      consumer,
		store:         store,
		processor:     processor,
		cfg:           cfg,
		topic:         topic,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *LogIngestConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting log ingest consumer", "topic", c.topic, "consumerGroup", c.consumerGroup)

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

func (c *LogIngestConsumer) processMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	var bundle messages.LogIngestBundle
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

	ll.Info("Processing log ingest bundle",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	return c.processor.ProcessBundle(ctx, key, bundle.Messages, int32(msg.Partition), msg.Offset)
}

func (c *LogIngestConsumer) Close() error {
	return c.consumer.Close()
}
