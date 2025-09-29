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
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)

// LogIngestBoxerConsumer handles log ingestion bundling using CommonConsumer
type LogIngestBoxerConsumer struct {
	*CommonConsumer[*messages.ObjStoreNotificationMessage, messages.IngestKey]
}

// NewLogIngestBoxerConsumer creates a new log ingestion boxer consumer using the common consumer framework
func NewLogIngestBoxerConsumer(
	ctx context.Context,
	cfg *config.Config,
	store BoxerStore,
	factory *fly.Factory,
) (*LogIngestBoxerConsumer, error) {
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	processor := newLogIngestBoxerProcessor(cfg, producer, store)

	registry := cfg.TopicRegistry
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  registry.GetConsumerGroup(config.TopicObjstoreIngestLogs),
		Topic:         registry.GetTopic(config.TopicObjstoreIngestLogs),
		ConsumerGroup: registry.GetConsumerGroup(config.TopicObjstoreIngestLogs),
		FlushInterval: 10 * time.Second,
		StaleAge:      20 * time.Second,
		MaxAge:        20 * time.Second,
	}

	commonConsumer, err := NewCommonConsumer[*messages.ObjStoreNotificationMessage](
		ctx,
		factory,
		cfg,
		consumerConfig,
		store,
		processor,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create common consumer: %w", err)
	}

	return &LogIngestBoxerConsumer{
		CommonConsumer: commonConsumer,
	}, nil
}
