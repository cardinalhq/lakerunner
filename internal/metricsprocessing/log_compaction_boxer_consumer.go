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

// LogCompactionBoxerConsumer handles log compaction using the generic framework
type LogCompactionBoxerConsumer struct {
	*CommonConsumer[*messages.LogCompactionMessage, messages.LogCompactionKey]
}

// NewLogCompactionBoxerConsumer creates a new log compaction boxer consumer using the generic framework
func NewLogCompactionBoxerConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store LogCompactionStore,
) (*LogCompactionBoxerConsumer, error) {

	// Create Kafka producer for sending compaction bundles
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create LogCompactionBoxer processor
	processor := newLogCompactionBoxerProcessor(cfg, producer, store)

	// Set up timing - use shorter accumulation for compaction since it's more time-sensitive
	maxAccumulationTime := 2 * time.Minute
	flushInterval := 30 * time.Second

	// Configure the consumer - consuming from boxer compaction input topic
	registry := cfg.TopicRegistry
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-boxer-logs-compact",
		Topic:         registry.GetTopic(config.TopicBoxerLogsCompact),
		ConsumerGroup: registry.GetConsumerGroup(config.TopicBoxerLogsCompact),
		FlushInterval: flushInterval,
		StaleAge:      maxAccumulationTime,
		MaxAge:        maxAccumulationTime,
	}

	// Create common consumer with boxer store
	commonConsumer, err := NewCommonConsumer[*messages.LogCompactionMessage](
		ctx,
		factory,
		cfg,
		consumerConfig,
		store,
		processor,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create log compaction boxer consumer: %w", err)
	}

	return &LogCompactionBoxerConsumer{
		CommonConsumer: commonConsumer,
	}, nil
}
