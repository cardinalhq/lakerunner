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

// TraceCompactionBoxerConsumer handles trace compaction bundling using CommonConsumer
type TraceCompactionBoxerConsumer struct {
	*CommonConsumer[*messages.TraceCompactionMessage, messages.TraceCompactionKey]
}

// NewTraceCompactionBoxerConsumer creates a new trace compaction boxer consumer using the common consumer framework
func NewTraceCompactionBoxerConsumer(
	ctx context.Context,
	cfg *config.Config,
	store BoxerStore,
	factory *fly.Factory,
) (*TraceCompactionBoxerConsumer, error) {
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	processor := newTraceCompactionBoxerProcessor(cfg, producer, store)

	registry := cfg.TopicRegistry
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  registry.GetConsumerGroup(config.TopicBoxerTracesCompact),
		Topic:         registry.GetTopic(config.TopicBoxerTracesCompact),
		ConsumerGroup: registry.GetConsumerGroup(config.TopicBoxerTracesCompact),
		FlushInterval: 10 * time.Second,
		StaleAge:      20 * time.Second,
		MaxAge:        20 * time.Second,
	}

	commonConsumer, err := NewCommonConsumer[*messages.TraceCompactionMessage](
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

	return &TraceCompactionBoxerConsumer{
		CommonConsumer: commonConsumer,
	}, nil
}
