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

// MetricCompactionBoxerConsumer handles metric compaction bundling using CommonConsumer
type MetricCompactionBoxerConsumer struct {
	*CommonConsumer[*messages.MetricCompactionMessage, messages.CompactionKey]
}

// NewMetricCompactionBoxerConsumer creates a new metric compaction boxer consumer using the common consumer framework
func NewMetricCompactionBoxerConsumer(
	ctx context.Context,
	cfg *config.Config,
	store BoxerStore,
	factory *fly.Factory,
) (*MetricCompactionBoxerConsumer, error) {
	processor := newMetricCompactionBoxerProcessor(cfg, store)

	// Set up timing - use shorter accumulation for compaction since it's more time-sensitive
	maxAccumulationTime := 2 * time.Minute
	flushInterval := 30 * time.Second

	// Configure the consumer - consuming from boxer compaction input topic
	registry := cfg.TopicRegistry
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  registry.GetConsumerGroup(config.TopicBoxerMetricsCompact),
		Topic:         registry.GetTopic(config.TopicBoxerMetricsCompact),
		ConsumerGroup: registry.GetConsumerGroup(config.TopicBoxerMetricsCompact),
		FlushInterval: flushInterval,
		StaleAge:      maxAccumulationTime,
		MaxAge:        maxAccumulationTime,
	}

	// Create common consumer with boxer store
	commonConsumer, err := NewCommonConsumer[*messages.MetricCompactionMessage](
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

	return &MetricCompactionBoxerConsumer{
		CommonConsumer: commonConsumer,
	}, nil
}
