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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// BoxerStore defines the interface required by the metric boxer
type BoxerStore interface {
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
	KafkaJournalBatchUpsert(ctx context.Context, arg []lrdb.KafkaJournalBatchUpsertParams) *lrdb.KafkaJournalBatchUpsertBatchResults
}

// MetricRollupBoxerConsumer handles metric rollup bundling using CommonConsumer
type MetricRollupBoxerConsumer struct {
	*CommonConsumer[*messages.MetricRollupMessage, messages.RollupKey]
}

// NewMetricBoxerConsumer creates a new metric boxer consumer using the common consumer framework
func NewMetricBoxerConsumer(
	ctx context.Context,
	factory *fly.Factory,
	store BoxerStore,
	cfg *config.Config,
) (*MetricRollupBoxerConsumer, error) {

	// Create Kafka producer for sending rollup bundles
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create MetricBoxer processor
	processor := newMetricBoxerProcessor(producer, store)

	// Get the accumulation time based on rollup frequencies
	// We'll use the longest accumulation time as a default
	maxAccumulationTime := time.Duration(0)
	for _, accTime := range rollupAccumulationTimes {
		maxAccumulationTime = max(maxAccumulationTime, accTime)
	}
	if maxAccumulationTime == 0 {
		maxAccumulationTime = 5 * time.Minute // Fallback
	}

	// Set up periodic flushing - flush more frequently for boxing since it's lightweight
	flushInterval := max(maxAccumulationTime/4, 15*time.Second)

	// Configure the consumer - consuming from boxer input topic
	registry := config.DefaultTopicRegistry()
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-boxer-metrics-rollup",
		Topic:         registry.GetTopic(config.TopicBoxerMetricsRollup),
		ConsumerGroup: registry.GetConsumerGroup(config.TopicBoxerMetricsRollup),
		FlushInterval: flushInterval,
		StaleAge:      maxAccumulationTime,
		MaxAge:        maxAccumulationTime,
	}

	// Create common consumer with boxer store
	commonConsumer, err := NewCommonConsumer[*messages.MetricRollupMessage](
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

	return &MetricRollupBoxerConsumer{
		CommonConsumer: commonConsumer,
	}, nil
}
