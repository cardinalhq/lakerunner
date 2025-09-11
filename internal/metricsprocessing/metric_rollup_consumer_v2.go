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
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// Rollup accumulation times to avoid import cycle with config package
var rollupAccumulationTimesV2 = map[int32]time.Duration{
	10_000:    90 * time.Second,  // 10s->60s: wait max 90 seconds
	60_000:    200 * time.Second, // 60s->300s: wait max 200 seconds
	300_000:   5 * time.Minute,   // 5m->20m: wait max 5 minutes
	1_200_000: 5 * time.Minute,   // 20m->1h: wait max 5 minutes
}

// MetricRollupConsumerV2 handles metric rollup using CommonConsumer
type MetricRollupConsumerV2 struct {
	*CommonConsumer[*messages.MetricRollupMessage, messages.RollupKey]
}

// NewMetricRollupConsumerV2 creates a new metric rollup consumer using the common consumer framework
func NewMetricRollupConsumerV2(
	ctx context.Context,
	factory *fly.Factory,
	store RollupStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) (*MetricRollupConsumerV2, error) {

	// Create Kafka producer for sending rollup messages
	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create MetricRollupProcessor (reusing existing)
	processor := newMetricRollupProcessor(store, storageProvider, cmgr, cfg, producer)

	// Get the accumulation time based on rollup frequencies
	// We'll use the longest accumulation time as a default
	maxAccumulationTime := time.Duration(0)
	for _, accTime := range rollupAccumulationTimesV2 {
		maxAccumulationTime = max(maxAccumulationTime, accTime)
	}
	if maxAccumulationTime == 0 {
		maxAccumulationTime = 5 * time.Minute // Fallback
	}

	// Set up periodic flushing - flush more frequently for rollups since they have tighter time windows
	flushInterval := max(maxAccumulationTime/2, 30*time.Second)

	// Configure the consumer
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-metric-rollup-v2",
		Topic:         "lakerunner.segments.metrics.rollup",
		ConsumerGroup: "lakerunner.rollup.metrics",
		FlushInterval: flushInterval,
		StaleAge:      maxAccumulationTime,
		MaxAge:        maxAccumulationTime,
	}

	// Create common consumer
	commonConsumer, err := NewCommonConsumer[*messages.MetricRollupMessage, messages.RollupKey](
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

	return &MetricRollupConsumerV2{
		CommonConsumer: commonConsumer,
	}, nil
}