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

// MetricCompactionConsumer handles metric compaction using the generic framework
type MetricCompactionConsumer struct {
	*CommonConsumer[*messages.MetricCompactionMessage, messages.CompactionKey]
}

// NewMetricCompactionConsumer creates a new metric compaction consumer using the generic framework
func NewMetricCompactionConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store MetricCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricCompactionConsumer, error) {

	// Create processor
	processor := NewMetricCompactionProcessor(store, storageProvider, cmgr, cfg)

	// Configure the consumer
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-metric-compaction-v2",
		Topic:         "lakerunner.segments.metrics.compact",
		ConsumerGroup: "lakerunner.compact.metrics",
		FlushInterval: 1 * time.Minute,
		StaleAge:      1 * time.Minute,
		MaxAge:        5 * time.Minute,
	}

	// Create generic consumer
	genericConsumer, err := NewCommonConsumer[*messages.MetricCompactionMessage](
		ctx,
		factory,
		cfg,
		consumerConfig,
		store,
		processor,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic compaction consumer: %w", err)
	}

	return &MetricCompactionConsumer{
		CommonConsumer: genericConsumer,
	}, nil
}
