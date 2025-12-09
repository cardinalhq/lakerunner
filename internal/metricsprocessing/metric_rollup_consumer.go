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
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
)

// Rollup accumulation times to avoid import cycle with config package
var rollupAccumulationTimes = map[int32]time.Duration{
	10_000:    90 * time.Second,  // 10s->60s: wait max 90 seconds
	60_000:    200 * time.Second, // 60s->300s: wait max 200 seconds
	300_000:   5 * time.Minute,   // 5m->20m: wait max 5 minutes
	1_200_000: 5 * time.Minute,   // 20m->1h: wait max 5 minutes
}

// MetricRollupConsumer handles metric rollup bundles from the work queue
type MetricRollupConsumer struct {
	*QueueWorkerConsumer
	processor *MetricRollupProcessor
}

// NewMetricRollupConsumer creates a new metric rollup consumer that processes bundles from the work queue
func NewMetricRollupConsumer(
	ctx context.Context,
	cfg *config.Config,
	kafkaFactory *fly.Factory,
	store MetricRollupStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricRollupConsumer, error) {
	kafkaProducer, err := kafkaFactory.CreateProducer()
	if err != nil {
		return nil, err
	}

	processor := newMetricRollupProcessor(cfg, store, storageProvider, cmgr, kafkaProducer)

	workerID := time.Now().UnixNano() // Unique worker ID
	manager := workqueue.NewManager(
		store,
		workerID,
		config.BoxerTaskRollupMetrics,
		workqueue.WithHeartbeatInterval(time.Minute),
		workqueue.WithMaxRetries(5),
	)

	queueConsumer := NewQueueWorkerConsumer(manager, processor, config.BoxerTaskRollupMetrics)

	return &MetricRollupConsumer{
		QueueWorkerConsumer: queueConsumer,
		processor:           processor,
	}, nil
}
