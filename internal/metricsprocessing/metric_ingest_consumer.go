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

// MetricIngestConsumer handles metric ingest bundles from the work queue
type MetricIngestConsumer struct {
	*QueueWorkerConsumer
	processor *MetricIngestProcessor
}

// NewMetricIngestConsumer creates a new metric ingest consumer that processes bundles from the work queue
func NewMetricIngestConsumer(
	ctx context.Context,
	cfg *config.Config,
	kafkaFactory *fly.Factory,
	store MetricIngestStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricIngestConsumer, error) {
	kafkaProducer, err := kafkaFactory.CreateProducer()
	if err != nil {
		return nil, err
	}

	processor := newMetricIngestProcessor(cfg, store, storageProvider, cmgr, kafkaProducer)

	// Create work queue manager
	workerID := time.Now().UnixNano() // Unique worker ID
	manager := workqueue.NewManager(
		store,
		workerID,
		config.BoxerTaskIngestMetrics,
		workqueue.WithHeartbeatInterval(time.Minute),
		workqueue.WithMaxRetries(5),
	)

	queueConsumer := NewQueueWorkerConsumer(manager, processor, config.BoxerTaskIngestMetrics, cfg.Scaling.GetIngestConcurrency())

	return &MetricIngestConsumer{
		QueueWorkerConsumer: queueConsumer,
		processor:           processor,
	}, nil
}
