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
	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
)

// MetricCompactionConsumer handles metric compaction bundles from the work queue
type MetricCompactionConsumer struct {
	*QueueWorkerConsumer
	processor *MetricCompactionProcessor
	duckDB    *duckdbx.DB
}

// NewMetricCompactionConsumer creates a new metric compaction consumer that processes bundles from the work queue
func NewMetricCompactionConsumer(
	ctx context.Context,
	cfg *config.Config,
	store MetricCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricCompactionConsumer, error) {
	// Create DuckDB instance for aggregation with config settings
	duckDB, err := duckdbx.NewDB(
		duckdbx.WithMetrics(30*time.Second),
		duckdbx.WithDuckDBSettings(duckdbx.DuckDBSettings{
			MemoryLimitMB:        cfg.DuckDB.GetMemoryLimit(),
			TempDirectory:        cfg.DuckDB.GetTempDirectory(),
			MaxTempDirectorySize: cfg.DuckDB.GetMaxTempDirectorySize(),
			PoolSize:             cfg.DuckDB.GetPoolSize(),
			Threads:              cfg.DuckDB.GetThreads(),
		}),
	)
	if err != nil {
		return nil, err
	}

	processor := NewMetricCompactionProcessor(store, storageProvider, cmgr, cfg, duckDB)

	workerID := time.Now().UnixNano() // Unique worker ID
	manager := workqueue.NewManager(
		store,
		workerID,
		config.BoxerTaskCompactMetrics,
		workqueue.WithHeartbeatInterval(time.Minute),
		workqueue.WithMaxRetries(5),
	)

	queueConsumer := NewQueueWorkerConsumer(manager, processor, config.BoxerTaskCompactMetrics, cfg.Scaling.GetWorkerConcurrency())

	return &MetricCompactionConsumer{
		QueueWorkerConsumer: queueConsumer,
		processor:           processor,
		duckDB:              duckDB,
	}, nil
}

// Close closes the consumer and releases resources
func (c *MetricCompactionConsumer) Close() error {
	if c.duckDB != nil {
		return c.duckDB.Close()
	}
	return nil
}
