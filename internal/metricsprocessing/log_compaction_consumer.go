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
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
)

// LogCompactionConsumer handles log compaction bundles from the work queue
type LogCompactionConsumer struct {
	*QueueWorkerConsumer
	processor *LogCompactionProcessor
}

// NewLogCompactionConsumer creates a new log compaction consumer that processes bundles from the work queue
func NewLogCompactionConsumer(
	ctx context.Context,
	cfg *config.Config,
	store LogCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*LogCompactionConsumer, error) {
	// Create processor without Kafka producer (no longer needed for work queue)
	processor := NewLogCompactionProcessor(store, storageProvider, cmgr, cfg)

	// Create work queue manager
	workerID := time.Now().UnixNano() // Unique worker ID
	manager := workqueue.NewManager(
		store,
		workerID,
		config.BoxerTaskCompactLogs,
		workqueue.WithHeartbeatInterval(time.Minute),
		workqueue.WithMaxRetries(5),
	)

	queueConsumer := NewQueueWorkerConsumer(manager, processor, config.BoxerTaskCompactLogs, cfg.Scaling.GetWorkerConcurrency())

	return &LogCompactionConsumer{
		QueueWorkerConsumer: queueConsumer,
		processor:           processor,
	}, nil
}
