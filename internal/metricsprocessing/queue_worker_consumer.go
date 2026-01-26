// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
)

const shutdownTimeout = 30 * time.Second

// BundleProcessor defines the interface for processing a bundle from the work queue
type BundleProcessor interface {
	ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error
}

// QueueWorkerConsumer is a consumer that pulls work from the PostgreSQL work queue
type QueueWorkerConsumer struct {
	manager     *workqueue.Manager
	processor   BundleProcessor
	taskName    string
	concurrency int
}

// NewQueueWorkerConsumer creates a new queue-based worker consumer.
// Concurrency defaults to runtime.NumCPU() if not specified or if 0 is passed.
func NewQueueWorkerConsumer(
	manager *workqueue.Manager,
	processor BundleProcessor,
	taskName string,
	concurrency int,
) *QueueWorkerConsumer {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	return &QueueWorkerConsumer{
		manager:     manager,
		processor:   processor,
		taskName:    taskName,
		concurrency: concurrency,
	}
}

// Run starts the consumer, processing work items from the queue.
// It spawns c.concurrency worker goroutines that each independently
// request and process work items.
func (c *QueueWorkerConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting queue worker consumer",
		slog.String("taskName", c.taskName),
		slog.Int("concurrency", c.concurrency))

	// Start the manager's heartbeat and work claiming loop
	go c.manager.Run(ctx)

	// Spawn worker goroutines
	var wg sync.WaitGroup
	for i := range c.concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.runWorker(ctx, workerID)
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()

	ll.Info("All workers stopped, waiting for shutdown")
	return c.waitForShutdown()
}

// runWorker is the main loop for a single worker goroutine
func (c *QueueWorkerConsumer) runWorker(ctx context.Context, workerID int) {
	ll := logctx.FromContext(ctx).With(slog.Int("workerID", workerID))

	for {
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled, worker stopping")
			return

		default:
			workItem, err := c.manager.RequestWork(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				ll.Error("Failed to request work", slog.Any("error", err))
				continue
			}

			if workItem == nil {
				// No work available, continue polling
				continue
			}

			// Process the work item
			if err := c.processWorkItem(ctx, workItem); err != nil {
				ll.Error("Failed to process work item",
					slog.Int64("workID", workItem.ID()),
					slog.Any("error", err))
			}
		}
	}
}

// processWorkItem processes a single work item
func (c *QueueWorkerConsumer) processWorkItem(ctx context.Context, workItem workqueue.Workable) error {
	// Create a logger with workID attached for tracing through the entire processing
	ll := logctx.FromContext(ctx).With(slog.Int64("workID", workItem.ID()))
	ctx = logctx.WithLogger(ctx, ll)

	ll.Info("Processing work item",
		slog.String("taskName", workItem.TaskName()),
		slog.String("organizationID", workItem.OrganizationID().String()),
		slog.Int("instanceNum", int(workItem.InstanceNum())),
		slog.Int("tries", int(workItem.Tries())))

	// NOP_PROCESSING mode: skip actual processing and mark as completed immediately
	if os.Getenv("NOP_PROCESSING") == "true" {
		ll.Info("NOP_PROCESSING enabled, skipping processing and marking as completed")
		if err := workItem.Complete(); err != nil {
			ll.Error("Failed to complete work item in NOP mode", slog.Any("error", err))
			return err
		}
		return nil
	}

	// Process the bundle using the processor
	if err := c.processor.ProcessBundleFromQueue(ctx, workItem); err != nil {
		ll.Error("Failed to process bundle",
			slog.Any("error", err))

		// Mark work item as failed
		reason := fmt.Sprintf("Failed to process bundle: %v", err)
		if failErr := workItem.Fail(&reason); failErr != nil {
			ll.Error("Failed to mark work item as failed", slog.Any("error", failErr))
		}
		return err
	}

	// Mark work item as completed
	if err := workItem.Complete(); err != nil {
		ll.Error("Failed to complete work item", slog.Any("error", err))
		return err
	}

	ll.Info("Successfully processed work item")

	return nil
}

// Close closes the consumer
func (c *QueueWorkerConsumer) Close() error {
	// The manager doesn't have an explicit Close method,
	// shutdown is handled by context cancellation
	return nil
}

// waitForShutdown waits for outstanding work to complete with a fresh context
func (c *QueueWorkerConsumer) waitForShutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return c.manager.WaitForOutstandingWork(ctx)
}
