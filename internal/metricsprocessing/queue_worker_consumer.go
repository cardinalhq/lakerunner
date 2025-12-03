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
	"log/slog"
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
	manager   *workqueue.Manager
	processor BundleProcessor
	taskName  string
}

// NewQueueWorkerConsumer creates a new queue-based worker consumer
func NewQueueWorkerConsumer(
	manager *workqueue.Manager,
	processor BundleProcessor,
	taskName string,
) *QueueWorkerConsumer {
	return &QueueWorkerConsumer{
		manager:   manager,
		processor: processor,
		taskName:  taskName,
	}
}

// Run starts the consumer, processing work items from the queue
func (c *QueueWorkerConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting queue worker consumer", slog.String("taskName", c.taskName))

	// Start the manager's heartbeat and work claiming loop
	go c.manager.Run(ctx)

	// Main work processing loop
	for {
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled, shutting down queue worker")
			return c.waitForShutdown()

		default:
			workItem, err := c.manager.RequestWork(ctx)
			if err != nil {
				if ctx.Err() != nil {
					// Context cancelled, exit gracefully
					return c.waitForShutdown()
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
				// Work item will be failed by processWorkItem
			}
		}
	}
}

// processWorkItem processes a single work item
func (c *QueueWorkerConsumer) processWorkItem(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	ll.Info("Processing work item",
		slog.Int64("workID", workItem.ID()),
		slog.String("taskName", workItem.TaskName()),
		slog.String("organizationID", workItem.OrganizationID().String()),
		slog.Int("instanceNum", int(workItem.InstanceNum())),
		slog.Int("tries", int(workItem.Tries())))

	// Process the bundle using the processor
	if err := c.processor.ProcessBundleFromQueue(ctx, workItem); err != nil {
		ll.Error("Failed to process bundle",
			slog.Int64("workID", workItem.ID()),
			slog.Any("error", err))

		// Mark work item as failed
		reason := fmt.Sprintf("Failed to process bundle: %v", err)
		if failErr := workItem.Fail(&reason); failErr != nil {
			ll.Error("Failed to mark work item as failed",
				slog.Int64("workID", workItem.ID()),
				slog.Any("error", failErr))
		}
		return err
	}

	// Mark work item as completed
	if err := workItem.Complete(); err != nil {
		ll.Error("Failed to complete work item",
			slog.Int64("workID", workItem.ID()),
			slog.Any("error", err))
		return err
	}

	ll.Info("Successfully processed work item",
		slog.Int64("workID", workItem.ID()))

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
