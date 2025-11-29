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
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// MessageProcessor defines the interface for processing a single message
type MessageProcessor interface {
	// ProcessMessage processes a single message
	// The processor is responsible for inserting Kafka offsets into the database for deduplication
	ProcessMessage(ctx context.Context, msg fly.ConsumedMessage) error

	// GetTopic returns the topic name
	GetTopic() string

	// GetConsumerGroup returns the consumer group name
	GetConsumerGroup() string
}

// WorkerConsumer is a base consumer for single-message-per-work-item processing
type WorkerConsumer struct {
	consumer      fly.Consumer
	processor     MessageProcessor
	offsetTracker *offsetTracker
	store         OffsetTrackerStore
}

// NewWorkerConsumer creates a new worker consumer
func NewWorkerConsumer(
	consumer fly.Consumer,
	processor MessageProcessor,
	store OffsetTrackerStore,
) *WorkerConsumer {
	return &WorkerConsumer{
		consumer:      consumer,
		processor:     processor,
		store:         store,
		offsetTracker: newOffsetTracker(store, processor.GetConsumerGroup(), processor.GetTopic()),
	}
}

// Run starts the consumer
func (w *WorkerConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	ll.Info("Starting worker consumer",
		"topic", w.processor.GetTopic(),
		"consumerGroup", w.processor.GetConsumerGroup())

	// Start lag monitoring
	go w.monitorLag(ctx)

	return w.consumer.Consume(ctx, w.handleBatch)
}

// monitorLag logs consumer lag every minute
func (w *WorkerConsumer) monitorLag(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	ll := logctx.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := w.consumer.Stats()
			ll.Info("Consumer lag",
				slog.Int64("lag", stats.Lag),
				slog.String("topic", w.processor.GetTopic()),
				slog.String("consumerGroup", w.processor.GetConsumerGroup()))
		}
	}
}

// handleBatch processes a batch of messages
func (w *WorkerConsumer) handleBatch(ctx context.Context, msgs []fly.ConsumedMessage) error {
	for _, msg := range msgs {
		if err := w.handleMessage(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

// handleMessage processes a single message with deduplication and committing
func (w *WorkerConsumer) handleMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	// Check if this message was already processed
	isDuplicate, err := w.checkDuplicate(ctx, msg)
	if err != nil {
		return err
	}

	if isDuplicate {
		// Skip processing but still commit to advance consumer group
		return w.commitMessage(ctx, msg)
	}

	// Process the message - the processor will handle offset tracking in the DB
	if err := w.processor.ProcessMessage(ctx, msg); err != nil {
		ll.Error("Failed to process message",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err)
		return err
	}

	// Commit the message to Kafka consumer group after successful processing
	if err := w.commitMessage(ctx, msg); err != nil {
		return err
	}

	ll.Debug("Successfully processed and committed message",
		"partition", msg.Partition,
		"offset", msg.Offset)

	return nil
}

// checkDuplicate checks if a message has already been processed
func (w *WorkerConsumer) checkDuplicate(ctx context.Context, msg fly.ConsumedMessage) (bool, error) {
	ll := logctx.FromContext(ctx)

	isProcessed, err := w.offsetTracker.isOffsetProcessed(ctx, int32(msg.Partition), msg.Offset)
	if err != nil {
		ll.Error("Failed to check offset processed status",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err)
		return false, err
	}

	if isProcessed {
		ll.Info("Skipping already processed message",
			"partition", msg.Partition,
			"offset", msg.Offset)
	}

	return isProcessed, nil
}

// commitMessage commits a message to the Kafka consumer group
func (w *WorkerConsumer) commitMessage(ctx context.Context, msg fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx)

	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		ll.Error("Failed to commit message to Kafka",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err)
		return err
	}

	return nil
}

// Close closes the consumer
func (w *WorkerConsumer) Close() error {
	return w.consumer.Close()
}
