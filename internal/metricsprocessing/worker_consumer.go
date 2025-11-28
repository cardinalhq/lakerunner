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
	"sort"
	"sync"

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

// WorkerConsumer is a base consumer for single-message-per-work-item processing.
// It processes messages from different partitions in parallel, but maintains
// sequential processing within each partition to preserve offset ordering
// required by the offsetTracker's deduplication logic.
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

	return w.consumer.Consume(ctx, w.handleBatch)
}

// handleBatch processes a batch of messages with per-partition parallelism.
// Messages from different partitions are processed in parallel, but within
// each partition messages are processed sequentially to maintain offset ordering
// required by the offsetTracker's deduplication logic.
func (w *WorkerConsumer) handleBatch(ctx context.Context, msgs []fly.ConsumedMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	ll := logctx.FromContext(ctx)

	// Group messages by partition
	partitionMsgs := make(map[int][]fly.ConsumedMessage)
	for _, msg := range msgs {
		partitionMsgs[msg.Partition] = append(partitionMsgs[msg.Partition], msg)
	}

	// Sort messages within each partition by offset
	for partition := range partitionMsgs {
		pMsgs := partitionMsgs[partition]
		sort.Slice(pMsgs, func(i, j int) bool {
			return pMsgs[i].Offset < pMsgs[j].Offset
		})
	}

	// Process partitions in parallel, messages within partition sequentially
	type partitionResult struct {
		partition    int
		highestMsg   *fly.ConsumedMessage
		err          error
		processCount int
	}

	results := make(chan partitionResult, len(partitionMsgs))
	var wg sync.WaitGroup

	for partition, pMsgs := range partitionMsgs {
		wg.Add(1)
		go func(partition int, messages []fly.ConsumedMessage) {
			defer wg.Done()

			var lastSuccessMsg *fly.ConsumedMessage
			var processCount int

			// Process messages sequentially within this partition
			for i := range messages {
				msg := messages[i]

				// Check for duplicate
				isDuplicate, err := w.checkDuplicate(ctx, msg)
				if err != nil {
					results <- partitionResult{partition: partition, highestMsg: lastSuccessMsg, err: err, processCount: processCount}
					return
				}

				if isDuplicate {
					// Still count as success for commit purposes
					lastSuccessMsg = &messages[i]
					processCount++
					continue
				}

				// Process the message
				if err := w.processor.ProcessMessage(ctx, msg); err != nil {
					ll.Error("Failed to process message",
						"partition", msg.Partition,
						"offset", msg.Offset,
						"error", err)
					results <- partitionResult{partition: partition, highestMsg: lastSuccessMsg, err: err, processCount: processCount}
					return
				}

				lastSuccessMsg = &messages[i]
				processCount++
			}

			results <- partitionResult{partition: partition, highestMsg: lastSuccessMsg, processCount: processCount}
		}(partition, pMsgs)
	}

	// Wait for all partitions to complete
	wg.Wait()
	close(results)

	// Collect results and commit offsets
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}

		// Commit the highest successful offset for this partition
		if r.highestMsg != nil {
			if err := w.commitMessage(ctx, *r.highestMsg); err != nil {
				ll.Error("Failed to commit messages",
					"partition", r.partition,
					"offset", r.highestMsg.Offset,
					"error", err)
				if firstErr == nil {
					firstErr = err
				}
			} else {
				ll.Debug("Committed messages up to offset",
					"partition", r.partition,
					"offset", r.highestMsg.Offset,
					"count", r.processCount)
			}
		}
	}

	return firstErr
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
