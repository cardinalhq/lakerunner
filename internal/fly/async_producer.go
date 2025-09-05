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

package fly

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// MessageResult represents the result of an async message send
type MessageResult struct {
	Message Message
	Error   error
	Topic   string
}

// AsyncProducer wraps a regular producer to provide async sending with callbacks
type AsyncProducer interface {
	// SendAsync sends a message asynchronously and calls the callback when done
	SendAsync(ctx context.Context, topic string, message Message, callback func(error))
	// BatchSendAsync sends messages asynchronously with individual callbacks
	BatchSendAsync(ctx context.Context, topic string, messages []Message, callbacks []func(error))
	// Close waits for pending messages and closes the producer
	Close() error
}

type asyncProducer struct {
	producer  Producer
	workers   int
	workQueue chan *asyncWork
	wg        sync.WaitGroup
	closing   chan struct{}
	closed    bool
	closeMu   sync.Mutex
	logger    *slog.Logger
}

type asyncWork struct {
	ctx      context.Context
	topic    string
	message  Message
	callback func(error)
}

// NewAsyncProducer creates a new async producer with the specified number of workers
func NewAsyncProducer(producer Producer, workers int, logger *slog.Logger) AsyncProducer {
	if workers <= 0 {
		workers = 10
	}

	ap := &asyncProducer{
		producer:  producer,
		workers:   workers,
		workQueue: make(chan *asyncWork, workers*10), // Buffer for smooth operation
		closing:   make(chan struct{}),
		logger:    logger,
	}

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		ap.wg.Add(1)
		go ap.worker(i)
	}

	return ap
}

func (ap *asyncProducer) worker(id int) {
	defer ap.wg.Done()

	for {
		select {
		case work := <-ap.workQueue:
			if work == nil {
				return // Channel closed
			}

			// Send the message synchronously within this worker
			err := ap.producer.Send(work.ctx, work.topic, work.message)

			// Call the callback with the result
			if work.callback != nil {
				work.callback(err)
			}

			if err != nil {
				ap.logger.Debug("Async worker failed to send message",
					slog.Int("worker_id", id),
					slog.String("topic", work.topic),
					slog.Any("error", err))
			}

		case <-ap.closing:
			// Drain remaining work before exiting
			for {
				select {
				case work := <-ap.workQueue:
					if work == nil {
						return
					}
					// Try to send with a short timeout during shutdown
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := ap.producer.Send(ctx, work.topic, work.message)
					cancel()
					if work.callback != nil {
						work.callback(err)
					}
				default:
					return
				}
			}
		}
	}
}

func (ap *asyncProducer) SendAsync(ctx context.Context, topic string, message Message, callback func(error)) {
	ap.closeMu.Lock()
	if ap.closed {
		ap.closeMu.Unlock()
		if callback != nil {
			callback(fmt.Errorf("producer is closed"))
		}
		return
	}
	ap.closeMu.Unlock()

	work := &asyncWork{
		ctx:      ctx,
		topic:    topic,
		message:  message,
		callback: callback,
	}

	select {
	case ap.workQueue <- work:
		// Successfully queued
	case <-ctx.Done():
		// Context cancelled
		if callback != nil {
			callback(ctx.Err())
		}
	case <-time.After(10 * time.Second):
		// Queue is full, timeout
		if callback != nil {
			callback(fmt.Errorf("timeout queueing message, work queue is full"))
		}
	}
}

func (ap *asyncProducer) BatchSendAsync(ctx context.Context, topic string, messages []Message, callbacks []func(error)) {
	if len(messages) != len(callbacks) {
		panic("messages and callbacks must have the same length")
	}

	for i, msg := range messages {
		ap.SendAsync(ctx, topic, msg, callbacks[i])
	}
}

func (ap *asyncProducer) Close() error {
	ap.closeMu.Lock()
	if ap.closed {
		ap.closeMu.Unlock()
		return nil
	}
	ap.closed = true
	ap.closeMu.Unlock()

	// Signal workers to start shutting down
	close(ap.closing)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		ap.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers finished
	case <-time.After(30 * time.Second):
		ap.logger.Warn("Timeout waiting for async producer workers to finish")
	}

	// Close the work queue
	close(ap.workQueue)

	// Close the underlying producer
	return ap.producer.Close()
}

// AsyncBatchProducer provides high-performance batch async sending
type AsyncBatchProducer struct {
	producer  Producer
	batchSize int
	interval  time.Duration

	mu      sync.Mutex
	pending map[string][]*pendingMessage // Topic -> pending messages
	timer   *time.Timer
	closing chan struct{}
	closed  bool
	wg      sync.WaitGroup
	logger  *slog.Logger
}

type pendingMessage struct {
	message  Message
	callback func(error)
	addedAt  time.Time
}

// NewAsyncBatchProducer creates a producer that batches messages for efficiency
func NewAsyncBatchProducer(producer Producer, batchSize int, interval time.Duration, logger *slog.Logger) *AsyncBatchProducer {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	abp := &AsyncBatchProducer{
		producer:  producer,
		batchSize: batchSize,
		interval:  interval,
		pending:   make(map[string][]*pendingMessage),
		closing:   make(chan struct{}),
		logger:    logger,
	}

	abp.wg.Add(1)
	go abp.flusher()

	return abp
}

func (abp *AsyncBatchProducer) SendAsync(ctx context.Context, topic string, message Message, callback func(error)) {
	abp.mu.Lock()
	defer abp.mu.Unlock()

	if abp.closed {
		if callback != nil {
			callback(fmt.Errorf("producer is closed"))
		}
		return
	}

	pm := &pendingMessage{
		message:  message,
		callback: callback,
		addedAt:  time.Now(),
	}

	abp.pending[topic] = append(abp.pending[topic], pm)

	// Flush if we've reached batch size
	if len(abp.pending[topic]) >= abp.batchSize {
		abp.flushTopic(topic)
	} else if abp.timer == nil {
		// Start timer for interval-based flush
		abp.timer = time.AfterFunc(abp.interval, func() {
			abp.mu.Lock()
			defer abp.mu.Unlock()
			abp.flushAll()
		})
	}
}

func (abp *AsyncBatchProducer) flushTopic(topic string) {
	messages := abp.pending[topic]
	if len(messages) == 0 {
		return
	}

	// Clear pending for this topic
	abp.pending[topic] = nil

	// Convert to Message slice
	msgs := make([]Message, len(messages))
	for i, pm := range messages {
		msgs[i] = pm.message
	}

	// Send batch asynchronously
	abp.wg.Add(1)
	go func() {
		defer abp.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := abp.producer.BatchSend(ctx, topic, msgs)

		// Notify all callbacks
		for _, pm := range messages {
			if pm.callback != nil {
				pm.callback(err)
			}
		}

		if err != nil {
			abp.logger.Error("Failed to send batch",
				slog.String("topic", topic),
				slog.Int("count", len(messages)),
				slog.Any("error", err))
		} else {
			abp.logger.Debug("Successfully sent batch",
				slog.String("topic", topic),
				slog.Int("count", len(messages)))
		}
	}()
}

func (abp *AsyncBatchProducer) flushAll() {
	for topic := range abp.pending {
		abp.flushTopic(topic)
	}
	abp.timer = nil
}

func (abp *AsyncBatchProducer) flusher() {
	defer abp.wg.Done()

	ticker := time.NewTicker(abp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			abp.mu.Lock()
			abp.flushAll()
			abp.mu.Unlock()

		case <-abp.closing:
			// Final flush
			abp.mu.Lock()
			abp.flushAll()
			abp.mu.Unlock()
			return
		}
	}
}

func (abp *AsyncBatchProducer) Close() error {
	abp.mu.Lock()
	if abp.closed {
		abp.mu.Unlock()
		return nil
	}
	abp.closed = true

	// Stop timer
	if abp.timer != nil {
		abp.timer.Stop()
	}

	// Flush remaining messages
	abp.flushAll()
	abp.mu.Unlock()

	// Signal flusher to stop
	close(abp.closing)

	// Wait for all async operations to complete
	done := make(chan struct{})
	go func() {
		abp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All operations finished
	case <-time.After(30 * time.Second):
		abp.logger.Warn("Timeout waiting for async batch producer to finish")
	}

	// Close underlying producer
	return abp.producer.Close()
}
