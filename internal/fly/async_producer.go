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
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerCallback is called after a message send attempt completes
type ProducerCallback func(msg Message, partition int, offset int64, err error)

// AsyncProducer provides asynchronous Kafka message production with callbacks
type AsyncProducer interface {
	Producer // Embed synchronous interface for compatibility

	// SendAsync queues a message for async sending with optional callback
	SendAsync(ctx context.Context, topic string, message Message, callback ProducerCallback) error

	// SendAsyncBatch queues multiple messages with a single callback
	SendAsyncBatch(ctx context.Context, topic string, messages []Message, callback ProducerCallback) error

	// Flush waits for all pending messages to be sent
	Flush(ctx context.Context) error

	// PendingCount returns the number of messages waiting to be sent
	PendingCount() int

	// Stats returns producer statistics
	Stats() producerStats
}

// producerStats tracks async producer performance
type producerStats struct {
	MessagesSent     int64
	MessagesFailed   int64
	MessagesQueued   int64
	CallbacksInvoked int64
	BatchesSent      int64
	BytesSent        int64
}

// asyncMessage represents a message with its callback
type asyncMessage struct {
	ctx      context.Context
	topic    string
	message  Message
	callback ProducerCallback
}

// asyncKafkaProducer implements AsyncProducer with batching and callbacks
type asyncKafkaProducer struct {
	*kafkaProducer // Embed sync producer for shared functionality

	// Async channels
	sendChan  chan *asyncMessage
	flushChan chan chan error
	closeChan chan chan error

	// Worker pool
	workers int
	wg      sync.WaitGroup

	// Stats
	stats     producerStats
	statsLock sync.RWMutex

	// Pending tracking
	pending atomic.Int64

	// Configuration
	asyncBatchSize    int
	asyncBatchTimeout time.Duration
	channelSize       int
}

// AsyncProducerConfig extends ProducerConfig with async-specific settings
type AsyncProducerConfig struct {
	ProducerConfig

	// Number of worker goroutines
	Workers int

	// Size of the send channel buffer
	ChannelSize int

	// Async batching (can be different from sync batch settings)
	AsyncBatchSize    int
	AsyncBatchTimeout time.Duration
}

// NewAsyncProducer creates a new async producer
func NewAsyncProducer(config AsyncProducerConfig) AsyncProducer {
	// Set defaults
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.ChannelSize <= 0 {
		config.ChannelSize = 1000
	}
	if config.AsyncBatchSize <= 0 {
		config.AsyncBatchSize = 100
	}
	if config.AsyncBatchTimeout <= 0 {
		config.AsyncBatchTimeout = 100 * time.Millisecond
	}

	// Create base sync producer
	syncProducer := NewProducer(config.ProducerConfig).(*kafkaProducer)

	p := &asyncKafkaProducer{
		kafkaProducer:     syncProducer,
		sendChan:          make(chan *asyncMessage, config.ChannelSize),
		flushChan:         make(chan chan error),
		closeChan:         make(chan chan error),
		workers:           config.Workers,
		asyncBatchSize:    config.AsyncBatchSize,
		asyncBatchTimeout: config.AsyncBatchTimeout,
		channelSize:       config.ChannelSize,
	}

	// Start worker goroutines
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	return p
}

// SendAsync queues a message for asynchronous sending
func (p *asyncKafkaProducer) SendAsync(ctx context.Context, topic string, message Message, callback ProducerCallback) error {
	msg := &asyncMessage{
		ctx:      ctx,
		topic:    topic,
		message:  message,
		callback: callback,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.sendChan <- msg:
		p.pending.Add(1)
		p.updateStats(func(s *producerStats) {
			s.MessagesQueued++
		})
		return nil
	}
}

// SendAsyncBatch queues multiple messages with a single callback
func (p *asyncKafkaProducer) SendAsyncBatch(ctx context.Context, topic string, messages []Message, callback ProducerCallback) error {
	for _, msg := range messages {
		if err := p.SendAsync(ctx, topic, msg, callback); err != nil {
			return err
		}
	}
	return nil
}

// worker processes messages from the send channel
func (p *asyncKafkaProducer) worker(id int) {
	defer p.wg.Done()

	batch := make([]*asyncMessage, 0, p.asyncBatchSize)
	ticker := time.NewTicker(p.asyncBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-p.sendChan:
			if !ok {
				// Channel closed, send remaining batch
				if len(batch) > 0 {
					p.sendBatch(batch)
				}
				return
			}

			batch = append(batch, msg)
			if len(batch) >= p.asyncBatchSize {
				p.sendBatch(batch)
				batch = batch[:0]
				ticker.Reset(p.asyncBatchTimeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				p.sendBatch(batch)
				batch = batch[:0]
			}

		case done := <-p.flushChan:
			if len(batch) > 0 {
				p.sendBatch(batch)
				batch = batch[:0]
			}
			done <- nil

		case done := <-p.closeChan:
			if len(batch) > 0 {
				p.sendBatch(batch)
			}
			done <- nil
			return
		}
	}
}

// sendBatch sends a batch of messages and invokes callbacks
func (p *asyncKafkaProducer) sendBatch(batch []*asyncMessage) {
	// Group by topic for efficient sending
	byTopic := make(map[string][]*asyncMessage)
	for _, msg := range batch {
		byTopic[msg.topic] = append(byTopic[msg.topic], msg)
	}

	for topic, msgs := range byTopic {
		// Convert to kafka messages
		kafkaMsgs := make([]kafka.Message, len(msgs))
		for i, m := range msgs {
			kafkaMsgs[i] = m.message.ToKafkaMessage()
		}

		// Get writer and send
		writer := p.getWriter(topic)

		// Use a context that won't be cancelled for the actual send
		sendCtx := context.Background()
		err := writer.WriteMessages(sendCtx, kafkaMsgs...)

		// Update stats
		p.updateStats(func(s *producerStats) {
			s.BatchesSent++
			if err == nil {
				s.MessagesSent += int64(len(msgs))
				for _, m := range msgs {
					s.BytesSent += int64(len(m.message.Value))
				}
			} else {
				s.MessagesFailed += int64(len(msgs))
			}
		})

		// Invoke callbacks
		for _, m := range msgs {
			p.pending.Add(-1)

			if m.callback != nil {
				// Extract partition and offset from response if available
				var partition int
				var offset int64

				// Note: kafka-go Writer doesn't return partition/offset info directly
				// In production, you might want to use a different approach
				partition = -1
				offset = -1

				m.callback(m.message, partition, offset, err)

				p.updateStats(func(s *producerStats) {
					s.CallbacksInvoked++
				})
			}
		}
	}
}

// Flush waits for all pending messages to be sent
func (p *asyncKafkaProducer) Flush(ctx context.Context) error {
	// Send flush signal to all workers
	done := make(chan error, p.workers)

	for i := 0; i < p.workers; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.flushChan <- done:
		}
	}

	// Wait for all workers to acknowledge
	for i := 0; i < p.workers; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}

	// Wait for pending count to reach zero
	deadline := time.Now().Add(30 * time.Second)
	for p.pending.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if p.pending.Load() > 0 {
		return fmt.Errorf("flush timeout: %d messages still pending", p.pending.Load())
	}

	return nil
}

// PendingCount returns the number of messages waiting to be sent
func (p *asyncKafkaProducer) PendingCount() int {
	return int(p.pending.Load())
}

// Stats returns current producer statistics
func (p *asyncKafkaProducer) Stats() producerStats {
	p.statsLock.RLock()
	defer p.statsLock.RUnlock()
	return p.stats
}

// updateStats safely updates statistics
func (p *asyncKafkaProducer) updateStats(update func(*producerStats)) {
	p.statsLock.Lock()
	defer p.statsLock.Unlock()
	update(&p.stats)
}

// Close flushes pending messages and shuts down the producer
func (p *asyncKafkaProducer) Close() error {
	// Signal all workers to stop
	done := make(chan error, p.workers)
	for i := 0; i < p.workers; i++ {
		p.closeChan <- done
	}

	// Wait for all workers to finish
	for i := 0; i < p.workers; i++ {
		<-done
	}

	// Close the send channel
	close(p.sendChan)

	// Wait for all workers to exit
	p.wg.Wait()

	// Close the underlying sync producer
	return p.kafkaProducer.Close()
}
