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

//go:build kafkatest
// +build kafkatest

package fly

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAsyncProducerBasicSend tests basic async send with callback
func TestAsyncProducerBasicSend(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-basic-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         10,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           2,
		ChannelSize:       100,
		AsyncBatchSize:    5,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-basic-topic"

	// Track callback
	var callbackInvoked atomic.Bool
	var callbackErr error
	var callbackMsg Message

	callback := func(msg Message, partition int, offset int64, err error) {
		callbackInvoked.Store(true)
		callbackErr = err
		callbackMsg = msg
	}

	// Send message
	msg := Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	err := producer.SendAsync(ctx, topic, msg, callback)
	require.NoError(t, err)

	// Flush and wait for callback
	err = producer.Flush(ctx)
	require.NoError(t, err)

	// Verify callback was invoked
	assert.Eventually(t, func() bool {
		return callbackInvoked.Load()
	}, 5*time.Second, 10*time.Millisecond)

	assert.NoError(t, callbackErr)
	assert.Equal(t, msg.Key, callbackMsg.Key)
	assert.Equal(t, msg.Value, callbackMsg.Value)

	// Check stats
	stats := producer.Stats()
	assert.Greater(t, stats.MessagesSent, int64(0))
	assert.Greater(t, stats.CallbacksInvoked, int64(0))
	assert.Equal(t, int64(0), stats.MessagesFailed)
}

// TestAsyncProducerBatchSend tests sending multiple messages in batch
func TestAsyncProducerBatchSend(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-batch-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         20,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           2,
		ChannelSize:       100,
		AsyncBatchSize:    10,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-batch-topic"

	// Track callbacks
	var callbackCount atomic.Int32
	var errorCount atomic.Int32

	callback := func(msg Message, partition int, offset int64, err error) {
		callbackCount.Add(1)
		if err != nil {
			errorCount.Add(1)
		}
	}

	// Send batch of messages
	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	err := producer.SendAsyncBatch(ctx, topic, messages, callback)
	require.NoError(t, err)

	// Flush
	err = producer.Flush(ctx)
	require.NoError(t, err)

	// Wait for all callbacks
	assert.Eventually(t, func() bool {
		return callbackCount.Load() == int32(len(messages))
	}, 5*time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(0), errorCount.Load())

	// Check stats
	stats := producer.Stats()
	assert.GreaterOrEqual(t, stats.MessagesSent, int64(len(messages)))
	assert.GreaterOrEqual(t, stats.CallbacksInvoked, int64(len(messages)))
}

// TestAsyncProducerConcurrentSends tests concurrent message sending
func TestAsyncProducerConcurrentSends(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-concurrent-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         50,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           4,
		ChannelSize:       1000,
		AsyncBatchSize:    25,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-concurrent-topic"

	// Track callbacks
	var totalCallbacks atomic.Int64
	var totalErrors atomic.Int64
	callbackMessages := sync.Map{}

	// Send from multiple goroutines
	numGoroutines := 10
	messagesPerGoroutine := 20

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < messagesPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, i)
				value := fmt.Sprintf("value-%d-%d", goroutineID, i)

				msg := Message{
					Key:   []byte(key),
					Value: []byte(value),
				}

				callback := func(m Message, partition int, offset int64, err error) {
					totalCallbacks.Add(1)
					if err != nil {
						totalErrors.Add(1)
					} else {
						callbackMessages.Store(string(m.Key), string(m.Value))
					}
				}

				err := producer.SendAsync(ctx, topic, msg, callback)
				if err != nil {
					t.Errorf("Failed to send message: %v", err)
				}
			}
		}(g)
	}

	// Wait for all sends to be queued
	wg.Wait()

	// Flush all messages
	err := producer.Flush(ctx)
	require.NoError(t, err)

	// Wait for all callbacks
	expectedCallbacks := int64(numGoroutines * messagesPerGoroutine)
	assert.Eventually(t, func() bool {
		return totalCallbacks.Load() == expectedCallbacks
	}, 10*time.Second, 50*time.Millisecond)

	assert.Equal(t, int64(0), totalErrors.Load())

	// Verify all messages were sent
	messageCount := 0
	callbackMessages.Range(func(key, value interface{}) bool {
		messageCount++
		return true
	})
	assert.Equal(t, numGoroutines*messagesPerGoroutine, messageCount)

	// Check stats
	stats := producer.Stats()
	assert.Equal(t, expectedCallbacks, stats.MessagesSent)
	assert.Equal(t, expectedCallbacks, stats.CallbacksInvoked)
}

// TestAsyncProducerFlush tests flush functionality
func TestAsyncProducerFlush(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-flush-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         100,
			BatchTimeout:      1 * time.Second, // Long timeout
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           2,
		ChannelSize:       100,
		AsyncBatchSize:    50,
		AsyncBatchTimeout: 1 * time.Second, // Long timeout
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-flush-topic"

	// Track callback
	var callbackInvoked atomic.Bool

	callback := func(msg Message, partition int, offset int64, err error) {
		callbackInvoked.Store(true)
	}

	// Send message
	msg := Message{
		Key:   []byte("flush-test-key"),
		Value: []byte("flush-test-value"),
	}

	err := producer.SendAsync(ctx, topic, msg, callback)
	require.NoError(t, err)

	// Message should not be sent immediately due to long batch timeout
	time.Sleep(100 * time.Millisecond)
	assert.False(t, callbackInvoked.Load())

	// Flush should force the message to be sent
	err = producer.Flush(ctx)
	require.NoError(t, err)

	// Callback should now be invoked
	assert.Eventually(t, func() bool {
		return callbackInvoked.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

// TestAsyncProducerPendingCount tests pending message tracking
func TestAsyncProducerPendingCount(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-pending-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         100,
			BatchTimeout:      5 * time.Second, // Very long timeout
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           1,
		ChannelSize:       1000,
		AsyncBatchSize:    100,
		AsyncBatchTimeout: 5 * time.Second, // Very long timeout
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-pending-topic"

	// Initially no pending messages
	assert.Equal(t, 0, producer.PendingCount())

	// Send multiple messages without flushing
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		err := producer.SendAsync(ctx, topic, msg, nil)
		require.NoError(t, err)
	}

	// Should have pending messages
	assert.Eventually(t, func() bool {
		return producer.PendingCount() == numMessages
	}, 1*time.Second, 10*time.Millisecond)

	// Flush all messages
	err := producer.Flush(ctx)
	require.NoError(t, err)

	// Should have no pending messages after flush
	assert.Eventually(t, func() bool {
		return producer.PendingCount() == 0
	}, 5*time.Second, 10*time.Millisecond)
}

// TestAsyncProducerMultipleTopics tests sending to multiple topics
func TestAsyncProducerMultipleTopics(t *testing.T) {
	topics := []string{"async-topic1", "async-topic2", "async-topic3"}
	k := NewKafkaTestContainer(t, topics...)

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         20,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           3,
		ChannelSize:       100,
		AsyncBatchSize:    10,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Track callbacks per topic
	topicCallbacks := make(map[string]*atomic.Int32)
	for _, topic := range topics {
		topicCallbacks[topic] = &atomic.Int32{}
	}

	// Send messages to different topics
	messagesPerTopic := 5
	for _, topic := range topics {
		for i := 0; i < messagesPerTopic; i++ {
			msg := Message{
				Key:   []byte(fmt.Sprintf("%s-key-%d", topic, i)),
				Value: []byte(fmt.Sprintf("%s-value-%d", topic, i)),
			}

			topicName := topic // Capture for closure
			callback := func(m Message, partition int, offset int64, err error) {
				if err == nil {
					topicCallbacks[topicName].Add(1)
				}
			}

			err := producer.SendAsync(ctx, topic, msg, callback)
			require.NoError(t, err)
		}
	}

	// Flush all
	err := producer.Flush(ctx)
	require.NoError(t, err)

	// Verify all topics received their messages
	for topic, counter := range topicCallbacks {
		assert.Eventually(t, func() bool {
			return counter.Load() == int32(messagesPerTopic)
		}, 5*time.Second, 10*time.Millisecond, "Topic %s should receive %d messages", topic, messagesPerTopic)
	}
}

// TestAsyncProducerContextCancellation tests context cancellation
func TestAsyncProducerContextCancellation(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-cancel-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         10,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           1,
		ChannelSize:       1, // Small channel to ensure it fills up
		AsyncBatchSize:    5,
		AsyncBatchTimeout: 10 * time.Second, // Long timeout to prevent auto-flush
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	topic := "async-cancel-topic"

	// Fill the channel first
	msg := Message{
		Key:   []byte("filler"),
		Value: []byte("filler"),
	}
	err := producer.SendAsync(context.Background(), topic, msg, nil)
	require.NoError(t, err)

	// Now create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	// Try to send with cancelled context - should fail with context error
	msg2 := Message{
		Key:   []byte("cancelled-key"),
		Value: []byte("cancelled-value"),
	}

	err = producer.SendAsync(ctx, topic, msg2, nil)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestAsyncProducerNilCallback tests sending without callback
func TestAsyncProducerNilCallback(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-nil-callback-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         10,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           2,
		ChannelSize:       100,
		AsyncBatchSize:    5,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-nil-callback-topic"

	// Send message with nil callback
	msg := Message{
		Key:   []byte("no-callback-key"),
		Value: []byte("no-callback-value"),
	}

	err := producer.SendAsync(ctx, topic, msg, nil)
	require.NoError(t, err)

	// Flush
	err = producer.Flush(ctx)
	require.NoError(t, err)

	// Verify message was sent via stats
	stats := producer.Stats()
	assert.Greater(t, stats.MessagesSent, int64(0))
	assert.Equal(t, int64(0), stats.CallbacksInvoked) // No callback to invoke
}

// TestAsyncProducerClose tests graceful shutdown
func TestAsyncProducerClose(t *testing.T) {
	k := NewKafkaTestContainer(t, "async-close-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         10,
			BatchTimeout:      100 * time.Millisecond,
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           2,
		ChannelSize:       100,
		AsyncBatchSize:    5,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)

	ctx := context.Background()
	topic := "async-close-topic"

	// Track callbacks
	var callbackCount atomic.Int32
	callback := func(msg Message, partition int, offset int64, err error) {
		callbackCount.Add(1)
	}

	// Send some messages
	for i := 0; i < 5; i++ {
		msg := Message{
			Key:   []byte(fmt.Sprintf("close-key-%d", i)),
			Value: []byte(fmt.Sprintf("close-value-%d", i)),
		}
		err := producer.SendAsync(ctx, topic, msg, callback)
		require.NoError(t, err)
	}

	// Close should flush pending messages
	err := producer.Close()
	require.NoError(t, err)

	// All callbacks should have been invoked
	assert.Eventually(t, func() bool {
		return callbackCount.Load() == 5
	}, 5*time.Second, 10*time.Millisecond)
}

// TestAsyncProducerWriteTimeout tests write timeout behavior
func TestAsyncProducerWriteTimeout(t *testing.T) {
	// This test validates that WriteMessages respects timeouts
	// Note: In the current implementation, we use context.Background() which has no timeout
	// The kafka.Writer may have internal timeouts based on BatchTimeout

	k := NewKafkaTestContainer(t, "async-timeout-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         10,
			BatchTimeout:      100 * time.Millisecond, // This affects internal Writer timeout
			RequiredAcks:      1,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           1,
		ChannelSize:       10,
		AsyncBatchSize:    5,
		AsyncBatchTimeout: 50 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-timeout-topic"

	// Send a message
	msg := Message{
		Key:   []byte("timeout-test"),
		Value: []byte("timeout-value"),
	}

	var callbackInvoked atomic.Bool
	var sendErr error
	callback := func(m Message, partition int, offset int64, err error) {
		sendErr = err
		callbackInvoked.Store(true)
	}

	err := producer.SendAsync(ctx, topic, msg, callback)
	require.NoError(t, err)

	// Flush and verify no timeout error
	err = producer.Flush(ctx)
	require.NoError(t, err)

	// Wait for callback to be invoked
	assert.Eventually(t, func() bool {
		return callbackInvoked.Load()
	}, 2*time.Second, 10*time.Millisecond, "Callback should be invoked")

	assert.NoError(t, sendErr, "WriteMessages should not timeout with valid broker")

	// Note: To properly test timeout, we'd need to simulate a non-responsive broker
	// which is beyond the scope of unit tests
}
