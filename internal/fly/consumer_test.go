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

//go:build testkafka

package fly

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func produceTestMessages(t *testing.T, topic string, messages []kafka.Message) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(testBroker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    1,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	err := w.WriteMessages(context.Background(), messages...)
	require.NoError(t, err)
}

func TestConsumer_Consume(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-group-%s", uuid.New().String())
	createTestTopic(t, topic, 1)
	defer deleteTestTopic(t, topic)

	// Produce test messages
	testMessages := []kafka.Message{
		{
			Key:   []byte("key1"),
			Value: []byte("value1"),
			Headers: []kafka.Header{
				{Key: "h1", Value: []byte("v1")},
			},
		},
		{
			Key:   []byte("key2"),
			Value: []byte("value2"),
		},
		{
			Key:   []byte("key3"),
			Value: []byte("value3"),
		},
	}
	produceTestMessages(t, topic, testMessages)

	config := ConsumerConfig{
		Brokers:       []string{testBroker},
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6,
		MaxWait:       100 * time.Millisecond,
		BatchSize:     2,
		StartOffset:   kafka.FirstOffset,
		AutoCommit:    false,
		CommitBatch:   true,
		RetryAttempts: 3,
	}

	consumer := NewConsumer(config)
	defer consumer.Close()

	receivedMessages := make([]ConsumedMessage, 0)
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, messages...)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start consuming in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed
	time.Sleep(2 * time.Second)
	cancel()

	// Check if context cancellation error is returned
	err := <-errCh
	assert.ErrorIs(t, err, context.Canceled)

	// Verify messages were consumed
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 3, len(receivedMessages))

	// Check message contents
	messageMap := make(map[string]string)
	for _, msg := range receivedMessages {
		messageMap[string(msg.Key)] = string(msg.Value)
	}
	assert.Equal(t, "value1", messageMap["key1"])
	assert.Equal(t, "value2", messageMap["key2"])
	assert.Equal(t, "value3", messageMap["key3"])
}

func TestConsumer_BatchProcessing(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-batch-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-batch-group-%s", uuid.New().String())
	createTestTopic(t, topic, 1)
	defer deleteTestTopic(t, topic)

	// Produce test messages
	var testMessages []kafka.Message
	for i := 0; i < 5; i++ {
		testMessages = append(testMessages, kafka.Message{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value%d", i)),
		})
	}
	produceTestMessages(t, topic, testMessages)

	config := ConsumerConfig{
		Brokers:       []string{testBroker},
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6,
		MaxWait:       200 * time.Millisecond,
		BatchSize:     3, // Process in batches of 3
		StartOffset:   kafka.FirstOffset,
		AutoCommit:    false,
		CommitBatch:   true,
		RetryAttempts: 3,
	}

	consumer := NewConsumer(config)
	defer consumer.Close()

	batchSizes := make([]int, 0)
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		batchSizes = append(batchSizes, len(messages))
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start consuming in background
	go func() {
		_ = consumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed
	time.Sleep(2 * time.Second)
	cancel()

	// Verify batch sizes
	mu.Lock()
	defer mu.Unlock()

	// Should have received batches of size 3, 2 (or 3, 3 if timeout triggered)
	assert.GreaterOrEqual(t, len(batchSizes), 2)
	if len(batchSizes) >= 2 {
		assert.LessOrEqual(t, batchSizes[0], 3)
		assert.LessOrEqual(t, batchSizes[1], 3)
	}
}

func TestConsumer_ErrorHandlingAndRetry(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-error-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-error-group-%s", uuid.New().String())
	createTestTopic(t, topic, 1)
	defer deleteTestTopic(t, topic)

	// Produce test message
	produceTestMessages(t, topic, []kafka.Message{
		{Key: []byte("key1"), Value: []byte("value1")},
	})

	config := ConsumerConfig{
		Brokers:       []string{testBroker},
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6,
		MaxWait:       100 * time.Millisecond,
		BatchSize:     1,
		StartOffset:   kafka.FirstOffset,
		AutoCommit:    false,
		CommitBatch:   false,
		RetryAttempts: 3,
	}

	consumer := NewConsumer(config)
	defer consumer.Close()

	attemptCount := 0
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		attemptCount++
		count := attemptCount
		mu.Unlock()

		if count < 3 {
			return fmt.Errorf("simulated error attempt %d", count)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consuming in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, handler)
	}()

	// Wait for retries to complete
	time.Sleep(5 * time.Second)
	cancel()

	<-errCh

	// Verify retry attempts
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 3, attemptCount) // Should retry and succeed on third attempt
}

func TestConsumer_CommitMessages(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-commit-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-commit-group-%s", uuid.New().String())
	createTestTopic(t, topic, 1)
	defer deleteTestTopic(t, topic)

	// Produce test messages
	testMessages := []kafka.Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	produceTestMessages(t, topic, testMessages)

	config := ConsumerConfig{
		Brokers:       []string{testBroker},
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6,
		MaxWait:       100 * time.Millisecond,
		BatchSize:     10,
		StartOffset:   kafka.FirstOffset,
		AutoCommit:    false,
		CommitBatch:   false, // Test individual commits
		RetryAttempts: 1,
	}

	consumer := NewConsumer(config)
	defer consumer.Close()

	receivedMessages := make([]ConsumedMessage, 0)
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, messages...)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start consuming in background
	go func() {
		_ = consumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed
	time.Sleep(1 * time.Second)

	// Manually commit messages
	mu.Lock()
	if len(receivedMessages) > 0 {
		err := consumer.CommitMessages(context.Background(), receivedMessages...)
		assert.NoError(t, err)
	}
	mu.Unlock()

	cancel()
}

func TestConsumerOptions(t *testing.T) {
	config := DefaultConsumerConfig("", "")

	WithTopic("test-topic")(&config)
	assert.Equal(t, "test-topic", config.Topic)

	WithConsumerGroup("test-group")(&config)
	assert.Equal(t, "test-group", config.GroupID)

	WithBatchSize(50)(&config)
	assert.Equal(t, 50, config.BatchSize)

	WithBrokers("broker1:9092", "broker2:9092")(&config)
	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, config.Brokers)
}

func TestNewConsumerWithOptions(t *testing.T) {
	consumer := NewConsumerWithOptions(
		WithTopic("test-topic"),
		WithConsumerGroup("test-group"),
		WithBatchSize(25),
		WithBrokers("localhost:9092"),
	)
	defer consumer.Close()

	// Type assert to access internal config
	kc, ok := consumer.(*kafkaConsumer)
	require.True(t, ok)

	assert.Equal(t, 25, kc.config.BatchSize)
	assert.Equal(t, []string{"localhost:9092"}, kc.config.Brokers)
}
