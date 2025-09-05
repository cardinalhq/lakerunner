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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func produceTestMessagesWithContainer(t *testing.T, kafkaContainer *KafkaTestContainer, topic string, messages []Message) {
	config := kafkaContainer.CreateProducerConfig()
	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()
	for _, msg := range messages {
		err := producer.Send(ctx, topic, msg)
		require.NoError(t, err)
	}
}

func TestConsumer_Consume(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-consumer-group-%d", time.Now().UnixNano())

	// Start Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.Stop(t)

	// Produce test messages
	testMessages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	// Create consumer config
	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.BatchSize = 2 // Test batching

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consuming in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed
	time.Sleep(3 * time.Second)
	cancel()

	// Check if context cancellation error is returned
	consumerErr := <-errCh
	assert.ErrorIs(t, consumerErr, context.Canceled)

	// Verify messages were consumed
	mu.Lock()
	defer mu.Unlock()

	t.Logf("Received %d messages from consumer", len(receivedMessages))
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
	
	// Start Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.Stop(t)

	// Produce test messages
	var testMessages []Message
	for i := 0; i < 5; i++ {
		testMessages = append(testMessages, Message{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value%d", i)),
		})
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.BatchSize = 3 // Process in batches of 3
	config.MaxWait = 200 * time.Millisecond

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
	
	// Start Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.Stop(t)

	// Produce test message
	testMessages := []Message{{Key: []byte("key1"), Value: []byte("value1")}}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.BatchSize = 1
	config.CommitBatch = false
	config.RetryAttempts = 3

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
	
	// Start Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.Stop(t)

	// Produce test messages
	testMessages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.CommitBatch = false // Test individual commits
	config.BatchSize = 10
	config.RetryAttempts = 1

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
