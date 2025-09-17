//go:build kafkatest

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

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

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

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

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

func TestConsumer_ErrorHandling(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-error-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-error-group-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Produce test message
	testMessages := []Message{{Key: []byte("key1"), Value: []byte("value1")}}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.BatchSize = 1
	config.CommitBatch = false

	consumer := NewConsumer(config)
	defer consumer.Close()

	attemptCount := 0
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		attemptCount++
		mu.Unlock()

		// Always return error to test error handling
		return fmt.Errorf("simulated error")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start consuming in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, handler)
	}()

	// Wait for error or timeout
	err := <-errCh

	// Should get an error (either from handler failure or context timeout)
	assert.Error(t, err)

	// Verify handler was called at least once
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, attemptCount, 1) // Should have tried at least once
}

func TestConsumer_CommitMessages(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-commit-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-commit-group-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Produce test messages
	testMessages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.CommitBatch = false // Test individual commits
	config.BatchSize = 10

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

func TestConsumer_NoAutoCommit(t *testing.T) {
	topic := fmt.Sprintf("test-no-autocommit-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-no-autocommit-group-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Produce test messages
	testMessages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	// First consumer - consume messages but DO NOT commit
	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.AutoCommit = false
	config.CommitBatch = false
	config.BatchSize = 10
	config.MaxWait = 500 * time.Millisecond

	firstConsumer := NewConsumer(config)

	firstConsumerMessages := make([]ConsumedMessage, 0)
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		firstConsumerMessages = append(firstConsumerMessages, messages...)
		mu.Unlock()
		// Intentionally NOT committing messages
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start consuming in background
	go func() {
		_ = firstConsumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed
	time.Sleep(2 * time.Second)
	cancel()

	// Close first consumer without committing
	firstConsumer.Close()

	// Verify we received messages
	mu.Lock()
	receivedCount := len(firstConsumerMessages)
	mu.Unlock()
	assert.Equal(t, 3, receivedCount, "First consumer should have received 3 messages")

	// Second consumer - should receive the SAME messages since they weren't committed
	secondConsumer := NewConsumer(config)
	defer secondConsumer.Close()

	secondConsumerMessages := make([]ConsumedMessage, 0)

	handler2 := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		secondConsumerMessages = append(secondConsumerMessages, messages...)
		mu.Unlock()
		return nil
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	// Start consuming in background
	go func() {
		_ = secondConsumer.Consume(ctx2, handler2)
	}()

	// Wait for messages to be consumed
	time.Sleep(2 * time.Second)
	cancel2()

	// Verify second consumer received the same messages
	mu.Lock()
	secondReceivedCount := len(secondConsumerMessages)
	mu.Unlock()

	assert.Equal(t, 3, secondReceivedCount,
		"Second consumer should receive all 3 messages since first consumer didn't commit")

	// Verify the message contents are the same
	messageMap1 := make(map[string]string)
	messageMap2 := make(map[string]string)

	mu.Lock()
	for _, msg := range firstConsumerMessages {
		messageMap1[string(msg.Key)] = string(msg.Value)
	}
	for _, msg := range secondConsumerMessages {
		messageMap2[string(msg.Key)] = string(msg.Value)
	}
	mu.Unlock()

	assert.Equal(t, messageMap1, messageMap2,
		"Both consumers should have received the same messages")
}

func TestConsumer_ExplicitCommitPersists(t *testing.T) {
	topic := fmt.Sprintf("test-explicit-commit-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-explicit-commit-group-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Produce test messages
	testMessages := []Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, testMessages)

	// First consumer - consume and COMMIT messages
	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	config.AutoCommit = false
	config.CommitBatch = false
	config.BatchSize = 10
	config.MaxWait = 500 * time.Millisecond

	firstConsumer := NewConsumer(config)

	firstConsumerMessages := make([]ConsumedMessage, 0)
	var mu sync.Mutex

	handler := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		firstConsumerMessages = append(firstConsumerMessages, messages...)
		mu.Unlock()

		// Explicitly commit each message
		for _, msg := range messages {
			if err := firstConsumer.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("failed to commit message: %w", err)
			}
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start consuming in background
	go func() {
		_ = firstConsumer.Consume(ctx, handler)
	}()

	// Wait for messages to be consumed and committed
	time.Sleep(2 * time.Second)
	cancel()
	firstConsumer.Close()

	// Verify we received and committed messages
	mu.Lock()
	receivedCount := len(firstConsumerMessages)
	mu.Unlock()
	assert.Equal(t, 3, receivedCount, "First consumer should have received 3 messages")

	// Produce more messages to the same topic
	newMessages := []Message{
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}
	produceTestMessagesWithContainer(t, kafkaContainer, topic, newMessages)

	// Second consumer - should only receive NEW messages since old ones were committed
	secondConsumer := NewConsumer(config)
	defer secondConsumer.Close()

	secondConsumerMessages := make([]ConsumedMessage, 0)

	handler2 := func(ctx context.Context, messages []ConsumedMessage) error {
		mu.Lock()
		secondConsumerMessages = append(secondConsumerMessages, messages...)
		mu.Unlock()
		return nil
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	// Start consuming in background
	go func() {
		_ = secondConsumer.Consume(ctx2, handler2)
	}()

	// Wait for messages to be consumed
	time.Sleep(2 * time.Second)
	cancel2()

	// Verify second consumer only received the new messages
	mu.Lock()
	secondReceivedCount := len(secondConsumerMessages)
	mu.Unlock()

	assert.Equal(t, 2, secondReceivedCount,
		"Second consumer should only receive 2 new messages since first consumer committed the first 3")

	// Verify the second consumer got only the new messages
	mu.Lock()
	for _, msg := range secondConsumerMessages {
		key := string(msg.Key)
		assert.Contains(t, []string{"key4", "key5"}, key,
			"Second consumer should only have received new messages (key4, key5)")
	}
	mu.Unlock()
}
