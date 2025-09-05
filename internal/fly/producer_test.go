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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_Send(t *testing.T) {
	topic := fmt.Sprintf("test-producer-send-%s", uuid.New().String())
	
	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{"test-reader"})

	config := kafkaContainer.CreateProducerConfig()

	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Test sending a message
	msg := Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
		Headers: map[string]string{
			"header1": "value1",
		},
	}

	err := producer.Send(ctx, topic, msg)
	assert.NoError(t, err)

	// Verify message was sent by reading it
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaContainer.Broker()},
		Topic:    topic,
		GroupID:  "test-reader",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	readMsg, err := reader.FetchMessage(readCtx)
	require.NoError(t, err)
	assert.Equal(t, msg.Key, readMsg.Key)
	assert.Equal(t, msg.Value, readMsg.Value)

	err = reader.CommitMessages(ctx, readMsg)
	assert.NoError(t, err)
}

func TestProducer_BatchSend(t *testing.T) {
	topic := fmt.Sprintf("test-producer-batch-%s", uuid.New().String())
	
	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{"test-batch-reader"})

	config := kafkaContainer.CreateProducerConfig()

	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Create multiple messages
	messages := []Message{
		{
			Key:   []byte("key1"),
			Value: []byte("value1"),
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

	err := producer.BatchSend(ctx, topic, messages)
	assert.NoError(t, err)

	// Verify messages were sent
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaContainer.Broker()},
		Topic:    topic,
		GroupID:  "test-batch-reader",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	readMessages := make(map[string]string)
	for i := 0; i < len(messages); i++ {
		msg, err := reader.FetchMessage(readCtx)
		require.NoError(t, err)
		readMessages[string(msg.Key)] = string(msg.Value)
		err = reader.CommitMessages(ctx, msg)
		assert.NoError(t, err)
	}

	assert.Equal(t, "value1", readMessages["key1"])
	assert.Equal(t, "value2", readMessages["key2"])
	assert.Equal(t, "value3", readMessages["key3"])
}

func TestProducer_SendToPartition(t *testing.T) {
	topic := fmt.Sprintf("test-producer-partition-%s", uuid.New().String())
	
	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{})

	config := kafkaContainer.CreateProducerConfig()

	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Send message to partition 0 (only partition with 1-partition setup)
	msg := Message{
		Key:   []byte("key-partition-0"),
		Value: []byte("value-partition-0"),
	}
	err := producer.SendToPartition(ctx, topic, 0, msg)
	assert.NoError(t, err)

	// Verify message went to partition 0
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaContainer.Broker()},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   1 * time.Second,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	readMsg, err := reader.FetchMessage(readCtx)
	require.NoError(t, err)
	assert.Equal(t, 0, readMsg.Partition)
	assert.Equal(t, "key-partition-0", string(readMsg.Key))
	assert.Equal(t, "value-partition-0", string(readMsg.Value))
}

func TestProducer_GetPartitionCount(t *testing.T) {
	topic := fmt.Sprintf("test-partition-count-%s", uuid.New().String())
	
	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{})

	config := kafkaContainer.CreateProducerConfig()

	producer := NewProducer(config)
	defer producer.Close()

	count, err := producer.GetPartitionCount(topic)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Test caching - should return the same value
	count2, err := producer.GetPartitionCount(topic)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
}

func TestProducer_EmptyBatch(t *testing.T) {
	topic := fmt.Sprintf("test-empty-batch-%s", uuid.New().String())
	
	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{})

	config := kafkaContainer.CreateProducerConfig()

	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Send empty batch should not error
	err := producer.BatchSend(ctx, topic, []Message{})
	assert.NoError(t, err)
}

func TestManualBalancer(t *testing.T) {
	balancer := &manualBalancer{partition: 2}

	tests := []struct {
		name       string
		partitions []int
		expected   int
	}{
		{
			name:       "target partition exists",
			partitions: []int{0, 1, 2, 3},
			expected:   2,
		},
		{
			name:       "target partition doesn't exist",
			partitions: []int{0, 1, 3},
			expected:   0, // Falls back to first
		},
		{
			name:       "no partitions available",
			partitions: []int{},
			expected:   0,
		},
		{
			name:       "single partition that matches",
			partitions: []int{2},
			expected:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := balancer.Balance(kafka.Message{}, tt.partitions...)
			assert.Equal(t, tt.expected, result)
		})
	}
}
