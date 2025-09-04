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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBroker = "localhost:9092"
)

func createTestTopic(t *testing.T, topic string, partitions int) {
	conn, err := kafka.Dial("tcp", testBroker)
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err)
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil && err.Error() != "Topic with this name already exists" {
		require.NoError(t, err)
	}
}

func deleteTestTopic(t *testing.T, topic string) {
	conn, err := kafka.Dial("tcp", testBroker)
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err)
	defer controllerConn.Close()

	_ = controllerConn.DeleteTopics(topic)
}

func TestProducer_Send(t *testing.T) {
	topic := fmt.Sprintf("test-producer-send-%s", uuid.New().String())
	createTestTopic(t, topic, 3)
	defer deleteTestTopic(t, topic)

	config := ProducerConfig{
		Brokers:      []string{testBroker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

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
		Brokers:  []string{testBroker},
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
	createTestTopic(t, topic, 3)
	defer deleteTestTopic(t, topic)

	config := ProducerConfig{
		Brokers:      []string{testBroker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

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
		Brokers:  []string{testBroker},
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
	createTestTopic(t, topic, 3)
	defer deleteTestTopic(t, topic)

	config := ProducerConfig{
		Brokers:      []string{testBroker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	producer := NewProducer(config)
	defer producer.Close()

	ctx := context.Background()

	// Send messages to specific partitions
	for partition := 0; partition < 3; partition++ {
		msg := Message{
			Key:   []byte(fmt.Sprintf("key-partition-%d", partition)),
			Value: []byte(fmt.Sprintf("value-partition-%d", partition)),
		}
		err := producer.SendToPartition(ctx, topic, partition, msg)
		assert.NoError(t, err)
	}

	// Verify messages went to correct partitions
	for partition := 0; partition < 3; partition++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{testBroker},
			Topic:     topic,
			Partition: partition,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   1 * time.Second,
		})

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.FetchMessage(readCtx)
		cancel()

		require.NoError(t, err)
		assert.Equal(t, partition, msg.Partition)
		assert.Equal(t, fmt.Sprintf("key-partition-%d", partition), string(msg.Key))
		assert.Equal(t, fmt.Sprintf("value-partition-%d", partition), string(msg.Value))

		reader.Close()
	}
}

func TestProducer_GetPartitionCount(t *testing.T) {
	topic := fmt.Sprintf("test-partition-count-%s", uuid.New().String())
	createTestTopic(t, topic, 5)
	defer deleteTestTopic(t, topic)

	config := ProducerConfig{
		Brokers:      []string{testBroker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	producer := NewProducer(config)
	defer producer.Close()

	count, err := producer.GetPartitionCount(topic)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)

	// Test caching - should return the same value
	count2, err := producer.GetPartitionCount(topic)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
}

func TestProducer_EmptyBatch(t *testing.T) {
	topic := fmt.Sprintf("test-empty-batch-%s", uuid.New().String())
	createTestTopic(t, topic, 1)
	defer deleteTestTopic(t, topic)

	config := ProducerConfig{
		Brokers:      []string{testBroker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

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
