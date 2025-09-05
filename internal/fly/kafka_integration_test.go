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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaConnectivity(t *testing.T) {
	// Use shared Kafka container
	kafkaContainer := NewKafkaTestContainer(t, "connectivity-test")
	defer kafkaContainer.CleanupAfterTest(t, []string{"connectivity-test"}, []string{})

	// Simple test to verify we can connect to Kafka
	conn, err := kafka.Dial("tcp", kafkaContainer.Broker())
	require.NoError(t, err, "Failed to connect to Kafka container")
	defer conn.Close()

	// Get broker info
	broker := conn.Broker()
	t.Logf("Connected to Kafka broker: ID=%d, Host=%s, Port=%d", broker.ID, broker.Host, broker.Port)

	// List existing topics
	partitions, err := conn.ReadPartitions()
	if err == nil {
		topics := make(map[string]bool)
		for _, p := range partitions {
			topics[p.Topic] = true
		}
		t.Logf("Existing topics: %v", topics)
	}
}

func TestSimpleProducerConsumer(t *testing.T) {
	topic := fmt.Sprintf("test-simple-integration-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-integration-%d", time.Now().UnixNano())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Create a simple writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaContainer.Broker()),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// Write a message
	ctx := context.Background()
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	})
	require.NoError(t, err, "Failed to write message")

	// Create a simple reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaContainer.Broker()},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer reader.Close()

	// Read the message
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := reader.FetchMessage(readCtx)
	require.NoError(t, err, "Failed to read message")

	assert.Equal(t, []byte("test-key"), msg.Key)
	assert.Equal(t, []byte("test-value"), msg.Value)

	// Commit the message
	err = reader.CommitMessages(ctx, msg)
	assert.NoError(t, err)
}
