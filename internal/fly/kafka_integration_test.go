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

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaConnectivity(t *testing.T) {
	// Simple test to verify we can connect to Kafka
	conn, err := kafka.Dial("tcp", "localhost:9092")
	require.NoError(t, err, "Failed to connect to Kafka at localhost:9092")
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
	topic := "test-simple-integration"

	// Create topic first
	conn, err := kafka.Dial("tcp", "localhost:9092")
	require.NoError(t, err)

	controller, err := conn.Controller()
	require.NoError(t, err)
	conn.Close()

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err)

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Logf("Topic creation error (might already exist): %v", err)
	}
	controllerConn.Close()

	// Create a simple writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// Write a message
	ctx := context.Background()
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	})
	require.NoError(t, err, "Failed to write message")

	// Create a simple reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		GroupID:  "test-group",
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
