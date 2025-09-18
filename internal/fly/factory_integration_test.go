//go:build kafkatest
// +build kafkatest

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
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
)

func TestFactory_CreateConsumer(t *testing.T) {
	tests := []struct {
		name    string
		config  *config.KafkaConfig
		topic   string
		groupID string
		wantErr bool
	}{
		{
			name: "basic consumer",
			config: &config.KafkaConfig{
				Brokers:           []string{"localhost:9092"},
				ConsumerBatchSize: 100,
				ConsumerMaxWait:   500000000, // 500ms
				ConsumerMinBytes:  1024,
				ConsumerMaxBytes:  1048576,
				ConnectionTimeout: 50 * time.Millisecond, // Very fast timeout for tests
			},
			topic:   "test-topic",
			groupID: "test-group",
			wantErr: false,
		},
		{
			name: "consumer with SASL",
			config: &config.KafkaConfig{
				Brokers:           []string{"localhost:9092"},
				SASLEnabled:       true,
				SASLMechanism:     "SCRAM-SHA-256",
				SASLUsername:      "user",
				SASLPassword:      "pass",
				ConnectionTimeout: 50 * time.Millisecond, // Very fast timeout for tests
			},
			topic:   "test-topic",
			groupID: "test-group",
			wantErr: false,
		},
		{
			name: "consumer with invalid SASL",
			config: &config.KafkaConfig{
				Brokers:           []string{"localhost:9092"},
				SASLEnabled:       true,
				SASLMechanism:     "INVALID",
				ConnectionTimeout: 50 * time.Millisecond, // Very fast timeout for tests
			},
			topic:   "test-topic",
			groupID: "test-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory(tt.config)
			consumer, err := factory.CreateConsumer(tt.topic, tt.groupID)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
				if consumer != nil {
					_ = consumer.Close()
				}
			}
		})
	}
}

func TestFactory_CreateConsumerWithService(t *testing.T) {
	config := &config.KafkaConfig{
		Brokers:             []string{"localhost:9092"},
		ConsumerGroupPrefix: "lakerunner",
		ConnectionTimeout:   100 * time.Millisecond, // Fast timeout for tests
	}

	factory := NewFactory(config)

	consumer, err := factory.CreateConsumerWithService("test-topic", "ingest")
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// Verify the group ID was constructed correctly
	kc, ok := consumer.(*kafkaConsumer)
	require.True(t, ok)
	assert.Equal(t, "lakerunner.ingest", kc.config.GroupID)

	_ = consumer.Close()
}

func TestManager_Lifecycle(t *testing.T) {
	config := &config.KafkaConfig{
		Brokers:           []string{"localhost:9092"},
		ConnectionTimeout: 100 * time.Millisecond, // Fast timeout for tests
	}
	factory := NewFactory(config)
	manager := NewManager(factory)

	// Create producer
	producer, err := manager.CreateProducer()
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	assert.Len(t, manager.producers, 1)

	// Create consumer
	consumer, err := manager.CreateConsumer("topic1", "group1")
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Len(t, manager.consumers, 1)

	// Create consumer with service
	consumer2, err := manager.CreateConsumerWithService("topic2", "ingest")
	assert.NoError(t, err)
	assert.NotNil(t, consumer2)
	assert.Len(t, manager.consumers, 2)

	// Close all
	err = manager.Close()
	assert.NoError(t, err)
}

func TestManager_CloseWithErrors(t *testing.T) {
	config := &config.KafkaConfig{
		Brokers:           []string{"localhost:9092"},
		ConnectionTimeout: 100 * time.Millisecond, // Fast timeout for tests
	}
	factory := NewFactory(config)
	manager := NewManager(factory)

	// Create multiple producers and consumers
	p1, _ := manager.CreateProducer()
	_, _ = manager.CreateProducer() // p2
	c1, _ := manager.CreateConsumer("topic1", "group1")
	_, _ = manager.CreateConsumer("topic2", "group2") // c2

	// Close some manually to simulate already closed
	_ = p1.Close()
	_ = c1.Close()

	// Manager close should still work
	err := manager.Close()
	// Error may or may not occur depending on implementation
	// The important thing is it doesn't panic
	if err != nil {
		t.Logf("Close returned error (expected): %v", err)
	}
}

// TestFactoryIntegration requires a running Kafka broker
// Run with: go test -tags kafkatest
func TestFactoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &config.KafkaConfig{
		Brokers:              []string{"localhost:9092"},
		ProducerBatchSize:    10,
		ProducerBatchTimeout: 100000000, // 100ms
		ConsumerBatchSize:    10,
		ConsumerMaxWait:      100000000, // 100ms
		ConsumerGroupPrefix:  "test",
		ConnectionTimeout:    50 * time.Millisecond, // Very fast timeout for tests
	}

	factory := NewFactory(config)

	// Test producer creation
	producer, err := factory.CreateProducer()
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify producer config was applied
	kp, ok := producer.(*kafkaProducer)
	require.True(t, ok)
	assert.Equal(t, 10, kp.config.BatchSize)
	assert.Equal(t, kafka.RequireNone, kp.config.RequiredAcks)

	_ = producer.Close()

	// Test consumer creation
	consumer, err := factory.CreateConsumer("test-topic", "test-group")
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify consumer config was applied
	kc, ok := consumer.(*kafkaConsumer)
	require.True(t, ok)
	assert.Equal(t, 10, kc.config.BatchSize)
	assert.Equal(t, "test-topic", kc.config.Topic)
	assert.Equal(t, "test-group", kc.config.GroupID)

	_ = consumer.Close()

	// Test consumer with service
	consumer2, err := factory.CreateConsumerWithService("test-topic2", "ingest")
	require.NoError(t, err)
	require.NotNil(t, consumer2)

	kc2, ok := consumer2.(*kafkaConsumer)
	require.True(t, ok)
	assert.Equal(t, "test.ingest", kc2.config.GroupID)

	_ = consumer2.Close()
}
