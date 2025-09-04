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

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFactory(t *testing.T) {
	config := &Config{
		Brokers: []string{"broker1:9092", "broker2:9092"},
		Enabled: true,
	}

	factory := NewFactory(config)
	assert.NotNil(t, factory)
	assert.Equal(t, config, factory.GetConfig())
}

func TestFactory_CreateProducer(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "basic producer",
			config: &Config{
				Brokers:              []string{"localhost:9092"},
				ProducerBatchSize:    100,
				ProducerBatchTimeout: 1000000000, // 1 second in nanoseconds
				ProducerCompression:  "snappy",
				SASLEnabled:          false,
				TLSEnabled:           false,
			},
			wantErr: false,
		},
		{
			name: "producer with SASL SCRAM-SHA-256",
			config: &Config{
				Brokers:              []string{"localhost:9092"},
				ProducerBatchSize:    50,
				ProducerBatchTimeout: 500000000, // 500ms
				SASLEnabled:          true,
				SASLMechanism:        "SCRAM-SHA-256",
				SASLUsername:         "user",
				SASLPassword:         "pass",
			},
			wantErr: false,
		},
		{
			name: "producer with SASL SCRAM-SHA-512",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "SCRAM-SHA-512",
				SASLUsername:  "user",
				SASLPassword:  "pass",
			},
			wantErr: false,
		},
		{
			name: "producer with TLS",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				TLSEnabled:    true,
				TLSSkipVerify: true,
			},
			wantErr: false,
		},
		{
			name: "producer with unsupported SASL mechanism",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "PLAIN", // Not supported
				SASLUsername:  "user",
				SASLPassword:  "pass",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory(tt.config)
			producer, err := factory.CreateProducer()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, producer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, producer)
				if producer != nil {
					producer.Close()
				}
			}
		})
	}
}

func TestFactory_CreateConsumer(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		topic   string
		groupID string
		wantErr bool
	}{
		{
			name: "basic consumer",
			config: &Config{
				Brokers:           []string{"localhost:9092"},
				ConsumerBatchSize: 100,
				ConsumerMaxWait:   500000000, // 500ms
				ConsumerMinBytes:  1024,
				ConsumerMaxBytes:  1048576,
			},
			topic:   "test-topic",
			groupID: "test-group",
			wantErr: false,
		},
		{
			name: "consumer with SASL",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "SCRAM-SHA-256",
				SASLUsername:  "user",
				SASLPassword:  "pass",
			},
			topic:   "test-topic",
			groupID: "test-group",
			wantErr: false,
		},
		{
			name: "consumer with invalid SASL",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "INVALID",
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
					consumer.Close()
				}
			}
		})
	}
}

func TestFactory_CreateConsumerWithService(t *testing.T) {
	config := &Config{
		Brokers:             []string{"localhost:9092"},
		ConsumerGroupPrefix: "lakerunner",
	}

	factory := NewFactory(config)

	consumer, err := factory.CreateConsumerWithService("test-topic", "ingest")
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// Verify the group ID was constructed correctly
	kc, ok := consumer.(*kafkaConsumer)
	require.True(t, ok)
	assert.Equal(t, "lakerunner.ingest", kc.config.GroupID)

	consumer.Close()
}

func TestFactory_IsEnabled(t *testing.T) {
	factory := NewFactory(&Config{Enabled: true})
	assert.True(t, factory.IsEnabled())
}

func TestManager_Lifecycle(t *testing.T) {
	config := &Config{
		Brokers: []string{"localhost:9092"},
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
	config := &Config{
		Brokers: []string{"localhost:9092"},
	}
	factory := NewFactory(config)
	manager := NewManager(factory)

	// Create multiple producers and consumers
	p1, _ := manager.CreateProducer()
	_, _ = manager.CreateProducer() // p2
	c1, _ := manager.CreateConsumer("topic1", "group1")
	_, _ = manager.CreateConsumer("topic2", "group2") // c2

	// Close some manually to simulate already closed
	p1.Close()
	c1.Close()

	// Manager close should still work
	err := manager.Close()
	// Error may or may not occur depending on implementation
	// The important thing is it doesn't panic
	if err != nil {
		t.Logf("Close returned error (expected): %v", err)
	}
}

func TestFactory_SASLMechanismCreation(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		wantErr   bool
	}{
		{"SCRAM-SHA-256", "SCRAM-SHA-256", false},
		{"SCRAM-SHA-512", "SCRAM-SHA-512", false},
		{"unsupported", "PLAIN", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &Factory{
				config: &Config{
					SASLMechanism: tt.mechanism,
					SASLUsername:  "user",
					SASLPassword:  "pass",
				},
			}

			mechanism, err := factory.createSASLMechanism()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, mechanism)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, mechanism)
			}
		})
	}
}

// TestFactoryIntegration requires a running Kafka broker
// Run with: go test -tags testkafka
func TestFactoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &Config{
		Brokers:              []string{"localhost:9092"},
		Enabled:              true,
		ProducerBatchSize:    10,
		ProducerBatchTimeout: 100000000, // 100ms
		ConsumerBatchSize:    10,
		ConsumerMaxWait:      100000000, // 100ms
		ConsumerGroupPrefix:  "test",
	}

	factory := NewFactory(config)
	assert.True(t, factory.IsEnabled())

	// Test producer creation
	producer, err := factory.CreateProducer()
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify producer config was applied
	kp, ok := producer.(*kafkaProducer)
	require.True(t, ok)
	assert.Equal(t, 10, kp.config.BatchSize)
	assert.Equal(t, kafka.RequireOne, kp.config.RequiredAcks)

	producer.Close()

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

	consumer.Close()

	// Test consumer with service
	consumer2, err := factory.CreateConsumerWithService("test-topic2", "ingest")
	require.NoError(t, err)
	require.NotNil(t, consumer2)

	kc2, ok := consumer2.(*kafkaConsumer)
	require.True(t, ok)
	assert.Equal(t, "test.ingest", kc2.config.GroupID)

	consumer2.Close()
}
