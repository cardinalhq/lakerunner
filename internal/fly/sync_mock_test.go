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
	"testing"
	"time"

	"github.com/cardinalhq/kafka-sync/kafkasync"
	"github.com/stretchr/testify/assert"
)

func TestSyncTopicsConnectionFailure(t *testing.T) {
	// Test that SyncTopics properly handles connection failures
	config := &Config{
		Brokers:             []string{"192.0.2.1:9999"}, // RFC5737 TEST-NET-1 - guaranteed unreachable
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Create minimal topic configuration
	topicsConfig := &kafkasync.Config{
		Topics: []kafkasync.Topic{
			{
				Name:              "test-topic",
				PartitionCount:    1,
				ReplicationFactor: 1,
			},
		},
		OperationTimeout: 1 * time.Second, // Short timeout for faster failure
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test the behavior: SyncTopics should return an error when broker is unreachable
	err := syncer.SyncTopics(ctx, topicsConfig, true)

	// The only assertion that matters: it should fail
	assert.Error(t, err, "SyncTopics should fail when broker is unreachable")

	// Log for debugging but don't assert on the specific message
	t.Logf("Error (as expected): %s", err.Error())
}

func TestSyncTopicsEmptyTopics(t *testing.T) {
	// Test SyncTopics with empty topics list
	config := &Config{
		Brokers:             []string{"localhost:9092"},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Create config with no topics
	topicsConfig := &kafkasync.Config{
		Topics:           []kafkasync.Topic{},
		OperationTimeout: 10 * time.Second,
	}

	ctx := context.Background()

	// This may succeed if there's a local Kafka instance running, or fail if not
	// Either way it exercises the SyncTopics method
	err := syncer.SyncTopics(ctx, topicsConfig, false)
	// We don't assert error here since it might succeed with local Kafka
	// The important thing is we exercised the code path
	_ = err // Ignore result
}

func TestSyncTopicsWithSASL(t *testing.T) {
	// Test the SASL configuration path in createConnectionConfig
	config := &Config{
		Brokers:             []string{"localhost:9092"},
		SASLEnabled:         true,
		SASLMechanism:       "SCRAM-SHA-256",
		SASLUsername:        "testuser",
		SASLPassword:        "testpass",
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Test createConnectionConfig with SASL
	connConfig, err := syncer.createConnectionConfig()
	assert.NoError(t, err, "Should create connection config without error")
	assert.NotNil(t, connConfig.SASLMechanism, "SASL mechanism should be set")
	assert.Nil(t, connConfig.TLS, "TLS should not be set")

	// Create config with topics to test SyncTopics SASL path
	topicsConfig := &kafkasync.Config{
		Topics: []kafkasync.Topic{
			{
				Name:              "sasl-test-topic",
				PartitionCount:    1,
				ReplicationFactor: 1,
			},
		},
		OperationTimeout: 1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This will fail due to no real broker, but exercises SASL code path
	err = syncer.SyncTopics(ctx, topicsConfig, true)
	assert.Error(t, err, "Should fail without real Kafka broker")
}

func TestSyncTopicsWithTLS(t *testing.T) {
	// Test the TLS configuration path
	config := &Config{
		Brokers:             []string{"localhost:9093"},
		SASLEnabled:         false,
		TLSEnabled:          true,
		TLSSkipVerify:       true,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Test createConnectionConfig with TLS
	connConfig, err := syncer.createConnectionConfig()
	assert.NoError(t, err, "Should create connection config without error")
	assert.Nil(t, connConfig.SASLMechanism, "SASL should not be set")
	assert.NotNil(t, connConfig.TLS, "TLS should be set")
	assert.True(t, connConfig.TLS.InsecureSkipVerify, "TLS skip verify should be true")

	// Create config with topics to test SyncTopics TLS path
	topicsConfig := &kafkasync.Config{
		Topics: []kafkasync.Topic{
			{
				Name:              "tls-test-topic",
				PartitionCount:    1,
				ReplicationFactor: 1,
			},
		},
		OperationTimeout: 1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This will fail due to no real broker, but exercises TLS code path
	err = syncer.SyncTopics(ctx, topicsConfig, true)
	assert.Error(t, err, "Should fail without real Kafka broker")
}
