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
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/kafka-sync/kafkasync"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// TopicSyncer handles Kafka topic synchronization using kafka-sync
type TopicSyncer struct {
	config *Config
}

// NewTopicSyncer creates a new topic syncer
func NewTopicSyncer(config *Config) *TopicSyncer {
	return &TopicSyncer{
		config: config,
	}
}

// SyncTopics synchronizes Kafka topics according to the provided configuration
func (ts *TopicSyncer) SyncTopics(ctx context.Context, topicsConfig *kafkasync.Config, fix bool) error {
	// Create connection configuration
	connConfig, err := ts.createConnectionConfig()
	if err != nil {
		return fmt.Errorf("failed to create connection config: %w", err)
	}

	// Create syncer
	syncer, err := kafkasync.NewSyncer(connConfig, topicsConfig)
	if err != nil {
		return fmt.Errorf("failed to create syncer: %w", err)
	}

	// Determine sync mode
	mode := kafkasync.SyncModeInfo
	modeStr := "info"
	if fix {
		mode = kafkasync.SyncModeFix
		modeStr = "fix"
	}

	slog.Info("Starting Kafka topic synchronization",
		slog.String("mode", modeStr),
		slog.Int("topic_count", len(topicsConfig.Topics)))

	// Perform sync
	if err := syncer.Sync(ctx, mode); err != nil {
		return fmt.Errorf("failed to sync topics: %w", err)
	}

	slog.Info("Kafka topic synchronization completed")
	return nil
}

// createConnectionConfig creates a kafkasync ConnectionConfig from our fly Config
func (ts *TopicSyncer) createConnectionConfig() (kafkasync.ConnectionConfig, error) {
	connConfig := kafkasync.ConnectionConfig{
		BootstrapServers: ts.config.Brokers,
	}

	// Configure SASL if enabled
	if ts.config.SASLEnabled {
		mechanism, err := ts.createSASLMechanism()
		if err != nil {
			return connConfig, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		connConfig.SASLMechanism = mechanism
	}

	// Configure TLS if enabled
	if ts.config.TLSEnabled {
		connConfig.TLS = &tls.Config{
			InsecureSkipVerify: ts.config.TLSSkipVerify,
		}
	}

	return connConfig, nil
}

// createSASLMechanism creates the appropriate SASL mechanism based on configuration
func (ts *TopicSyncer) createSASLMechanism() (sasl.Mechanism, error) {
	switch ts.config.SASLMechanism {
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, ts.config.SASLUsername, ts.config.SASLPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, ts.config.SASLUsername, ts.config.SASLPassword)
	case "PLAIN":
		return plain.Mechanism{
			Username: ts.config.SASLUsername,
			Password: ts.config.SASLPassword,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", ts.config.SASLMechanism)
	}
}

// LoadTopicsConfig loads a kafkasync configuration from a file
func LoadTopicsConfig(filename string) (*kafkasync.Config, error) {
	return kafkasync.LoadConfigFromFile(filename)
}

// CreateDefaultTopicsConfig creates a default topics configuration
func CreateDefaultTopicsConfig(topics []kafkasync.Topic) *kafkasync.Config {
	return &kafkasync.Config{
		Defaults: kafkasync.Defaults{
			PartitionCount:    16,
			ReplicationFactor: 2,
			TopicConfig: map[string]string{
				"retention.ms": "7200000", // 2 hours
			},
		},
		Topics:           topics,
		OperationTimeout: 30 * time.Second,
	}
}
