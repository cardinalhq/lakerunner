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
)

// TopicSyncer handles Kafka topic synchronization using kafka-sync
type TopicSyncer struct {
	factory *Factory
}

// NewTopicSyncer creates a new topic syncer
func NewTopicSyncer(factory *Factory) *TopicSyncer {
	return &TopicSyncer{
		factory: factory,
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
	config := ts.factory.GetConfig()
	connConfig := kafkasync.ConnectionConfig{
		BootstrapServers: config.Brokers,
	}

	// Configure SASL if enabled
	if config.SASLEnabled {
		mechanism, err := ts.factory.createSASLMechanism()
		if err != nil {
			return connConfig, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		connConfig.SASLMechanism = mechanism
	}

	// Configure TLS if enabled
	if config.TLSEnabled {
		connConfig.TLS = &tls.Config{
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}

	return connConfig, nil
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
		OperationTimeout: 5 * time.Minute, // Increased timeout for topic operations
	}
}
