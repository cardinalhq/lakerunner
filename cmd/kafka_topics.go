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

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/cardinalhq/lakerunner/cmd/initialize"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
)

type TopicConfig struct {
	Name              string            `yaml:"name"`
	Partitions        int               `yaml:"partitions"`
	ReplicationFactor int               `yaml:"replicationFactor"`
	Config            map[string]string `yaml:"config,omitempty"`
}

func ensureKafkaTopics(ctx context.Context) error {
	if err := validateKafkaConfig(); err != nil {
		return fmt.Errorf("Kafka configuration validation failed: %w", err)
	}

	var kafkaTopicsFile string

	// Check KAFKA_TOPICS_FILE environment variable first
	if kafkaTopicsFileEnv := os.Getenv("KAFKA_TOPICS_FILE"); kafkaTopicsFileEnv != "" {
		kafkaTopicsFile = kafkaTopicsFileEnv
		slog.Info("Using Kafka topics file from KAFKA_TOPICS_FILE", slog.String("file", kafkaTopicsFile))
	} else {
		// Look for Kafka topics in the ConfigMap mount location
		kafkaTopicsPath := "/app/config/kafka_topics.yaml"
		if _, err := os.Stat(kafkaTopicsPath); err == nil {
			kafkaTopicsFile = kafkaTopicsPath
			slog.Info("Auto-detected Kafka topics file", slog.String("file", kafkaTopicsFile))
		} else {
			slog.Info("No Kafka topics configuration found, skipping")
			return nil
		}
	}

	// Load and create Kafka topics
	topics, err := loadKafkaTopics(kafkaTopicsFile)
	if err != nil {
		return fmt.Errorf("failed to load Kafka topics: %w", err)
	}

	if len(topics) == 0 {
		slog.Info("No Kafka topics configured, skipping")
		return nil
	}

	// Load Kafka connection config from existing env vars
	appConfig, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load app config: %w", err)
	}

	// Use existing Kafka factory
	factory := fly.NewFactory(&appConfig.Fly)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return createTopics(ctx, factory, topics)
}

func loadKafkaTopics(filename string) ([]TopicConfig, error) {
	fileReader := initialize.OSFileReader{}
	return loadKafkaTopicsWithReader(filename, fileReader)
}

func loadKafkaTopicsWithReader(filename string, fileReader initialize.FileReader) ([]TopicConfig, error) {
	contents, err := initialize.LoadFileContentsWithReader(filename, fileReader)
	if err != nil {
		return nil, err
	}

	var topics []TopicConfig
	if err := initialize.UnmarshalYAML(contents, &topics); err != nil {
		return nil, fmt.Errorf("failed to parse Kafka topics config: %w", err)
	}

	return topics, nil
}

func createTopics(ctx context.Context, factory *fly.Factory, topics []TopicConfig) error {
	config := factory.GetConfig()

	// Create authenticated dialer using factory
	dialer, err := factory.CreateDialer()
	if err != nil {
		return fmt.Errorf("failed to create authenticated dialer: %w", err)
	}

	// Connect using the authenticated dialer
	conn, err := dialer.DialContext(ctx, "tcp", config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Test connectivity
	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("failed to get broker metadata: %w", err)
	}
	slog.Info("Connected to Kafka", slog.Any("brokers", config.Brokers))

	// Get existing topics
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	existing := make(map[string]bool)
	for _, p := range partitions {
		existing[p.Topic] = true
	}

	// Create missing topics
	var toCreate []kafka.TopicConfig
	for _, topic := range topics {
		if existing[topic.Name] {
			slog.Info("Topic already exists", slog.String("topic", topic.Name))
			continue
		}

		kafkaConfig := kafka.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: topic.ReplicationFactor,
		}

		// Add custom config
		for key, value := range topic.Config {
			kafkaConfig.ConfigEntries = append(kafkaConfig.ConfigEntries,
				kafka.ConfigEntry{ConfigName: key, ConfigValue: value})
		}

		toCreate = append(toCreate, kafkaConfig)
		slog.Info("Will create topic",
			slog.String("name", topic.Name),
			slog.Int("partitions", topic.Partitions))
	}

	if len(toCreate) == 0 {
		slog.Info("All topics already exist")
		return nil
	}

	// Create topics
	if err := conn.CreateTopics(toCreate...); err != nil {
		return fmt.Errorf("failed to create topics: %w", err)
	}

	slog.Info("Created topics", slog.Int("count", len(toCreate)))
	return nil
}

// validateKafkaConfig checks required Kafka environment variables similar to database validation
func validateKafkaConfig() error {
	// Check if Kafka is configured via environment variables or URL
	if brokers := os.Getenv("LAKERUNNER_FLY_BROKERS"); brokers != "" {
		return nil // We have brokers configured
	}

	// Check individual broker configuration
	var missing []string

	// For Kafka, we require at least one broker
	if os.Getenv("LAKERUNNER_FLY_BROKERS") == "" {
		missing = append(missing, "LAKERUNNER_FLY_BROKERS")
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"missing required Kafka environment variable(s): %s",
			strings.Join(missing, ", "),
		)
	}

	return nil
}
