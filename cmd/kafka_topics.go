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

	"github.com/cardinalhq/kafka-sync/kafkasync"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
)

func ensureKafkaTopicsWithFile(ctx context.Context, flagKafkaTopicsFile string) error {
	// Load Kafka connection config
	appConfig, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load app config: %w", err)
	}

	// Create topic syncer
	factory := fly.NewFactoryFromKafkaConfig(&appConfig.Kafka)
	syncer := factory.CreateTopicSyncer()

	var topicsConfig *kafkasync.Config

	finalKafkaTopicsConfig := appConfig.KafkaTopics

	// Priority: external file override > generated from base config
	if flagKafkaTopicsFile != "" {
		slog.Info("Loading Kafka topics override from command-line flag", slog.String("file", flagKafkaTopicsFile))
		override, err := config.LoadKafkaTopicsOverride(flagKafkaTopicsFile)
		if err != nil {
			return fmt.Errorf("failed to load Kafka topics override file: %w", err)
		}
		finalKafkaTopicsConfig = config.MergeKafkaTopicsOverride(appConfig.KafkaTopics, override)
		slog.Info("Merged Kafka topics override", slog.String("prefix", finalKafkaTopicsConfig.TopicPrefix))
	} else if kafkaTopicsFileEnv := os.Getenv("KAFKA_TOPICS_FILE"); kafkaTopicsFileEnv != "" {
		slog.Info("Loading Kafka topics override from KAFKA_TOPICS_FILE", slog.String("file", kafkaTopicsFileEnv))
		override, err := config.LoadKafkaTopicsOverride(kafkaTopicsFileEnv)
		if err != nil {
			return fmt.Errorf("failed to load Kafka topics override file: %w", err)
		}
		finalKafkaTopicsConfig = config.MergeKafkaTopicsOverride(appConfig.KafkaTopics, override)
		slog.Info("Merged Kafka topics override", slog.String("prefix", finalKafkaTopicsConfig.TopicPrefix))
	} else if kafkaTopicsPath := "/app/config/kafka_topics.yaml"; fileExists(kafkaTopicsPath) {
		slog.Info("Loading Kafka topics override from ConfigMap", slog.String("file", kafkaTopicsPath))
		override, err := config.LoadKafkaTopicsOverride(kafkaTopicsPath)
		if err != nil {
			return fmt.Errorf("failed to load Kafka topics override file: %w", err)
		}
		finalKafkaTopicsConfig = config.MergeKafkaTopicsOverride(appConfig.KafkaTopics, override)
		slog.Info("Merged Kafka topics override", slog.String("prefix", finalKafkaTopicsConfig.TopicPrefix))
	} else {
		slog.Info("Using internal Kafka topics configuration", slog.String("prefix", finalKafkaTopicsConfig.TopicPrefix))
	}

	// Generate topics config (either base config or merged with overrides)
	// Note: prefix cannot change via override - always use base TopicRegistry
	topicRegistry := appConfig.TopicRegistry

	syncConfig := topicRegistry.GenerateKafkaSyncConfig(finalKafkaTopicsConfig)
	topicsConfig = convertToKafkaSyncConfig(syncConfig)

	if len(topicsConfig.Topics) == 0 {
		slog.Info("No Kafka topics configured, skipping")
		return nil
	}

	slog.Info("Syncing Kafka topics", slog.Int("count", len(topicsConfig.Topics)))
	// Sync topics (with fix mode enabled to create missing topics)
	return syncer.SyncTopics(ctx, topicsConfig, true)
}

// fileExists checks if a file exists and is not a directory
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// convertToKafkaSyncConfig converts our KafkaSyncConfig to kafkasync.Config
func convertToKafkaSyncConfig(syncConfig config.KafkaSyncConfig) *kafkasync.Config {
	// Convert TopicConfig from interface{} to string values for kafkasync
	topicConfig := make(map[string]string)
	for k, v := range syncConfig.Defaults.TopicConfig {
		if s, ok := v.(string); ok {
			topicConfig[k] = s
		} else {
			topicConfig[k] = fmt.Sprintf("%v", v)
		}
	}

	kafkaConfig := &kafkasync.Config{
		Defaults: kafkasync.Defaults{
			PartitionCount:    syncConfig.Defaults.PartitionCount,
			ReplicationFactor: syncConfig.Defaults.ReplicationFactor,
			TopicConfig:       topicConfig,
		},
		Topics: make([]kafkasync.Topic, len(syncConfig.Topics)),
	}

	for i, topic := range syncConfig.Topics {
		kafkaTopic := kafkasync.Topic{
			Name: topic.Name,
		}

		// Set values (kafkasync uses 0 to mean "use defaults")
		if topic.PartitionCount > 0 {
			kafkaTopic.PartitionCount = topic.PartitionCount
		}
		if topic.ReplicationFactor > 0 {
			kafkaTopic.ReplicationFactor = topic.ReplicationFactor
		}
		if topic.TopicConfig != nil {
			// Convert interface{} values to strings
			config := make(map[string]string)
			for k, v := range topic.TopicConfig {
				if s, ok := v.(string); ok {
					config[k] = s
				} else {
					config[k] = fmt.Sprintf("%v", v)
				}
			}
			kafkaTopic.Config = config
		}

		kafkaConfig.Topics[i] = kafkaTopic
	}

	return kafkaConfig
}
