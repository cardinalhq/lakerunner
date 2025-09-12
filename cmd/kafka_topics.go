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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
)

func ensureKafkaTopics(ctx context.Context) error {
	return ensureKafkaTopicsWithFile(ctx, "")
}

func ensureKafkaTopicsWithFile(ctx context.Context, flagKafkaTopicsFile string) error {
	if err := validateKafkaConfig(); err != nil {
		return fmt.Errorf("Kafka configuration validation failed: %w", err)
	}

	var kafkaTopicsFile string

	// Priority order: command-line flag > environment variable > default location
	if flagKafkaTopicsFile != "" {
		kafkaTopicsFile = flagKafkaTopicsFile
		slog.Info("Using Kafka topics file from command-line flag", slog.String("file", kafkaTopicsFile))
	} else if kafkaTopicsFileEnv := os.Getenv("KAFKA_TOPICS_FILE"); kafkaTopicsFileEnv != "" {
		kafkaTopicsFile = kafkaTopicsFileEnv
		slog.Info("Using Kafka topics file from KAFKA_TOPICS_FILE", slog.String("file", kafkaTopicsFile))
	} else {
		// Look for Kafka topics in the ConfigMap mount location
		kafkaTopicsPath := "/app/config/kafka_topics.yaml"
		if _, err := os.Stat(kafkaTopicsPath); err == nil {
			kafkaTopicsFile = kafkaTopicsPath
			slog.Info("Auto-detected Kafka topics file", slog.String("file", kafkaTopicsPath))
		} else {
			slog.Info("No Kafka topics configuration found, skipping")
			return nil
		}
	}

	// Load Kafka connection config from existing env vars
	appConfig, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load app config: %w", err)
	}

	// Create topic syncer
	factory := fly.NewFactory(&appConfig.Fly)
	syncer := factory.CreateTopicSyncer()

	// Load topics configuration using kafka-sync
	topicsConfig, err := fly.LoadTopicsConfig(kafkaTopicsFile)
	if err != nil {
		return fmt.Errorf("failed to load Kafka topics configuration: %w", err)
	}

	if len(topicsConfig.Topics) == 0 {
		slog.Info("No Kafka topics configured, skipping")
		return nil
	}

	// Sync topics (with fix mode enabled to create missing topics)
	return syncer.SyncTopics(ctx, topicsConfig, true)
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
