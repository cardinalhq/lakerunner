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
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
)

var skipDB bool
var skipKafka bool

func init() {
	SetupCmd.Flags().BoolVar(&skipDB, "skip-db", false, "Skip database migrations")
	SetupCmd.Flags().BoolVar(&skipKafka, "skip-kafka", false, "Skip Kafka topic setup")

	// Inherit migrate command flags for database operations
	SetupCmd.Flags().StringVar(&databases, "databases", "lrdb,configdb", "Comma-separated list of databases to migrate (lrdb,configdb)")
	SetupCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to storage profile YAML configuration file")
	SetupCmd.Flags().StringVarP(&apiKeysFile, "api-keys", "k", "", "Path to API keys YAML configuration file")

	rootCmd.AddCommand(SetupCmd)
}

var SetupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Run complete LakeRunner setup (database migrations and Kafka topics)",
	Long:  "Run database migrations and ensure required Kafka topics exist",
	RunE:  setup,
}

func setup(cmd *cobra.Command, args []string) error {
	slog.Info("Starting LakeRunner setup")

	// Run database migrations unless skipped
	if !skipDB {
		slog.Info("Running database migrations")
		if err := migrate(cmd, args); err != nil {
			return fmt.Errorf("database migrations failed: %w", err)
		}
	} else {
		slog.Info("Skipping database migrations")
	}

	// Run Kafka topic setup unless skipped
	if !skipKafka {
		slog.Info("Setting up Kafka topics")
		if err := ensureKafkaTopics(); err != nil {
			return fmt.Errorf("Kafka topic setup failed: %w", err)
		}
	} else {
		slog.Info("Skipping Kafka topic setup")
	}

	slog.Info("LakeRunner setup completed successfully")
	return nil
}
