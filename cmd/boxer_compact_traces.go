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
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
)

func init() {
	cmd := &cobra.Command{
		Use:   "boxer-compact-traces",
		Short: "Traces compaction boxer",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-boxer-traces-compact"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "boxer"),
			)
			ctx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(ctx)

			// Start pprof server
			go debugging.RunPprof(ctx)

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			// Get main config
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Create database connection
			mdb, err := dbopen.LRDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to open LRDB store: %w", err)
			}

			// Create Kafka factory
			kafkaFactory := fly.NewFactory(&cfg.Fly)

			// Create Kafka-based compaction boxer consumer
			consumer, err := metricsprocessing.NewTraceCompactionBoxerConsumer(ctx, kafkaFactory, mdb, cfg)
			if err != nil {
				return fmt.Errorf("failed to create compaction boxer consumer: %w", err)
			}
			defer func() { _ = consumer.Close() }()

			healthServer.SetStatus(healthcheck.StatusHealthy)

			return consumer.Run(ctx)
		},
	}

	rootCmd.AddCommand(cmd)
}
