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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/queryworker"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

func init() {
	cmd := &cobra.Command{
		Use:   "query-worker",
		Short: "start query-worker service",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "query-worker"
			addlAttrs := attribute.NewSet()
			ctx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			// Load config
			cfg, err := config.Load()
			if err != nil {
				slog.Warn("Failed to load config, using defaults", slog.Any("error", err))
				cfg = &config.Config{}
			}
			slog.Info("Query worker config loaded",
				slog.Int("maxParallelDownloads", cfg.Query.MaxParallelDownloads))

			go diskUsageLoop(ctx)

			go debugging.RunPprof(ctx)

			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				slog.Error("Failed to connect to config database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to config database: %w", err)
			}

			configservice.NewGlobal(cdb, 5*time.Minute)

			sp := storageprofile.NewStorageProfileProvider(cdb)

			cloudManagers, err := cloudstorage.NewCloudManagers(ctx)
			if err != nil {
				return fmt.Errorf("failed to create cloud managers: %w", err)
			}

			healthServer.SetStatus(healthcheck.StatusHealthy)

			worker, err := queryworker.NewWorkerService(5, 5, 5, cfg.Query.MaxParallelDownloads, sp, cloudManagers)
			if err != nil {
				return fmt.Errorf("failed to create worker service: %w", err)
			}
			if err := worker.Run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("shutting down", "error", err)
					return nil
				}
				return err
			}
			return nil
		},
	}

	rootCmd.AddCommand(cmd)
}
