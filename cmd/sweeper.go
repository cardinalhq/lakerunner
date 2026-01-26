// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/sweeper"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

func init() {
	var syncLegacyTables bool

	cmd := &cobra.Command{
		Use:   "sweeper",
		Short: "Do general cleanup tasks",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "lakerunner-sweeper"
			addlAttrs := attribute.NewSet(
				attribute.String("action", "sweep"),
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

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			// Check environment variable first, then fall back to flag
			finalSyncLegacyTables := syncLegacyTables
			if envValue := os.Getenv("SYNC_LEGACY_TABLES"); envValue != "" {
				if parsed, err := strconv.ParseBool(envValue); err == nil {
					finalSyncLegacyTables = parsed
				}
			}

			// Load configuration
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Mark as healthy immediately - health is not dependent on database readiness
			healthServer.SetStatus(healthcheck.StatusHealthy)

			cmd := sweeper.New(myInstanceID, finalSyncLegacyTables, cfg)

			// Mark as ready now that database connections are established and migrations have been checked
			healthServer.SetReady(true)

			if err := cmd.Run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("shutting down", "error", err)
					return nil
				}
				return err
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&syncLegacyTables, "sync-legacy-tables", false, "Enable periodic sync from c_ tables to bucket management tables (can also be set via SYNC_LEGACY_TABLES env var)")

	rootCmd.AddCommand(cmd)
}
