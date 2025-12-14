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

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/exemplarreceiver"
	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/orgapikey"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	exemplarCmd := &cobra.Command{
		Use:   "exemplar-receiver",
		Short: "exemplar receiver commands",
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "start exemplar-receiver server",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "exemplar-receiver"
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

			// Start disk usage monitoring
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

			// Mark as healthy immediately - health is not dependent on database readiness
			healthServer.SetStatus(healthcheck.StatusHealthy)

			// Connect to lrdb
			mdb, err := lrdb.LRDBStore(ctx)
			if err != nil {
				slog.Error("Failed to connect to lr database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to lr database: %w", err)
			}

			// Connect to configdb for API key validation
			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				slog.Error("Failed to connect to config database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to config database: %w", err)
			}

			configservice.NewGlobal(cdb, 5*time.Minute)

			// Mark as ready now that database connections are established and migrations have been checked
			healthServer.SetReady(true)
			healthServer.SetStatus(healthcheck.StatusHealthy)

			// Create API key provider
			apiKeyProvider := orgapikey.NewDBProvider(cdb)

			// Create and start exemplar receiver service
			receiver, err := exemplarreceiver.NewReceiverService(mdb, apiKeyProvider)
			if err != nil {
				slog.Error("Failed to create exemplar receiver service", slog.Any("error", err))
				return fmt.Errorf("failed to create exemplar receiver service: %w", err)
			}

			if err := receiver.Run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("shutting down", "error", err)
					return nil
				}
				return err
			}
			return nil
		},
	}

	exemplarCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(exemplarCmd)
}
