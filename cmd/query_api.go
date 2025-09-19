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
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/lrdb"
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/orgapikey"
	"github.com/cardinalhq/lakerunner/queryapi"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

func init() {
	cmd := &cobra.Command{
		Use:   "query-api",
		Short: "start query-api server",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "query-api"
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

			// Mark as ready now that database connections are established and migrations have been checked
			healthServer.SetReady(true)
			healthServer.SetStatus(healthcheck.StatusHealthy)

			// Create API key provider
			apiKeyProvider := orgapikey.NewDBProvider(cdb)

			// Create and start worker discovery
			workerDiscovery, err := queryapi.CreateWorkerDiscovery()
			if err != nil {
				slog.Error("Failed to create worker discovery", slog.Any("error", err))
				return fmt.Errorf("failed to create worker discovery: %w", err)
			}

			if err := workerDiscovery.Start(ctx); err != nil {
				slog.Error("Failed to start worker discovery", slog.Any("error", err))
				return fmt.Errorf("failed to start worker discovery: %w", err)
			}
			defer func() {
				if err := workerDiscovery.Stop(); err != nil {
					slog.Error("Failed to stop worker discovery", slog.Any("error", err))
				}
			}()

			querier, err := queryapi.NewQuerierService(mdb, workerDiscovery, apiKeyProvider)
			if err != nil {
				slog.Error("Failed to create querier service", slog.Any("error", err))
				return fmt.Errorf("failed to create querier service: %w", err)
			}

			// All services are now fully initialized

			return querier.Run(ctx)
		},
	}

	rootCmd.AddCommand(cmd)
}
