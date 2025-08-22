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

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/promql"
)

func init() {
	cmd := &cobra.Command{
		Use:   "query-api",
		Short: "start query-api server",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "query-api"
			addlAttrs := attribute.NewSet()
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
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
				if err := healthServer.Start(doneCtx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			mdb, err := dbopen.LRDBStore(context.Background())
			if err != nil {
				slog.Error("Failed to connect to lr database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to lr database: %w", err)
			}

			// Create and start worker discovery
			workerDiscovery, err := promql.CreateWorkerDiscovery()
			if err != nil {
				slog.Error("Failed to create worker discovery", slog.Any("error", err))
				return fmt.Errorf("failed to create worker discovery: %w", err)
			}

			if err := workerDiscovery.Start(doneCtx); err != nil {
				slog.Error("Failed to start worker discovery", slog.Any("error", err))
				return fmt.Errorf("failed to start worker discovery: %w", err)
			}
			defer func() {
				if err := workerDiscovery.Stop(); err != nil {
					slog.Error("Failed to stop worker discovery", slog.Any("error", err))
				}
			}()

			querier, err := promql.NewQuerierService(mdb, workerDiscovery)
			if err != nil {
				slog.Error("Failed to create querier service", slog.Any("error", err))
				return fmt.Errorf("failed to create querier service: %w", err)
			}

			// Mark as healthy once all services are ready
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return querier.Run(doneCtx)
		},
	}

	rootCmd.AddCommand(cmd)
}
