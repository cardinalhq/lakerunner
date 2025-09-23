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

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/admin"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

var (
	adminGRPCPort string
)

func init() {
	adminAPICmd := &cobra.Command{
		Use:   "admin-api",
		Short: "Admin API services",
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the admin gRPC server",
		Long:  `Starts a gRPC server that provides administrative operations for Lakerunner.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "admin-api"
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

			// Mark as healthy immediately
			healthServer.SetStatus(healthcheck.StatusHealthy)

			// Create and start the admin service
			service, err := admin.NewService(adminGRPCPort)
			if err != nil {
				slog.Error("Failed to create admin service", slog.Any("error", err))
				return fmt.Errorf("failed to create admin service: %w", err)
			}

			// Mark as ready now that the service is initialized
			healthServer.SetReady(true)
			healthServer.SetStatus(healthcheck.StatusHealthy)

			// Run the admin service
			if err := service.Run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("shutting down", "error", err)
					return nil
				}
				return err
			}
			return nil
		},
	}

	serveCmd.Flags().StringVar(&adminGRPCPort, "grpc-port", ":9091", "gRPC server port")
	adminAPICmd.AddCommand(serveCmd)
	rootCmd.AddCommand(adminAPICmd)
}
