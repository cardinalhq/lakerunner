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
	"os/signal"
	"strconv"
	"syscall"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/externalscaler"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

var (
	monitoringPort     int
	monitoringGRPCPort int
)

func init() {
	monitoringCmd := &cobra.Command{
		Use:   "monitoring",
		Short: "Monitoring and scaling services",
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the KEDA external scaler service",
		Long:  `Starts a gRPC service that implements the KEDA external scaler interface for auto-scaling Lakerunner services.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMonitoringServe(cmd.Context())
		},
	}

	serveCmd.Flags().IntVar(&monitoringPort, "port", getEnvInt("MONITORING_PORT", 8090), "HTTP port for health checks")
	serveCmd.Flags().IntVar(&monitoringGRPCPort, "grpc-port", getEnvInt("MONITORING_GRPC_PORT", 9090), "gRPC port for external scaler")

	monitoringCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(monitoringCmd)
}

func runMonitoringServe(ctx context.Context) error {
	servicename := "monitoring"
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
	healthConfig := healthcheck.Config{Port: monitoringPort}
	healthServer := healthcheck.NewServer(healthConfig)

	go func() {
		if err := healthServer.Start(doneCtx); err != nil {
			slog.Error("Health check server stopped", slog.Any("error", err))
		}
	}()

	// Mark as healthy immediately - health is not dependent on database readiness
	healthServer.SetStatus(healthcheck.StatusHealthy)

	slog.Info("Starting KEDA external scaler service",
		"http_port", monitoringPort,
		"grpc_port", monitoringGRPCPort)

	config := externalscaler.Config{
		GRPCPort: monitoringGRPCPort,
	}

	service, err := externalscaler.NewService(doneCtx, config)
	if err != nil {
		slog.Error("Failed to create external scaler service", "error", err)
		return err
	}
	defer service.Close()

	// Mark as ready now that external scaler service is created (database connections established)
	healthServer.SetReady(true)

	signalCtx, cancel := signal.NotifyContext(doneCtx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := service.Start(signalCtx); err != nil {
		slog.Error("External scaler service failed", "error", err)
		return err
	}

	slog.Info("External scaler service stopped")
	return nil
}

func getEnvInt(key string, defaultValue int) int {
	if str := os.Getenv(key); str != "" {
		if val, err := strconv.Atoi(str); err == nil {
			return val
		}
	}
	return defaultValue
}
