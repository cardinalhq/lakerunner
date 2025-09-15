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
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/externalscaler"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
)

var (
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

	serveCmd.Flags().IntVar(&monitoringGRPCPort, "grpc-port", getEnvInt("MONITORING_GRPC_PORT", 9090), "gRPC port for external scaler")

	monitoringCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(monitoringCmd)
}

func runMonitoringServe(_ context.Context) error {
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
	healthConfig := healthcheck.GetConfigFromEnv()
	healthServer := healthcheck.NewServer(healthConfig)

	go func() {
		if err := healthServer.Start(doneCtx); err != nil {
			slog.Error("Health check server stopped", slog.Any("error", err))
		}
	}()

	// Mark as healthy immediately - health is not dependent on database readiness
	healthServer.SetStatus(healthcheck.StatusHealthy)

	// Initialize configuration for external scaler
	scalerConfig := externalscaler.Config{
		GRPCPort: monitoringGRPCPort,
	}

	// Load app config for Kafka
	appConfig, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config for Kafka monitoring", "error", err)
	} else {
		// Create consumer lag monitor using the convenience function
		lagMonitor, err := fly.NewConsumerLagMonitor(
			appConfig,
			30*time.Second, // Poll every 30 seconds
		)
		if err != nil {
			slog.Error("Failed to create Kafka lag monitor", "error", err)
		} else {
			// Start the lag monitor polling
			go lagMonitor.Start(doneCtx)

			// Provide the lag monitor directly to external scaler
			scalerConfig.LagMonitor = lagMonitor
			slog.Info("Kafka lag monitor integrated with external scaler")
		}
	}

	slog.Info("Starting KEDA external scaler service", "grpc_port", monitoringGRPCPort)

	service, err := externalscaler.NewService(doneCtx, scalerConfig)
	if err != nil {
		slog.Error("Failed to create external scaler service", "error", err)
		// Mark as ready even if database connection fails - this prevents the pod from being killed
		// The external scaler will retry database connections when needed
		healthServer.SetReady(true)
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
