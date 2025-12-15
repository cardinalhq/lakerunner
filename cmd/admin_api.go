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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/admin"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/configservice"
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

			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				slog.Error("Failed to connect to config database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to config database: %w", err)
			}

			configservice.NewGlobal(cdb, 5*time.Minute)

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

	createInitialKeyCmd := &cobra.Command{
		Use:   "create-initial-key",
		Short: "Create the initial admin API key (only works if no keys exist)",
		Long:  `Generates a secure random API key and stores it in the database. Only works if no admin API keys already exist.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx := context.Background()

			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to config database: %w", err)
			}

			// Check if any keys already exist
			existingKeys, err := cdb.GetAllAdminAPIKeys(ctx)
			if err != nil {
				return fmt.Errorf("failed to check existing keys: %w", err)
			}

			if len(existingKeys) > 0 {
				return fmt.Errorf("admin API keys already exist (%d keys); cannot create initial key", len(existingKeys))
			}

			// Generate a secure random key
			keyBytes := make([]byte, 20) // 20 bytes = 32 base32 chars
			if _, err := rand.Read(keyBytes); err != nil {
				return fmt.Errorf("failed to generate random key: %w", err)
			}
			apiKey := "ak_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(keyBytes))

			// Hash the key for storage
			h := sha256.Sum256([]byte(apiKey))
			keyHash := fmt.Sprintf("%x", h)

			// Store in database
			description := "Initial admin API key"
			_, err = cdb.CreateAdminAPIKey(ctx, configdb.CreateAdminAPIKeyParams{
				KeyHash:     keyHash,
				Name:        "initial-admin-key",
				Description: &description,
			})
			if err != nil {
				return fmt.Errorf("failed to create admin API key: %w", err)
			}

			fmt.Println("Initial admin API key created successfully.")
			fmt.Println("")
			fmt.Println("API Key (save this, it cannot be retrieved later):")
			fmt.Println(apiKey)
			fmt.Println("")
			fmt.Println("Set this as LAKERUNNER_ADMIN_API_KEY or use --api-key with lakectl.")

			return nil
		},
	}
	adminAPICmd.AddCommand(createInitialKeyCmd)

	rootCmd.AddCommand(adminAPICmd)
}
