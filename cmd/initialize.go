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

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/cmd/initialize"
	"github.com/cardinalhq/lakerunner/configdb"
)

func init() {
	var configFile string
	var apiKeysFile string
	var replace bool

	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "Initialize storage profiles and API keys from YAML configuration",
		Long: `Initialize storage profiles and API keys by reading YAML configuration files and
populating the database tables.

This command replaces the environment variable-based file configuration approach
with a one-time database initialization from YAML.

Storage profiles should be provided as a simple list (YAML array) format.

By default, this command will fail if storage profiles already exist in the database.
Use --replace to perform a proper sync that adds, updates, or removes profiles as needed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInitialize(configFile, apiKeysFile, replace)
		},
	}

	initializeCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to storage profile YAML configuration file (required)")
	initializeCmd.Flags().StringVarP(&apiKeysFile, "api-keys", "k", "", "Path to API keys YAML configuration file (optional)")
	initializeCmd.Flags().BoolVar(&replace, "replace", false, "Replace existing storage profiles and API keys with proper sync (add/update/remove)")
	if err := initializeCmd.MarkFlagRequired("config"); err != nil {
		panic(fmt.Sprintf("failed to mark flag as required: %v", err))
	}

	rootCmd.AddCommand(initializeCmd)
}

func runInitialize(configFile string, apiKeysFile string, replace bool) error {
	ctx := context.Background()

	// Connect to configdb
	configDBPool, err := dbopen.ConnectToConfigDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}
	defer configDBPool.Close()

	// Check if storage profiles already exist (safety check)
	if !replace {
		qtxCheck := configdb.New(configDBPool)
		hasProfiles, err := qtxCheck.HasExistingStorageProfiles(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existing storage profiles: %w", err)
		}
		if hasProfiles {
			return fmt.Errorf("storage profiles already exist in database. Use --replace flag to perform a sync that replaces existing data")
		}
	}

	// Start transaction for atomic import
	tx, err := configDBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			slog.Warn("Failed to rollback transaction", slog.Any("error", err))
		}
	}()

	qtx := configdb.New(tx)

	// Use initialize package for business logic
	if err := initialize.InitializeConfig(ctx, configFile, apiKeysFile, qtx, slog.Default(), replace); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Initialization completed successfully")
	return nil
}
