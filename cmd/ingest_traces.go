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

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/lrdb"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-traces",
		Short: "Ingest traces from the inqueue table or Kafka",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-traces"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "ingest"),
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

			go diskUsageLoop(ctx)

			go debugging.RunPprof(ctx)

			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			mdb, err := lrdb.LRDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to open LRDB store: %w", err)
			}

			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to open ConfigDB store: %w", err)
			}

			cmgr, err := cloudstorage.NewCloudManagers(ctx)
			if err != nil {
				return fmt.Errorf("failed to create cloud managers: %w", err)
			}

			sp := storageprofile.NewStorageProfileProvider(cdb)

			ll := logctx.FromContext(ctx).With("instanceID", myInstanceID)
			ctx = logctx.WithLogger(ctx, ll)

			kafkaFactory := fly.NewFactory(&cfg.Kafka)
			slog.Info("Starting traces ingestion with accumulation consumer")

			consumer, err := metricsprocessing.NewTraceIngestConsumer(ctx, kafkaFactory, cfg, mdb, sp, cmgr)
			if err != nil {
				return fmt.Errorf("failed to create Kafka ingest consumer: %w", err)
			}
			defer func() {
				if err := consumer.Close(); err != nil {
					slog.Error("Error closing Kafka consumer", slog.Any("error", err))
				}
			}()

			healthServer.SetStatus(healthcheck.StatusHealthy)

			if err := consumer.Run(ctx); err != nil {
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
