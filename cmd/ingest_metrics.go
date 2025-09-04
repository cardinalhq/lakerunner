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
	"log/slog"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-metrics",
		Short: "Ingest metrics from the inqueue table or Kafka",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
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

			// Kafka is required for ingestion
			kafkaFactory := fly.NewFactoryFromEnv()
			if !kafkaFactory.IsEnabledForIngestion() {
				return fmt.Errorf("Kafka is required for ingestion but is not enabled")
			}

			slog.Info("Starting metrics ingestion with Kafka consumer")

			consumer, err := NewKafkaIngestConsumer("metrics", "lakerunner.ingest.metrics")
			if err != nil {
				return fmt.Errorf("failed to create Kafka consumer: %w", err)
			}
			defer func() {
				if err := consumer.Close(); err != nil {
					slog.Error("Error closing Kafka consumer", slog.Any("error", err))
				}
			}()

			// Mark as healthy once consumer is created and about to start
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return consumer.Run(ctx)
		},
	}

	rootCmd.AddCommand(cmd)
}

func metricIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	cloudManagers *cloudstorage.CloudManagers, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	return ingestion.ProcessBatch(ctx, ll, tmpdir, sp, mdb, cloudManagers.AWS, items, ingest_dateint, rpfEstimate, loop.exemplarProcessor)
}
