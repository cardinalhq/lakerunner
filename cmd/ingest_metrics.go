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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-metrics",
		Short: "Ingest metrics from the inqueue table",
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

			loop, err := NewIngestLoopContext(ctx, "metrics")
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}
			defer func() {
				if err := loop.Close(); err != nil {
					slog.Error("Error closing ingest loop context", slog.Any("error", err))
				}
			}()

			// Mark as healthy once loop is created and about to start
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return IngestLoopWithBatch(loop, nil, metricIngestBatch)
		},
	}

	rootCmd.AddCommand(cmd)
}

func metricIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	return ingestion.ProcessBatch(ctx, ll, tmpdir, sp, mdb, awsmanager, items, ingest_dateint, rpfEstimate, loop.exemplarProcessor)
}
