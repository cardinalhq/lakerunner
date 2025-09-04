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
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logsprocessing/compaction"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-logs",
		Short: "Compact logs into optimally sized files",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-compact-logs"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			compactLogsDoneCtx = doneCtx

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(doneCtx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			loop, err := NewRunqueueLoopContext(doneCtx, "logs", "compact")
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			// Mark as healthy once loop is created and about to start
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return RunqueueLoop(loop, compactLogsFor, nil)
		},
	}
	rootCmd.AddCommand(cmd)
}

var compactLogsDoneCtx context.Context

func compactLogsFor(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	_ any,
) error {
	// Create metric recorder for compaction
	metricsRecorder := &compaction.MetricRecorder{
		Logger: ll,
		SegmentsFilteredCounter: segmentsFilteredCounter,
	}
	
	return compaction.CompactLogsFor(ctx, ll, tmpdir, awsmanager, sp, mdb, inf, rpfEstimate, metricsRecorder)
}