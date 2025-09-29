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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	ingestLogs     bool
	ingestMetrics  bool
	ingestTraces   bool
	compactLogs    bool
	compactMetrics bool
	compactTraces  bool
	rollupMetrics  bool
	allTasks       bool
)

func init() {
	boxerCmd := &cobra.Command{
		Use:   "boxer",
		Short: "Run boxer with specified tasks",
		Long:  `Run boxer with one or more specified tasks. At least one task must be specified.`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate at least one task is specified
			if !allTasks && !ingestLogs && !ingestMetrics && !ingestTraces && !compactLogs && !compactMetrics && !compactTraces && !rollupMetrics {
				return fmt.Errorf("at least one task must be specified")
			}
			// If --all, set all task flags
			if allTasks {
				ingestLogs = true
				ingestMetrics = true
				ingestTraces = true
				compactLogs = true
				compactMetrics = true
				compactTraces = true
				rollupMetrics = true
			}
			return nil
		},
		RunE: runBoxer,
	}

	boxerCmd.Flags().BoolVar(&ingestLogs, "ingest-logs", false, "Run log ingestion")
	boxerCmd.Flags().BoolVar(&ingestMetrics, "ingest-metrics", false, "Run metrics ingestion")
	boxerCmd.Flags().BoolVar(&ingestTraces, "ingest-traces", false, "Run trace ingestion")
	boxerCmd.Flags().BoolVar(&compactLogs, "compact-logs", false, "Run log compaction")
	boxerCmd.Flags().BoolVar(&compactMetrics, "compact-metrics", false, "Run metrics compaction")
	boxerCmd.Flags().BoolVar(&compactTraces, "compact-traces", false, "Run trace compaction")
	boxerCmd.Flags().BoolVar(&rollupMetrics, "rollup-metrics", false, "Run metrics rollup")
	boxerCmd.Flags().BoolVar(&allTasks, "all", false, "Run all tasks")

	rootCmd.AddCommand(boxerCmd)
}

func runBoxer(_ *cobra.Command, _ []string) error {
	helpers.SetupTempDir()

	// Determine which tasks to run
	tasks := getSelectedTasks()
	slog.Info("Starting boxer", "tasks", tasks)

	servicename := "lakerunner-boxer"
	addlAttrs := attribute.NewSet(
		attribute.String("action", "boxer"),
		attribute.StringSlice("tasks", tasks),
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

	// Get main config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create database connection
	mdb, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open LRDB store: %w", err)
	}

	// Create and run boxer manager with selected tasks
	manager, err := metricsprocessing.NewBoxerManager(ctx, cfg, mdb, tasks)
	if err != nil {
		return fmt.Errorf("failed to create boxer manager: %w", err)
	}
	defer func() { _ = manager.Close() }()

	healthServer.SetStatus(healthcheck.StatusHealthy)

	if err := manager.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("shutting down", "error", err)
			return nil
		}
		return err
	}
	return nil
}

func getSelectedTasks() []string {
	var tasks []string
	if ingestLogs {
		tasks = append(tasks, config.BoxerTaskIngestLogs)
	}
	if ingestMetrics {
		tasks = append(tasks, config.BoxerTaskIngestMetrics)
	}
	if ingestTraces {
		tasks = append(tasks, config.BoxerTaskIngestTraces)
	}
	if compactLogs {
		tasks = append(tasks, config.BoxerTaskCompactLogs)
	}
	if compactMetrics {
		tasks = append(tasks, config.BoxerTaskCompactMetrics)
	}
	if compactTraces {
		tasks = append(tasks, config.BoxerTaskCompactTraces)
	}
	if rollupMetrics {
		tasks = append(tasks, config.BoxerTaskRollupMetrics)
	}
	return tasks
}
