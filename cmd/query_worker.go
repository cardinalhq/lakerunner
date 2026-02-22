// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/queryworker"
	"github.com/cardinalhq/lakerunner/queryworker/artifactspool"
	"github.com/cardinalhq/lakerunner/queryworker/controlstream"
	"github.com/cardinalhq/lakerunner/queryworker/workmanager"
)

func init() {
	cmd := &cobra.Command{
		Use:   "query-worker",
		Short: "start query-worker service",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "query-worker"
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

			// Load config
			cfg, err := config.Load()
			if err != nil {
				slog.Warn("Failed to load config, using defaults", slog.Any("error", err))
				cfg = &config.Config{}
			}
			slog.Info("Query worker config loaded",
				slog.Int("maxParallelDownloads", cfg.Query.MaxParallelDownloads))

			go diskUsageLoop(ctx)

			go debugging.RunPprof(ctx)

			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			cdb, err := configdb.ConfigDBStore(ctx)
			if err != nil {
				slog.Error("Failed to connect to config database", slog.Any("error", err))
				return fmt.Errorf("failed to connect to config database: %w", err)
			}

			configservice.NewGlobal(cdb, 5*time.Minute)

			sp := storageprofile.NewStorageProfileProvider(cdb)

			cloudManagers, err := cloudstorage.NewCloudManagers(ctx)
			if err != nil {
				return fmt.Errorf("failed to create cloud managers: %w", err)
			}

			healthServer.SetStatus(healthcheck.StatusHealthy)

			duckdbSettings := duckdbx.DuckDBSettings{
				MemoryLimitMB:        cfg.DuckDB.GetMemoryLimit(),
				TempDirectory:        cfg.DuckDB.GetTempDirectory(),
				MaxTempDirectorySize: cfg.DuckDB.GetMaxTempDirectorySize(),
				PoolSize:             cfg.DuckDB.GetPoolSize(),
				Threads:              cfg.DuckDB.GetThreads(),
			}
			worker, err := queryworker.NewWorkerService(5, 5, 5, cfg.Query.MaxParallelDownloads, sp, cloudManagers, duckdbSettings)
			if err != nil {
				return fmt.Errorf("failed to create worker service: %w", err)
			}

			// --- Control-stream runtime ---

			// Artifact spool for local Parquet results.
			spool, err := artifactspool.NewSpool("/tmp/lakerunner-artifacts")
			if err != nil {
				return fmt.Errorf("failed to create artifact spool: %w", err)
			}
			spool.Start()
			worker.Spool = spool

			// Determine advertised HTTP address for artifact URLs.
			httpAddr := workerHTTPAddr()

			// Executor bridges PlanQuery + EvaluatePushDownToFile → ArtifactResult.
			executor := queryworker.NewWorkExecutor(worker, spool, httpAddr)

			// Work manager with concurrency control.
			workMgr := workmanager.NewManager(executor, 4, spool.Acknowledge)
			worker.WorkMgr = workMgr

			// Worker identity — stable across reconnects.
			workerID := workerIdentity()

			// gRPC control stream server.
			csServer := controlstream.NewServer(controlstream.Config{
				WorkerID: workerID,
				Handler:  workMgr,
			})
			worker.ControlStreamServer = csServer

			healthServer.SetReady(true)

			slog.Info("Worker runtime initialized",
				slog.String("worker_id", workerID),
				slog.String("http_addr", httpAddr))

			if err := worker.Run(ctx); err != nil {
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

// workerIdentity returns a stable worker ID, preferring POD_NAME, then HOSTNAME env,
// then os.Hostname().
func workerIdentity() string {
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	if h != "" {
		return h
	}
	return "query-worker"
}

// workerHTTPAddr returns the advertised host:port for artifact HTTP fetches.
// Uses WORKER_HTTP_ADDR env if set, otherwise POD_IP:8081, otherwise hostname:8081.
func workerHTTPAddr() string {
	if v := os.Getenv("WORKER_HTTP_ADDR"); v != "" {
		return v
	}
	if v := os.Getenv("POD_IP"); v != "" {
		return v + ":8081"
	}
	h, _ := os.Hostname()
	if h != "" {
		return h + ":8081"
	}
	return "localhost:8081"
}
