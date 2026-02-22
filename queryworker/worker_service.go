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

package queryworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/queryworker/artifactspool"
	"github.com/cardinalhq/lakerunner/queryworker/controlstream"
	"github.com/cardinalhq/lakerunner/queryworker/workmanager"
)

// WorkerService wires CacheManager, artifact spool, and control stream.
type WorkerService struct {
	MetricsCM            *CacheManager
	LogsCM               *CacheManager
	TracesCM             *CacheManager
	StorageProfilePoller storageprofile.StorageProfileProvider
	MetricsGlobSize      int
	LogsGlobSize         int
	TracesGlobSize       int
	pool                 *duckdbx.DB       // shared pool for all queries
	parquetCache         *ParquetFileCache // shared parquet file cache

	// Control-stream components (set before Run).
	Spool               *artifactspool.Spool
	ControlStreamServer *controlstream.Server
	WorkMgr             *workmanager.Manager

	httpAddr string // testing override; defaults to ":8081"
}

func NewWorkerService(
	metricsGlobSize int,
	logsGlobSize int,
	tracesGlobSize int,
	maxParallelDownloads int,
	sp storageprofile.StorageProfileProvider,
	cloudManagers cloudstorage.ClientProvider,
	duckdbSettings duckdbx.DuckDBSettings,
) (*WorkerService, error) {
	// Create shared parquet file cache for downloaded files
	parquetCache, err := NewParquetFileCache(DefaultCleanupInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file cache: %w", err)
	}

	// Scan for existing files from previous runs
	if err := parquetCache.ScanExistingFiles(); err != nil {
		slog.Warn("Failed to scan existing parquet files", slog.Any("error", err))
	}

	// Register parquet cache metrics
	if err := parquetCache.RegisterMetrics(); err != nil {
		slog.Error("Failed to register parquet cache metrics", slog.Any("error", err))
	}

	downloader := func(ctx context.Context, profile storageprofile.StorageProfile, objectIDs []string) error {
		if len(objectIDs) == 0 {
			return nil
		}

		storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
		if err != nil {
			return fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
		}

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(maxParallelDownloads)

		region := profile.Region
		bucket := profile.Bucket

		for _, objectID := range objectIDs {
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				// Ask the cache where to put this file
				localPath, exists, err := parquetCache.GetOrPrepare(region, bucket, objectID)
				if err != nil {
					return fmt.Errorf("failed to prepare cache path for %s: %w", objectID, err)
				}

				if exists {
					slog.Debug("File already cached locally, skipping download",
						slog.String("objectID", objectID),
						slog.String("path", localPath))
					return nil
				}

				dir := filepath.Dir(localPath)

				// Download to temp file in target directory
				tmpfn, _, is404, err := storageClient.DownloadObject(gctx, dir, bucket, objectID)
				if err != nil {
					slog.Error("Failed to download object",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", bucket),
						slog.String("objectID", objectID),
						slog.Any("error", err))
					return err
				}
				if is404 {
					// Non-fatal skip
					slog.Info("Object not found, skipping",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", bucket),
						slog.String("objectID", objectID))
					return nil
				}

				// Atomically move tmp file into place as the final localPath.
				// Since tmp is in the same dir, os.Rename is atomic on POSIX.
				if err := os.Rename(tmpfn, localPath); err != nil {
					// Windows: need to remove existing file first
					_ = os.Remove(localPath)
					if err2 := os.Rename(tmpfn, localPath); err2 != nil {
						_ = os.Remove(tmpfn)
						return fmt.Errorf("rename %q -> %q: %w", tmpfn, localPath, err2)
					}
				}

				// Track the newly downloaded file
				if err := parquetCache.TrackFile(region, bucket, objectID); err != nil {
					slog.Error("Failed to track downloaded file - file exists but won't be managed by cache",
						slog.String("objectID", objectID),
						slog.String("path", localPath),
						slog.Any("error", err))
				}

				return nil
			})
		}

		// Wait for all downloads; return first error if any
		if err := g.Wait(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return fmt.Errorf("one or more downloads failed: %w", err)
		}
		return nil
	}

	// Create a single shared DB pool for all queries with metrics enabled
	pool, err := duckdbx.NewDB(
		duckdbx.WithMetrics(10*time.Second),
		duckdbx.WithConnectionMaxAge(30*time.Minute),
		duckdbx.WithDuckDBSettings(duckdbSettings),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared DB pool: %w", err)
	}

	metricsCM := NewCacheManager(downloader, "metrics", sp, pool, parquetCache)
	logsCM := NewCacheManager(downloader, "logs", sp, pool, parquetCache)
	tracesCM := NewCacheManager(downloader, "traces", sp, pool, parquetCache)

	return &WorkerService{
		MetricsCM:            metricsCM,
		LogsCM:               logsCM,
		TracesCM:             tracesCM,
		StorageProfilePoller: sp,
		MetricsGlobSize:      metricsGlobSize,
		LogsGlobSize:         logsGlobSize,
		TracesGlobSize:       tracesGlobSize,
		pool:                 pool,
		parquetCache:         parquetCache,
	}, nil
}

func (ws *WorkerService) Run(doneCtx context.Context) error {
	slog.Info("Starting query-worker service")

	mux := http.NewServeMux()

	// Register artifact download routes if spool is configured.
	if ws.Spool != nil {
		artifactspool.RegisterRoutes(mux, ws.Spool)
	}

	httpAddr := ws.httpAddr
	if httpAddr == "" {
		httpAddr = ":8081"
	}
	srv := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP server", slog.Any("error", err))
		}
	}()

	// Start control stream gRPC server if configured.
	// Use a separate context so sessions stay open during drain.
	var serverCancel context.CancelFunc
	if ws.ControlStreamServer != nil {
		var serverCtx context.Context
		serverCtx, serverCancel = context.WithCancel(context.Background())
		go func() {
			if err := ws.ControlStreamServer.Start(serverCtx); err != nil {
				slog.Error("Control stream server stopped", slog.Any("error", err))
			}
		}()
	}

	<-doneCtx.Done()

	slog.Info("Shutting down query-worker service")

	// Drain: stop accepting new work, wait for in-flight to complete.
	// Sessions stay open so workers can send completion messages.
	if ws.ControlStreamServer != nil {
		ws.ControlStreamServer.SetDraining(true)
	}
	if ws.WorkMgr != nil {
		ws.WorkMgr.SetDraining(true)
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 25*time.Second)
		if err := ws.WorkMgr.WaitForDrain(drainCtx); err != nil {
			slog.Warn("Work drain timed out", slog.Any("error", err))
		}
		drainCancel()
	}

	// Now close control stream sessions and stop gRPC server.
	if serverCancel != nil {
		serverCancel()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("Graceful shutdown timed out, forcing close", slog.Any("error", err))
		_ = srv.Close()
	}

	// Clean up resources
	ws.Close()

	return nil
}

func (ws *WorkerService) Close() {
	if ws.Spool != nil {
		ws.Spool.Stop()
	}
	if ws.MetricsCM != nil {
		ws.MetricsCM.Close()
	}
	if ws.LogsCM != nil {
		ws.LogsCM.Close()
	}
	if ws.TracesCM != nil {
		ws.TracesCM.Close()
	}
	if ws.parquetCache != nil {
		ws.parquetCache.Close()
	}
	if ws.pool != nil {
		_ = ws.pool.Close()
	}
}
