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

package queryworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
)

type Service struct {
	port  int
	cache *ParquetCache
}

type Config struct {
	Port               int
	CacheDirectory     string
	CacheMaxSizeMB     int64
	EnableLocalCache   bool
}

func NewService() (*Service, error) {
	config, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize AWS manager
	awsManager, err := awsclient.NewManager(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	// Initialize storage profile provider
	storageProfiler, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		return nil, fmt.Errorf("failed to setup storage profiles: %w", err)
	}

	cache, err := NewParquetCache(ParquetCacheConfig{
		CacheDir:        config.CacheDirectory,
		MaxSizeMB:       config.CacheMaxSizeMB,
		AWSManager:      awsManager,
		StorageProfiler: storageProfiler,
		EnableCache:     config.EnableLocalCache,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &Service{
		port:  config.Port,
		cache: cache,
	}, nil
}

func (s *Service) Run(doneCtx context.Context) error {
	slog.Info("Starting query worker service", "port", s.port)

	mux := http.NewServeMux()
	mux.HandleFunc("/pushdown", s.handlePushdown)
	mux.HandleFunc("/health", s.handleHealth)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down query worker service")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Failed to shutdown HTTP server", slog.Any("error", err))
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	if err := s.cache.Close(); err != nil {
		slog.Error("Failed to close cache", slog.Any("error", err))
	}

	return nil
}

func loadConfig() (Config, error) {
	portStr := os.Getenv("QUERY_WORKER_PORT")
	if portStr == "" {
		portStr = "8080"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid QUERY_WORKER_PORT: %w", err)
	}

	cacheDir := os.Getenv("CACHE_DIRECTORY")
	if cacheDir == "" {
		cacheDir = "/tmp/query-worker-cache"
	}

	cacheSizeStr := os.Getenv("CACHE_MAX_SIZE_MB")
	if cacheSizeStr == "" {
		cacheSizeStr = "1024" // 1GB default
	}
	cacheSize, err := strconv.ParseInt(cacheSizeStr, 10, 64)
	if err != nil {
		return Config{}, fmt.Errorf("invalid CACHE_MAX_SIZE_MB: %w", err)
	}

	enableCacheStr := os.Getenv("ENABLE_LOCAL_CACHE")
	enableCache := enableCacheStr != "false" // default to true

	return Config{
		Port:               port,
		CacheDirectory:     cacheDir,
		CacheMaxSizeMB:     cacheSize,
		EnableLocalCache:   enableCache,
	}, nil
}