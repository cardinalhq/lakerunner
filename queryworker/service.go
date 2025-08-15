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
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/queryproto"
)

type Service struct {
	queryproto.UnimplementedQueryWorkerServer
	port        int
	grpcPort    int
	cache       *ParquetCache
	healthCheck *health.Server
}

type Config struct {
	Port             int
	GRPCPort         int
	CacheDirectory   string
	CacheMaxSizeMB   int64
	EnableLocalCache bool
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

	// Create health check server
	healthCheck := health.NewServer()

	return &Service{
		port:        config.Port,
		grpcPort:    config.GRPCPort,
		cache:       cache,
		healthCheck: healthCheck,
	}, nil
}

func (s *Service) Run(doneCtx context.Context) error {
	// Always run GRPC for main functionality
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.runGRPC(doneCtx)
	}()

	// Always run HTTP server for /healthz endpoint only
	httpErrCh := make(chan error, 1)
	go func() {
		httpErrCh <- s.runHealthHTTP(doneCtx)
	}()

	// Wait for either service to fail or context to be done
	select {
	case <-doneCtx.Done():
		slog.Info("Shutting down query worker service")
		if err := s.cache.Close(); err != nil {
			slog.Error("Failed to close cache", slog.Any("error", err))
		}
		return nil
	case err := <-grpcErrCh:
		if cacheErr := s.cache.Close(); cacheErr != nil {
			slog.Error("Failed to close cache", slog.Any("error", cacheErr))
		}
		return fmt.Errorf("GRPC server error: %w", err)
	case err := <-httpErrCh:
		if cacheErr := s.cache.Close(); cacheErr != nil {
			slog.Error("Failed to close cache", slog.Any("error", cacheErr))
		}
		return fmt.Errorf("HTTP health server error: %w", err)
	}
}

func (s *Service) runGRPC(doneCtx context.Context) error {
	slog.Info("Starting query worker GRPC service", "port", s.grpcPort)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	queryproto.RegisterQueryWorkerServer(grpcServer, s)
	
	// Register the standard GRPC health check service
	grpc_health_v1.RegisterHealthServer(grpcServer, s.healthCheck)
	
	// Set initial health status for the query worker service
	s.healthCheck.SetServingStatus("queryworker.QueryWorker", grpc_health_v1.HealthCheckResponse_SERVING)
	s.healthCheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING) // Overall health

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("Failed to start GRPC server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down query worker GRPC service")
	
	// Mark services as not serving before shutdown
	s.healthCheck.SetServingStatus("queryworker.QueryWorker", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.healthCheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	
	grpcServer.GracefulStop()

	return nil
}

func (s *Service) runHealthHTTP(doneCtx context.Context) error {
	slog.Info("Starting query worker HTTP health service", "port", s.port)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP health server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down query worker HTTP health service")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Failed to shutdown HTTP health server", slog.Any("error", err))
		return fmt.Errorf("failed to shutdown HTTP health server: %w", err)
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

	grpcPortStr := os.Getenv("QUERY_WORKER_GRPC_PORT")
	if grpcPortStr == "" {
		grpcPortStr = "9090"
	}
	grpcPort, err := strconv.Atoi(grpcPortStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid QUERY_WORKER_GRPC_PORT: %w", err)
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
		Port:             port,
		GRPCPort:         grpcPort,
		CacheDirectory:   cacheDir,
		CacheMaxSizeMB:   cacheSize,
		EnableLocalCache: enableCache,
	}, nil
}

// UpdateHealthStatus updates the GRPC health check status
func (s *Service) UpdateHealthStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	s.healthCheck.SetServingStatus(service, status)
}

// IsHealthy returns true if the overall service is healthy
func (s *Service) IsHealthy() bool {
	// This could be enhanced to check actual service health
	// For now, we check if the cache is available
	return s.cache != nil
}
