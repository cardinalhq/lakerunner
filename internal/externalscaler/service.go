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

package externalscaler

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// QueriesInterface defines the methods needed for scaling queries
type QueriesInterface interface {
	WorkQueueScalingDepth(ctx context.Context, arg lrdb.WorkQueueScalingDepthParams) (interface{}, error)
	MetricRollupQueueScalingDepth(ctx context.Context) (interface{}, error)
}

type Service struct {
	UnimplementedExternalScalerServer
	port        int
	grpcPort    int
	healthCheck *health.Server
	dbPool      *pgxpool.Pool
	queries     QueriesInterface
}

type Config struct {
	Port     int
	GRPCPort int
}

func NewService(ctx context.Context, cfg Config) (*Service, error) {
	dbPool, err := dbopen.ConnectTolrdb(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to LRDB: %w", err)
	}

	return &Service{
		port:        cfg.Port,
		grpcPort:    cfg.GRPCPort,
		healthCheck: health.NewServer(),
		dbPool:      dbPool,
		queries:     lrdb.New(dbPool),
	}, nil
}

func (s *Service) Close() {
	if s.dbPool != nil {
		s.dbPool.Close()
	}
}

func (s *Service) getQueueDepth(ctx context.Context, serviceType string) (int64, error) {
	var result any
	var err error

	switch serviceType {
	case "ingest-logs":
		// TODO: Replace with Kafka consumer lag metrics for ingestion scaling
		result, err = int64(5), nil
	case "ingest-metrics":
		// TODO: Replace with Kafka consumer lag metrics for ingestion scaling
		result, err = int64(5), nil
	case "ingest-traces":
		// TODO: Replace with Kafka consumer lag metrics for ingestion scaling
		result, err = int64(5), nil
	case "compact-logs":
		result, err = s.queries.WorkQueueScalingDepth(ctx, lrdb.WorkQueueScalingDepthParams{
			Signal: lrdb.SignalEnumLogs,
			Action: lrdb.ActionEnumCompact,
		})
	case "compact-traces":
		result, err = s.queries.WorkQueueScalingDepth(ctx, lrdb.WorkQueueScalingDepthParams{
			Signal: lrdb.SignalEnumTraces,
			Action: lrdb.ActionEnumCompact,
		})
	case "rollup-metrics":
		result, err = s.queries.MetricRollupQueueScalingDepth(ctx)
	default:
		return 0, fmt.Errorf("unsupported service type: %s", serviceType)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to execute queue depth query for %s: %w", serviceType, err)
	}

	count, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected result type for %s: %T", serviceType, result)
	}

	slog.Debug("Queue depth query result",
		"serviceType", serviceType,
		"count", count)

	return count, nil
}

func (s *Service) Start(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- s.startHTTPServer(ctx)
	}()

	go func() {
		errCh <- s.startGRPCServer(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) startHTTPServer(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","timestamp":"%s","service":"external-scaler"}`, time.Now().Format(time.RFC3339))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ready","timestamp":"%s","service":"external-scaler"}`, time.Now().Format(time.RFC3339))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"alive","timestamp":"%s","service":"external-scaler"}`, time.Now().Format(time.RFC3339))
	})

	server := &http.Server{
		Addr:    ":" + strconv.Itoa(s.port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
	}()

	slog.Info("Starting HTTP server", "port", s.port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server failed: %w", err)
	}

	return nil
}

func (s *Service) startGRPCServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(s.grpcPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.grpcPort, err)
	}

	grpcServer := grpc.NewServer()

	RegisterExternalScalerServer(grpcServer, s)
	grpc_health_v1.RegisterHealthServer(grpcServer, s.healthCheck)

	s.healthCheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info("Starting gRPC server", "port", s.grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	return nil
}

func (s *Service) IsActive(ctx context.Context, req *ScaledObjectRef) (*IsActiveResponse, error) {
	slog.Debug("IsActive called",
		"name", req.Name,
		"namespace", req.Namespace,
		"metadata", req.ScalerMetadata)

	serviceType, exists := req.ScalerMetadata["serviceType"]
	if !exists {
		slog.Warn("serviceType not specified in scaler metadata",
			"name", req.Name,
			"namespace", req.Namespace)
		return &IsActiveResponse{Result: false}, nil
	}

	switch serviceType {
	case "compact-logs", "compact-traces", "rollup-metrics":
		return &IsActiveResponse{Result: true}, nil
	case "ingest-logs", "ingest-metrics", "ingest-traces":
		return &IsActiveResponse{Result: true}, nil
	default:
		slog.Warn("unknown service type", "serviceType", serviceType)
		return &IsActiveResponse{Result: false}, nil
	}
}

func (s *Service) StreamIsActive(req *ScaledObjectRef, stream grpc.ServerStreamingServer[IsActiveResponse]) error {
	return status.Errorf(codes.Unimplemented, "StreamIsActive is not implemented")
}

func (s *Service) GetMetricSpec(ctx context.Context, req *ScaledObjectRef) (*GetMetricSpecResponse, error) {
	slog.Debug("GetMetricSpec called",
		"name", req.Name,
		"namespace", req.Namespace,
		"metadata", req.ScalerMetadata)

	serviceType, exists := req.ScalerMetadata["serviceType"]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "serviceType not specified in scaler metadata")
	}

	metricName := fmt.Sprintf("%s-queue-depth", serviceType)

	return &GetMetricSpecResponse{
		MetricSpecs: []*MetricSpec{
			{
				MetricName:      metricName,
				TargetSizeFloat: 10.0, // Target 10 items in queue for scaling
			},
		},
	}, nil
}

func (s *Service) GetMetrics(ctx context.Context, req *GetMetricsRequest) (*GetMetricsResponse, error) {
	slog.Debug("GetMetrics called",
		"name", req.ScaledObjectRef.Name,
		"namespace", req.ScaledObjectRef.Namespace,
		"metricName", req.MetricName,
		"metadata", req.ScaledObjectRef.ScalerMetadata)

	serviceType, exists := req.ScaledObjectRef.ScalerMetadata["serviceType"]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "serviceType not specified in scaler metadata")
	}

	count, err := s.getQueueDepth(ctx, serviceType)
	if err != nil {
		slog.Error("Failed to get queue depth", "serviceType", serviceType, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get queue depth for %s: %v", serviceType, err)
	}

	metricValue := float64(count)

	return &GetMetricsResponse{
		MetricValues: []*MetricValue{
			{
				MetricName:       req.MetricName,
				MetricValueFloat: metricValue,
			},
		},
	}, nil
}
