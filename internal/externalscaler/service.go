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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

// LagMonitorInterface defines the methods needed for Kafka lag monitoring
type LagMonitorInterface interface {
	GetDetailedMetrics() []fly.ConsumerGroupInfo
	GetQueueDepth(serviceType string) (int64, error)
	IsHealthy() bool
}

type Service struct {
	UnimplementedExternalScalerServer
	grpcPort      int
	healthCheck   *health.Server
	lagMonitor    LagMonitorInterface
	kafkaExporter *KafkaMetricsExporter
}

type Config struct {
	GRPCPort   int
	LagMonitor LagMonitorInterface
}

func NewService(_ context.Context, cfg Config) (*Service, error) {
	if cfg.LagMonitor == nil {
		return nil, errors.New("lag monitor is required")
	}

	s := &Service{
		grpcPort:    cfg.GRPCPort,
		healthCheck: health.NewServer(),
		lagMonitor:  cfg.LagMonitor,
	}

	// Initialize Kafka metrics exporter if lag monitor is provided and OTEL is enabled
	exporter, err := NewKafkaMetricsExporter(cfg.LagMonitor)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka metrics exporter: %w", err)
	}
	s.kafkaExporter = exporter

	return s, nil
}

func (s *Service) Close() {}

func (s *Service) getQueueDepth(_ context.Context, serviceType string) (int64, error) {
	depth, err := s.lagMonitor.GetQueueDepth(serviceType)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue depth for %s: %w", serviceType, err)
	}

	return depth, nil
}

func (s *Service) Start(ctx context.Context) error {
	return s.startGRPCServer(ctx)
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

	_, err := s.lagMonitor.GetQueueDepth(serviceType)
	return &IsActiveResponse{Result: err == nil}, nil
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
