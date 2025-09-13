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
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// QueriesInterface defines the methods needed for scaling queries
type QueriesInterface interface {
	// No methods needed - external scaler will use fixed values for now
}

type Service struct {
	UnimplementedExternalScalerServer
	grpcPort    int
	healthCheck *health.Server
}

type Config struct {
	GRPCPort int
}

func NewService(ctx context.Context, cfg Config) (*Service, error) {
	return &Service{
		grpcPort:    cfg.GRPCPort,
		healthCheck: health.NewServer(),
	}, nil
}

func (s *Service) Close() {
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
		// TODO: Implement proper segment-based scaling metrics when compaction system is redesigned
		// For now, return a fixed value to keep the scaler working
		result, err = int64(3), nil
	case "compact-traces":
		// TODO: Implement proper segment-based scaling metrics when compaction system is redesigned
		// For now, return a fixed value to keep the scaler working
		result, err = int64(3), nil
	case "rollup-metrics":
		// TODO: Implement proper metric rollup queue scaling when needed
		// For now, return a fixed value of 5
		return 5, nil
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
