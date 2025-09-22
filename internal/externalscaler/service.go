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
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/logctx"
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
	scalingConfig *config.ScalingConfig
}

type Config struct {
	GRPCPort      int
	LagMonitor    LagMonitorInterface
	ScalingConfig *config.ScalingConfig // Scaling configuration from main config
	TopicRegistry *config.TopicRegistry // Topic registry for service name lookup
}

func NewService(_ context.Context, cfg Config) (*Service, error) {
	if cfg.LagMonitor == nil {
		return nil, errors.New("lag monitor is required")
	}

	// Use scaling config if provided, otherwise use defaults
	scalingConfig := cfg.ScalingConfig
	if scalingConfig == nil {
		defaultScaling := config.GetDefaultScalingConfig()
		scalingConfig = &defaultScaling
	}

	s := &Service{
		grpcPort:      cfg.GRPCPort,
		healthCheck:   health.NewServer(),
		lagMonitor:    cfg.LagMonitor,
		scalingConfig: scalingConfig,
	}

	// Initialize Kafka metrics exporter if lag monitor is provided and OTEL is enabled
	exporter, err := NewKafkaMetricsExporter(cfg.LagMonitor, cfg.TopicRegistry)
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
	ll := logctx.FromContext(ctx)

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

	ll.Info("Starting gRPC server", "port", s.grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	return nil
}

func (s *Service) IsActive(ctx context.Context, req *ScaledObjectRef) (*IsActiveResponse, error) {
	ll := logctx.FromContext(ctx)

	serviceType, exists := req.ScalerMetadata["serviceType"]
	if !exists {
		ll.Warn("serviceType not specified in scaler metadata",
			"name", req.Name,
			"namespace", req.Namespace)
		return &IsActiveResponse{Result: false}, nil
	}

	if serviceType == config.ServiceTypeBoxer {
		return s.isBoxerActive(ctx, req.ScalerMetadata)
	}

	_, err := s.lagMonitor.GetQueueDepth(serviceType)
	return &IsActiveResponse{Result: err == nil}, nil
}

// isBoxerActive checks if any of the boxer task queues are active
func (s *Service) isBoxerActive(ctx context.Context, metadata map[string]string) (*IsActiveResponse, error) {
	ll := logctx.FromContext(ctx)

	boxerTasks, exists := metadata["boxerTasks"]
	if !exists {
		ll.Warn("boxerTasks not specified for boxer service")
		return &IsActiveResponse{Result: false}, nil
	}

	tasks := strings.Split(boxerTasks, ",")
	for _, task := range tasks {
		task = strings.TrimSpace(task)
		if task == "" {
			continue
		}

		taskServiceType := config.GetBoxerServiceType(task)
		_, err := s.lagMonitor.GetQueueDepth(taskServiceType)
		if err == nil {
			// At least one task queue is active
			return &IsActiveResponse{Result: true}, nil
		}
	}

	// No task queues are active
	return &IsActiveResponse{Result: false}, nil
}

func (s *Service) StreamIsActive(req *ScaledObjectRef, stream grpc.ServerStreamingServer[IsActiveResponse]) error {
	return status.Errorf(codes.Unimplemented, "StreamIsActive is not implemented")
}

func (s *Service) GetMetricSpec(ctx context.Context, req *ScaledObjectRef) (*GetMetricSpecResponse, error) {
	serviceType, exists := req.ScalerMetadata["serviceType"]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "serviceType not specified in scaler metadata")
	}

	if serviceType == config.ServiceTypeBoxer {
		return s.getBoxerMetricSpecs(ctx, req.ScalerMetadata)
	}

	target, err := s.scalingConfig.GetTargetQueueSize(serviceType)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to get target queue size: %v", err)
	}

	return &GetMetricSpecResponse{
		MetricSpecs: []*MetricSpec{
			{
				MetricName:      serviceType,
				TargetSizeFloat: float64(target),
			},
		},
	}, nil
}

// getBoxerMetricSpecs returns multiple metric specs for boxer tasks
func (s *Service) getBoxerMetricSpecs(ctx context.Context, metadata map[string]string) (*GetMetricSpecResponse, error) {
	ll := logctx.FromContext(ctx)

	boxerTasks, exists := metadata["boxerTasks"]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "boxerTasks not specified for boxer service")
	}

	tasks := strings.Split(boxerTasks, ",")
	var metricSpecs []*MetricSpec

	for _, task := range tasks {
		task = strings.TrimSpace(task)
		if task == "" {
			continue
		}

		taskServiceType := config.GetBoxerServiceType(task)

		target, err := s.scalingConfig.GetTargetQueueSize(taskServiceType)
		if err != nil {
			ll.Warn("Failed to get target queue size for boxer task",
				"task", task, "serviceType", taskServiceType, "error", err)
			target = s.scalingConfig.DefaultTarget
			if target <= 0 {
				return nil, status.Errorf(codes.InvalidArgument,
					"failed to get target queue size for task %s and no valid default target available", task)
			}
		}

		metricSpecs = append(metricSpecs, &MetricSpec{
			MetricName:      taskServiceType,
			TargetSizeFloat: float64(target),
		})
	}

	return &GetMetricSpecResponse{
		MetricSpecs: metricSpecs,
	}, nil
}

func (s *Service) GetMetrics(ctx context.Context, req *GetMetricsRequest) (*GetMetricsResponse, error) {
	ll := logctx.FromContext(ctx)

	serviceType := req.MetricName
	depth, err := s.lagMonitor.GetQueueDepth(serviceType)
	if err != nil {
		ll.Error("Failed to get queue depth", "serviceType", serviceType, "metricName", req.MetricName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get queue depth for %s: %v", serviceType, err)
	}

	return &GetMetricsResponse{
		MetricValues: []*MetricValue{
			{
				MetricName:       req.MetricName,
				MetricValueFloat: float64(depth),
			},
		},
	}, nil
}
