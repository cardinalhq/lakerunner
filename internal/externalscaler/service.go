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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/config"
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

	if serviceType == "boxer" {
		return s.getBoxerMetricSpecs(req.ScalerMetadata)
	}

	// Single service metric spec
	metricName := fmt.Sprintf("%s-queue-depth", serviceType)
	target, err := s.scalingConfig.GetTargetQueueSize(serviceType)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to get target queue size: %v", err)
	}

	slog.Debug("Returning metric spec",
		"serviceType", serviceType,
		"metricName", metricName,
		"targetSize", target)

	return &GetMetricSpecResponse{
		MetricSpecs: []*MetricSpec{
			{
				MetricName:      metricName,
				TargetSizeFloat: float64(target),
			},
		},
	}, nil
}

// getBoxerMetricSpecs returns multiple metric specs for boxer tasks
func (s *Service) getBoxerMetricSpecs(metadata map[string]string) (*GetMetricSpecResponse, error) {
	boxerTasks, exists := metadata["boxerTasks"]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "boxerTasks not specified for boxer service")
	}

	tasks := strings.Split(boxerTasks, ",")
	var metricSpecs []*MetricSpec

	slog.Debug("Processing boxer tasks for metric specs", "tasks", tasks)

	for _, task := range tasks {
		task = strings.TrimSpace(task)
		if task == "" {
			continue
		}

		taskServiceType := fmt.Sprintf("boxer-%s", task)
		metricName := fmt.Sprintf("%s-queue-depth", taskServiceType)

		target, err := s.scalingConfig.GetTargetQueueSize(taskServiceType)
		if err != nil {
			slog.Warn("Failed to get target queue size for boxer task",
				"task", task, "serviceType", taskServiceType, "error", err)
			// Use default target if specific service type not found
			target = s.scalingConfig.DefaultTarget
			if target <= 0 {
				return nil, status.Errorf(codes.InvalidArgument,
					"failed to get target queue size for task %s and no valid default target available", task)
			}
			slog.Debug("Using default target for boxer task", "task", task, "defaultTarget", target)
		}

		metricSpecs = append(metricSpecs, &MetricSpec{
			MetricName:      metricName,
			TargetSizeFloat: float64(target),
		})

		slog.Debug("Added metric spec for boxer task",
			"task", task, "serviceType", taskServiceType, "metricName", metricName, "target", target)
	}

	slog.Debug("Returning boxer metric specs", "tasks", tasks, "metricCount", len(metricSpecs))
	return &GetMetricSpecResponse{
		MetricSpecs: metricSpecs,
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

	var metricValue float64
	var err error

	if serviceType == "boxer" {
		// For boxer, extract the task from the metric name and get its specific queue depth
		metricValue, err = s.getBoxerTaskMetricValue(ctx, req.MetricName)
	} else {
		// For non-boxer services, use existing logic
		metricValue, err = s.getSingleServiceMetricValue(ctx, serviceType)
	}

	if err != nil {
		return nil, err
	}

	return &GetMetricsResponse{
		MetricValues: []*MetricValue{
			{
				MetricName:       req.MetricName,
				MetricValueFloat: metricValue,
			},
		},
	}, nil
}

// getBoxerTaskMetricValue extracts the task from a boxer metric name and returns its queue depth
func (s *Service) getBoxerTaskMetricValue(_ context.Context, metricName string) (float64, error) {
	// Extract the task service type from metric name like "boxer-compact-logs-queue-depth"
	if !strings.HasSuffix(metricName, "-queue-depth") {
		return 0, status.Errorf(codes.InvalidArgument, "invalid boxer metric name format: %s", metricName)
	}

	// Remove the "-queue-depth" suffix to get the service type
	taskServiceType := strings.TrimSuffix(metricName, "-queue-depth")

	slog.Debug("Getting queue depth for boxer task", "taskServiceType", taskServiceType, "metricName", metricName)

	depth, err := s.lagMonitor.GetQueueDepth(taskServiceType)
	if err != nil {
		slog.Error("Failed to get queue depth for boxer task",
			"taskServiceType", taskServiceType, "metricName", metricName, "error", err)
		return 0, status.Errorf(codes.Internal, "failed to get queue depth for %s: %v", taskServiceType, err)
	}

	slog.Debug("Got queue depth for boxer task",
		"taskServiceType", taskServiceType, "metricName", metricName, "depth", depth)
	return float64(depth), nil
}

// getSingleServiceMetricValue gets the queue depth for a non-boxer service
func (s *Service) getSingleServiceMetricValue(ctx context.Context, serviceType string) (float64, error) {
	depth, err := s.getQueueDepth(ctx, serviceType)
	if err != nil {
		slog.Error("Failed to get queue depth", "serviceType", serviceType, "error", err)
		return 0, status.Errorf(codes.Internal, "failed to get queue depth for %s: %v", serviceType, err)
	}
	return float64(depth), nil
}
