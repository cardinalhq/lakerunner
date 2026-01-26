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

package queryapi

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// EcsClientInterface defines the ECS client methods needed for worker discovery
type EcsClientInterface interface {
	ListTasks(ctx context.Context, params *ecs.ListTasksInput, optFns ...func(*ecs.Options)) (*ecs.ListTasksOutput, error)
	DescribeTasks(ctx context.Context, params *ecs.DescribeTasksInput, optFns ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error)
}

type EcsWorkerDiscovery struct {
	BaseWorkerDiscovery

	// config
	serviceName string
	clusterName string
	workerPort  int
	interval    time.Duration

	// clients
	ecsClient EcsClientInterface

	// state
	cancelFunc context.CancelFunc
}

var _ WorkerDiscovery = (*EcsWorkerDiscovery)(nil)

type EcsWorkerDiscoveryConfig struct {
	ServiceName string        // ECS service name for workers
	ClusterName string        // ECS cluster name
	WorkerPort  int           // Port workers listen on (default 8081)
	Interval    time.Duration // Polling interval (default 10s)
}

func NewEcsWorkerDiscovery(cfg EcsWorkerDiscoveryConfig) (*EcsWorkerDiscovery, error) {
	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	ecsClient := ecs.NewFromConfig(awsCfg)
	return NewEcsWorkerDiscoveryWithClient(cfg, ecsClient)
}

func NewEcsWorkerDiscoveryWithClient(cfg EcsWorkerDiscoveryConfig, ecsClient EcsClientInterface) (*EcsWorkerDiscovery, error) {
	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("ServiceName is required")
	}
	if cfg.ClusterName == "" {
		return nil, fmt.Errorf("ClusterName is required")
	}
	if cfg.WorkerPort == 0 {
		cfg.WorkerPort = 8081
	}
	if cfg.Interval == 0 {
		cfg.Interval = 10 * time.Second
	}

	return &EcsWorkerDiscovery{
		serviceName: cfg.ServiceName,
		clusterName: cfg.ClusterName,
		workerPort:  cfg.WorkerPort,
		interval:    cfg.Interval,
		ecsClient:   ecsClient,
	}, nil
}

func (e *EcsWorkerDiscovery) Start(ctx context.Context) error {
	if e.IsRunning() {
		return fmt.Errorf("ECS worker discovery is already running")
	}
	e.SetRunning(true)

	slog.Info("Starting ECS worker discovery",
		"service", e.serviceName,
		"cluster", e.clusterName,
		"port", e.workerPort,
		"interval", e.interval)

	runCtx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel

	// Initial discovery
	if err := e.discoverWorkers(runCtx); err != nil {
		slog.Error("Initial ECS worker discovery failed", slog.Any("error", err))
	}

	// Start periodic discovery
	ticker := time.NewTicker(e.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if err := e.discoverWorkers(runCtx); err != nil {
					slog.Error("ECS worker discovery failed", slog.Any("error", err))
				}
			}
		}
	}()

	return nil
}

func (e *EcsWorkerDiscovery) Stop() error {
	if !e.IsRunning() {
		return nil
	}
	e.SetRunning(false)
	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
	}
	return nil
}

func (e *EcsWorkerDiscovery) discoverWorkers(ctx context.Context) error {
	// Collect all task ARNs across paginated ListTasks calls
	var allTaskArns []string
	var nextToken *string

	for {
		listInput := &ecs.ListTasksInput{
			Cluster:     aws.String(e.clusterName),
			ServiceName: aws.String(e.serviceName),
			NextToken:   nextToken,
		}

		listResp, err := e.ecsClient.ListTasks(ctx, listInput)
		if err != nil {
			return fmt.Errorf("failed to list ECS tasks: %w", err)
		}

		allTaskArns = append(allTaskArns, listResp.TaskArns...)

		// Check if there are more pages
		if listResp.NextToken == nil {
			break
		}
		nextToken = listResp.NextToken
	}

	if len(allTaskArns) == 0 {
		e.updateWorkers(nil)
		return nil
	}

	// DescribeTasks has a limit of 100 tasks per call, so batch the requests
	var allTasks []types.Task
	batchSize := 100

	for i := 0; i < len(allTaskArns); i += batchSize {
		end := min(i+batchSize, len(allTaskArns))

		batch := allTaskArns[i:end]
		descResp, err := e.ecsClient.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(e.clusterName),
			Tasks:   batch,
		})
		if err != nil {
			return fmt.Errorf("failed to describe ECS tasks batch %d-%d: %w", i, end-1, err)
		}

		allTasks = append(allTasks, descResp.Tasks...)
	}

	workers := e.extractWorkers(allTasks)
	e.updateWorkers(workers)

	return nil
}

func (e *EcsWorkerDiscovery) extractWorkers(tasks []types.Task) []Worker {
	var workers []Worker

	for _, task := range tasks {
		// Only include running tasks
		if task.LastStatus == nil || *task.LastStatus != "RUNNING" {
			continue
		}

		// Extract IP from ENI attachments
		for _, attachment := range task.Attachments {
			if attachment.Type == nil || *attachment.Type != "ElasticNetworkInterface" {
				continue
			}

			for _, detail := range attachment.Details {
				if detail.Name != nil && *detail.Name == "privateIPv4Address" && detail.Value != nil {
					worker := Worker{
						IP:   *detail.Value,
						Port: e.workerPort,
					}
					workers = append(workers, worker)
					break
				}
			}
		}
	}

	// Sort workers for consistent ordering
	sort.Slice(workers, func(i, j int) bool {
		if workers[i].IP == workers[j].IP {
			return workers[i].Port < workers[j].Port
		}
		return workers[i].IP < workers[j].IP
	})

	return workers
}

func (e *EcsWorkerDiscovery) updateWorkers(newWorkers []Worker) {
	currentWorkers := e.GetWorkers()

	// Check if workers changed
	changed := len(newWorkers) != len(currentWorkers)
	if !changed {
		for i, w := range newWorkers {
			if i >= len(currentWorkers) || currentWorkers[i] != w {
				changed = true
				break
			}
		}
	}

	if changed {
		e.SetWorkers(newWorkers)
		slog.Info("ECS worker snapshot updated",
			"service", e.serviceName,
			"cluster", e.clusterName,
			"totalWorkers", len(newWorkers))
	}
}
