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

package queryapi

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type WorkerDiscovery interface {
	Start(ctx context.Context) error
	Stop() error
	GetWorkersForSegments(organizationID uuid.UUID, segmentIDs []int64) ([]SegmentWorkerMapping, error)
	GetAllWorkers() ([]Worker, error)
}

// CreateWorkerDiscovery creates the appropriate WorkerDiscovery implementation
// based on the EXECUTION_ENVIRONMENT environment variable.
//
// Supported values:
//   - "local": Creates LocalDevDiscovery for local development
//   - "kubernetes": Creates KubernetesWorkerDiscovery for Kubernetes environments
//   - "ecs": Creates EcsWorkerDiscovery for ECS environments
//   - unset or other values: Returns an error
func CreateWorkerDiscovery() (WorkerDiscovery, error) {
	execEnv := os.Getenv("EXECUTION_ENVIRONMENT")

	switch execEnv {
	case "local":
		return NewLocalDevDiscovery(), nil

	case "kubernetes":
		return createKubernetesWorkerDiscovery()

	case "ecs":
		return createEcsWorkerDiscovery()

	case "":
		return nil, fmt.Errorf("EXECUTION_ENVIRONMENT environment variable is required (must be 'local', 'kubernetes', or 'ecs')")

	default:
		return nil, fmt.Errorf("unsupported EXECUTION_ENVIRONMENT: %s (must be 'local', 'kubernetes', or 'ecs')", execEnv)
	}
}

// createKubernetesWorkerDiscovery creates a KubernetesWorkerDiscovery with required configuration
func createKubernetesWorkerDiscovery() (WorkerDiscovery, error) {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		return nil, fmt.Errorf("POD_NAMESPACE environment variable is required for kubernetes execution environment")
	}

	workerLabelSelector := os.Getenv("WORKER_POD_LABEL_SELECTOR")
	if workerLabelSelector == "" {
		return nil, fmt.Errorf("WORKER_POD_LABEL_SELECTOR environment variable is required for kubernetes execution environment")
	}

	workerPortStr := os.Getenv("QUERY_WORKER_PORT")
	if workerPortStr == "" {
		return nil, fmt.Errorf("QUERY_WORKER_PORT environment variable is required for kubernetes execution environment")
	}

	workerPort, err := strconv.Atoi(workerPortStr)
	if err != nil {
		return nil, fmt.Errorf("invalid QUERY_WORKER_PORT: %w", err)
	}

	config := KubernetesWorkerDiscoveryConfig{
		Namespace:           namespace,
		WorkerLabelSelector: workerLabelSelector,
		WorkerPort:          workerPort,
	}

	return NewKubernetesWorkerDiscovery(config)
}

// createEcsWorkerDiscovery creates an EcsWorkerDiscovery with required configuration
func createEcsWorkerDiscovery() (WorkerDiscovery, error) {
	serviceName := getFirstEnv([]string{"QUERY_WORKER_SERVICE_NAME", "ECS_WORKER_SERVICE_NAME"})
	if serviceName == "" {
		return nil, fmt.Errorf("QUERY_WORKER_SERVICE_NAME or ECS_WORKER_SERVICE_NAME environment variable is required for ECS execution environment")
	}

	clusterName := getFirstEnv([]string{"QUERY_WORKER_CLUSTER_NAME", "ECS_WORKER_CLUSTER_NAME"})
	if clusterName == "" {
		return nil, fmt.Errorf("QUERY_WORKER_CLUSTER_NAME or ECS_WORKER_CLUSTER_NAME environment variable is required for ECS execution environment")
	}

	workerPortStr := os.Getenv("QUERY_WORKER_PORT")
	if workerPortStr == "" {
		workerPortStr = "8081" // default for workers
	}

	workerPort, err := strconv.Atoi(workerPortStr)
	if err != nil {
		return nil, fmt.Errorf("invalid QUERY_WORKER_PORT: %w", err)
	}

	intervalSecsStr := os.Getenv("WORKER_POLL_INTERVAL_SECONDS")
	if intervalSecsStr == "" {
		intervalSecsStr = "10" // default
	}

	intervalSecs, err := strconv.Atoi(intervalSecsStr)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_POLL_INTERVAL_SECONDS: %w", err)
	}

	config := EcsWorkerDiscoveryConfig{
		ServiceName: serviceName,
		ClusterName: clusterName,
		WorkerPort:  workerPort,
		Interval:    time.Duration(intervalSecs) * time.Second,
	}

	return NewEcsWorkerDiscovery(config)
}

// getFirstEnv returns the value of the first environment variable that is set
func getFirstEnv(keys []string) string {
	for _, key := range keys {
		if val := os.Getenv(key); val != "" {
			return val
		}
	}
	return ""
}
