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
//   - unset or other values: Returns an error
func CreateWorkerDiscovery() (WorkerDiscovery, error) {
	execEnv := os.Getenv("EXECUTION_ENVIRONMENT")

	switch execEnv {
	case "local":
		return NewLocalDevDiscovery(), nil

	case "kubernetes":
		return createKubernetesWorkerDiscovery()

	case "":
		return nil, fmt.Errorf("EXECUTION_ENVIRONMENT environment variable is required (must be 'local' or 'kubernetes')")

	default:
		return nil, fmt.Errorf("unsupported EXECUTION_ENVIRONMENT: %s (must be 'local' or 'kubernetes')", execEnv)
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

func IsLocalDev() bool {
	return os.Getenv("EXECUTION_ENVIRONMENT") == "local"
}
