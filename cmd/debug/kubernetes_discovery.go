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

package debug

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/promql"
)

var kubernetesDiscoveryCmd = &cobra.Command{
	Use:   "kubernetes-discovery",
	Short: "Debug Kubernetes worker discovery",
	Long:  `Debug the Kubernetes worker discovery system by showing what workers are found.`,
	RunE:  runKubernetesDiscovery,
}

var (
	podNamespace        string
	workerLabelSelector string
	queryWorkerPort     int
	organizationIDStr   string
	segmentIDs          []string
	testSegments        string
)

func init() {
	kubernetesDiscoveryCmd.Flags().StringVar(&podNamespace, "pod-namespace", "", "POD_NAMESPACE: Kubernetes namespace to search in (required)")
	kubernetesDiscoveryCmd.Flags().StringVar(&workerLabelSelector, "worker-pod-label-selector", "", "WORKER_POD_LABEL_SELECTOR: Label selector for worker pods (required)")
	kubernetesDiscoveryCmd.Flags().IntVar(&queryWorkerPort, "query-worker-port", 0, "QUERY_WORKER_PORT: Port to use for worker connections (required)")
	kubernetesDiscoveryCmd.Flags().StringVar(&organizationIDStr, "organization-id", "550e8400-e29b-41d4-a716-446655440000", "Organization ID (UUID) for segment mapping")
	kubernetesDiscoveryCmd.Flags().StringVar(&testSegments, "test-segments", "tbl_1,tbl_2,tbl_3,tbl_4,tbl_5", "Comma-separated list of segment IDs to test mapping")

	kubernetesDiscoveryCmd.MarkFlagRequired("pod-namespace")
	kubernetesDiscoveryCmd.MarkFlagRequired("worker-pod-label-selector")
	kubernetesDiscoveryCmd.MarkFlagRequired("query-worker-port")
}

func runKubernetesDiscovery(cmd *cobra.Command, args []string) error {
	fmt.Printf("Debugging Kubernetes worker discovery...\n")
	fmt.Printf("POD_NAMESPACE: %s\n", podNamespace)
	fmt.Printf("WORKER_POD_LABEL_SELECTOR: %s\n", workerLabelSelector)
	fmt.Printf("QUERY_WORKER_PORT: %d\n", queryWorkerPort)
	fmt.Printf("Organization ID: %s\n", organizationIDStr)
	fmt.Printf("Test Segments: %s\n", testSegments)
	fmt.Printf("\n")

	// Parse organization ID
	organizationID, err := uuid.Parse(organizationIDStr)
	if err != nil {
		return fmt.Errorf("invalid organization ID UUID: %w", err)
	}

	// Parse segment IDs
	var segmentIDs []string
	if testSegments != "" {
		parts := strings.Split(testSegments, ",")
		for _, part := range parts {
			segmentIDs = append(segmentIDs, strings.TrimSpace(part))
		}
	}

	config := promql.KubernetesWorkerDiscoveryConfig{
		Namespace:           podNamespace,
		WorkerLabelSelector: workerLabelSelector,
		WorkerPort:          queryWorkerPort,
	}

	discovery, err := promql.NewKubernetesWorkerDiscovery(config)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes worker discovery: %w", err)
	}

	// Start the discovery service
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Starting worker discovery service...\n")
	if err := discovery.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker discovery: %w", err)
	}
	defer discovery.Stop()
	fmt.Printf("✅ Worker discovery service started successfully\n")

	// Give the informers a moment to settle
	fmt.Printf("Waiting for initial worker discovery...\n")
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("✅ Ready to test\n")

	// Get all workers
	fmt.Printf("Discovering workers...\n")
	workers, err := discovery.GetAllWorkers()
	if err != nil {
		return fmt.Errorf("failed to get workers: %w", err)
	}

	fmt.Printf("Found %d workers:\n", len(workers))
	for i, worker := range workers {
		fmt.Printf("  [%d] %s:%d\n", i+1, worker.IP, worker.Port)
	}

	if len(workers) == 0 {
		fmt.Printf("\nNo workers found. Check:\n")
		fmt.Printf("- Label selector matches worker pods: %s\n", workerLabelSelector)
		fmt.Printf("- Namespace contains the worker pods: %s\n", podNamespace)
		fmt.Printf("- Worker pods are in Ready state\n")
		fmt.Printf("- EndpointSlices exist for the service\n")
		return nil
	}

	// Test segment mapping
	if len(segmentIDs) > 0 {
		fmt.Printf("\nTesting segment-to-worker mapping:\n")
		mappings, err := discovery.GetWorkersForSegments(organizationID, segmentIDs)
		if err != nil {
			return fmt.Errorf("failed to get segment mappings: %w", err)
		}

		for _, mapping := range mappings {
			fmt.Printf("  Segment %s -> %s:%d\n", mapping.SegmentID, mapping.Worker.IP, mapping.Worker.Port)
		}

		// Test consistency - same segments should map to same workers
		fmt.Printf("\nTesting consistency (same mapping should be returned):\n")
		mappings2, err := discovery.GetWorkersForSegments(organizationID, segmentIDs)
		if err != nil {
			return fmt.Errorf("failed to get segment mappings for consistency test: %w", err)
		}

		consistent := true
		for i, mapping := range mappings {
			if mapping.Worker.IP != mappings2[i].Worker.IP {
				fmt.Printf("  ❌ Segment %s: %s != %s (INCONSISTENT)\n",
					mapping.SegmentID, mapping.Worker.IP, mappings2[i].Worker.IP)
				consistent = false
			} else {
				fmt.Printf("  ✅ Segment %s: %s (consistent)\n", mapping.SegmentID, mapping.Worker.IP)
			}
		}

		if consistent {
			fmt.Printf("\n✅ All segment mappings are consistent!\n")
		} else {
			fmt.Printf("\n❌ Some segment mappings are inconsistent!\n")
		}
	}

	return nil
}

// GetKubernetesDiscoveryCmd returns the kubernetes-discovery command for registration
func GetKubernetesDiscoveryCmd() *cobra.Command {
	return kubernetesDiscoveryCmd
}
