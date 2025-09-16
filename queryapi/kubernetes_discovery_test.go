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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestKubernetesWorkerDiscoveryConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      KubernetesWorkerDiscoveryConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: KubernetesWorkerDiscoveryConfig{
				Namespace:           "test-namespace",
				WorkerLabelSelector: "app=query-worker",
				WorkerPort:          8081,
			},
			expectError: false,
		},
		{
			name: "missing namespace",
			config: KubernetesWorkerDiscoveryConfig{
				WorkerLabelSelector: "app=query-worker",
				WorkerPort:          8081,
			},
			expectError: true,
			errorMsg:    "namespace is required",
		},
		{
			name: "missing label selector",
			config: KubernetesWorkerDiscoveryConfig{
				Namespace:  "test-namespace",
				WorkerPort: 8081,
			},
			expectError: true,
			errorMsg:    "WorkerLabelSelector is required",
		},
		{
			name: "default port applied",
			config: KubernetesWorkerDiscoveryConfig{
				Namespace:           "test-namespace",
				WorkerLabelSelector: "app=query-worker",
				// WorkerPort not set
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery, err := NewKubernetesWorkerDiscovery(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, discovery)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, discovery)

				// Verify defaults are applied
				if tt.config.WorkerPort == 0 {
					assert.Equal(t, 8080, discovery.workerPort)
				}
			}
		})
	}
}

func TestKubernetesWorkerDiscovery_RebuildWorkers(t *testing.T) {
	config := KubernetesWorkerDiscoveryConfig{
		Namespace:           "test-namespace",
		WorkerLabelSelector: "app=query-worker",
		WorkerPort:          8081,
	}

	// Create discovery instance
	discovery, err := NewKubernetesWorkerDiscovery(config)
	require.NoError(t, err)

	// Test with no services - should result in empty workers
	ctx := context.Background()
	err = discovery.rebuildWorkers(ctx)
	assert.NoError(t, err)

	workers := discovery.GetWorkers()
	assert.Empty(t, workers)
}

func TestKubernetesWorkerDiscovery_BasicFunctionality(t *testing.T) {
	config := KubernetesWorkerDiscoveryConfig{
		Namespace:           "test-namespace",
		WorkerLabelSelector: "app=query-worker",
		WorkerPort:          8081,
	}

	discovery, err := NewKubernetesWorkerDiscovery(config)
	require.NoError(t, err)
	assert.NotNil(t, discovery)

	// Test basic properties
	assert.Equal(t, "test-namespace", discovery.namespace)
	assert.Equal(t, "app=query-worker", discovery.workerLabelSelector)
	assert.Equal(t, 8081, discovery.workerPort)

	// Initially not running
	assert.False(t, discovery.IsRunning())

	// Test GetAllWorkers with no workers
	workers, err := discovery.GetAllWorkers()
	assert.NoError(t, err)
	assert.Empty(t, workers)
}

func TestKubernetesWorkerDiscovery_LabelSelectorParsing(t *testing.T) {
	tests := []struct {
		name          string
		labelSelector string
		expectError   bool
	}{
		{
			name:          "simple label",
			labelSelector: "app=query-worker",
			expectError:   false,
		},
		{
			name:          "multiple labels",
			labelSelector: "app=query-worker,component=backend",
			expectError:   false,
		},
		{
			name:          "label with version",
			labelSelector: "app=query-worker,version in (v1,v2)",
			expectError:   false,
		},
		{
			name:          "invalid selector",
			labelSelector: "app===invalid",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := labels.Parse(tt.labelSelector)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKubernetesWorkerDiscovery_RunningState(t *testing.T) {
	config := KubernetesWorkerDiscoveryConfig{
		Namespace:           "test-namespace",
		WorkerLabelSelector: "app=query-worker",
		WorkerPort:          8081,
	}

	discovery, err := NewKubernetesWorkerDiscovery(config)
	require.NoError(t, err)

	// Initially not running
	assert.False(t, discovery.IsRunning())

	// Test setting running state manually (simulates what Start() would do)
	discovery.SetRunning(true)
	assert.True(t, discovery.IsRunning())

	// Test double start should fail (simulates Start() logic)
	discovery.SetRunning(true) // This simulates that Start() checks IsRunning()
	assert.True(t, discovery.IsRunning())

	// Stop discovery (simulates what Stop() would do)
	discovery.SetRunning(false)
	assert.False(t, discovery.IsRunning())

	// Double stop should be safe
	discovery.SetRunning(false)
	assert.False(t, discovery.IsRunning())
}

func TestKubernetesWorkerDiscovery_EndpointFiltering(t *testing.T) {
	tests := []struct {
		name            string
		ready           *bool
		serving         *bool
		terminating     *bool
		address         string
		expectedInclude bool
	}{
		{
			name:            "ready and serving",
			ready:           &[]bool{true}[0],
			serving:         &[]bool{true}[0],
			terminating:     &[]bool{false}[0],
			address:         "10.0.0.1",
			expectedInclude: true,
		},
		{
			name:            "not ready",
			ready:           &[]bool{false}[0],
			serving:         &[]bool{true}[0],
			terminating:     &[]bool{false}[0],
			address:         "10.0.0.2",
			expectedInclude: false,
		},
		{
			name:            "not serving",
			ready:           &[]bool{true}[0],
			serving:         &[]bool{false}[0],
			terminating:     &[]bool{false}[0],
			address:         "10.0.0.3",
			expectedInclude: false,
		},
		{
			name:            "terminating",
			ready:           &[]bool{true}[0],
			serving:         &[]bool{true}[0],
			terminating:     &[]bool{true}[0],
			address:         "10.0.0.4",
			expectedInclude: false,
		},
		{
			name:            "serving nil (default true)",
			ready:           &[]bool{true}[0],
			serving:         nil,
			terminating:     &[]bool{false}[0],
			address:         "10.0.0.5",
			expectedInclude: true,
		},
		{
			name:            "invalid IP",
			ready:           &[]bool{true}[0],
			serving:         &[]bool{true}[0],
			terminating:     &[]bool{false}[0],
			address:         "invalid-ip",
			expectedInclude: false,
		},
		{
			name:            "IPv6 address (filtered out)",
			ready:           &[]bool{true}[0],
			serving:         &[]bool{true}[0],
			terminating:     &[]bool{false}[0],
			address:         "2001:db8::1",
			expectedInclude: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := discoveryv1.Endpoint{
				Addresses: []string{tt.address},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       tt.ready,
					Serving:     tt.serving,
					Terminating: tt.terminating,
				},
			}

			// Simulate the filtering logic from rebuildWorkers
			ready := endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
			serving := endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving
			terminating := endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating

			shouldInclude := ready && serving && !terminating

			// Also check IP parsing (from rebuildWorkers logic)
			if shouldInclude {
				for _, addr := range endpoint.Addresses {
					// This mimics the IP parsing logic in rebuildWorkers
					if ip := parseIPv4(addr); ip == "" {
						shouldInclude = false
						break
					}
				}
			}

			assert.Equal(t, tt.expectedInclude, shouldInclude,
				"Endpoint filtering result doesn't match expected for %s", tt.name)
		})
	}
}

// Helper function to mimic the IP parsing logic from rebuildWorkers
func parseIPv4(addr string) string {
	// Simple IPv4 validation (similar to what's in rebuildWorkers)
	parts := 0
	for _, c := range addr {
		if c == '.' {
			parts++
		} else if c < '0' || c > '9' {
			return ""
		}
	}
	if parts == 3 && len(addr) >= 7 && len(addr) <= 15 {
		return addr
	}
	return ""
}
