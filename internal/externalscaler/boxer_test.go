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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/config"
)

func TestService_GetMetrics_Boxer(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		metricName    string
		taskDepths    map[string]int64
		expectedValue float64
		expectedError bool
		errorContains string
	}{
		{
			name:          "boxer task metric",
			serviceType:   "boxer",
			metricName:    "boxer-compact-logs-queue-depth",
			taskDepths:    map[string]int64{"boxer-compact-logs": 100},
			expectedValue: 100,
		},
		{
			name:          "boxer task metric with different value",
			serviceType:   "boxer",
			metricName:    "boxer-compact-metrics-queue-depth",
			taskDepths:    map[string]int64{"boxer-compact-metrics": 250},
			expectedValue: 250,
		},
		{
			name:          "boxer task not found should error",
			serviceType:   "boxer",
			metricName:    "boxer-unknown-task-queue-depth",
			taskDepths:    map[string]int64{},
			expectedError: true,
			errorContains: "failed to get queue depth for boxer-unknown-task",
		},
		{
			name:          "invalid boxer metric name format should error",
			serviceType:   "boxer",
			metricName:    "invalid-metric-name",
			expectedError: true,
			errorContains: "invalid boxer metric name format",
		},
		{
			name:          "non-boxer service should use original logic",
			serviceType:   "ingest-logs",
			metricName:    "ingest-logs-queue-depth",
			taskDepths:    map[string]int64{"ingest-logs": 150},
			expectedValue: 150,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock lag monitor
			mockLagMonitor := &MockLagMonitor{}

			// Set up expectations for lookups
			for taskServiceType, depth := range tt.taskDepths {
				mockLagMonitor.On("GetQueueDepth", taskServiceType).Return(depth, nil)
			}

			// For error cases with unknown tasks
			if tt.expectedError && strings.Contains(tt.errorContains, "failed to get queue depth") {
				// Extract service type from metric name for error case
				if strings.HasSuffix(tt.metricName, "-queue-depth") {
					taskServiceType := strings.TrimSuffix(tt.metricName, "-queue-depth")
					mockLagMonitor.On("GetQueueDepth", taskServiceType).Return(int64(0), assert.AnError)
				}
			}

			// Create service with mock
			service := &Service{
				lagMonitor: mockLagMonitor,
			}

			// Create request
			req := &GetMetricsRequest{
				MetricName: tt.metricName,
				ScaledObjectRef: &ScaledObjectRef{
					Name:      "test-scaler",
					Namespace: "test-namespace",
					ScalerMetadata: map[string]string{
						"serviceType": tt.serviceType,
					},
				},
			}

			// Execute
			resp, err := service.GetMetrics(context.Background(), req)

			// Assert results
			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Len(t, resp.MetricValues, 1)
				assert.Equal(t, tt.metricName, resp.MetricValues[0].MetricName)
				assert.Equal(t, tt.expectedValue, resp.MetricValues[0].MetricValueFloat)
			}

			mockLagMonitor.AssertExpectations(t)
		})
	}
}

func TestService_GetMetricSpec_Boxer(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		boxerTasks    string
		taskTargets   map[string]int
		defaultTarget int
		expectedSpecs []struct {
			name   string
			target float64
		}
		expectedError bool
		errorContains string
	}{
		{
			name:        "boxer without boxerTasks should error",
			serviceType: "boxer",
			// boxerTasks not provided
			expectedError: true,
			errorContains: "boxerTasks not specified for boxer service",
		},
		{
			name:        "boxer with single task",
			serviceType: "boxer",
			boxerTasks:  "compact-logs",
			taskTargets: map[string]int{
				"boxer-compact-logs": 10,
			},
			expectedSpecs: []struct {
				name   string
				target float64
			}{
				{"boxer-compact-logs-queue-depth", 10},
			},
		},
		{
			name:        "boxer with multiple tasks",
			serviceType: "boxer",
			boxerTasks:  "compact-logs,compact-metrics",
			taskTargets: map[string]int{
				"boxer-compact-logs":    10,
				"boxer-compact-metrics": 20,
			},
			expectedSpecs: []struct {
				name   string
				target float64
			}{
				{"boxer-compact-logs-queue-depth", 10},
				{"boxer-compact-metrics-queue-depth", 20},
			},
		},
		{
			name:        "boxer with unknown task falls back to default",
			serviceType: "boxer",
			boxerTasks:  "compact-logs,unknown-task",
			taskTargets: map[string]int{
				"boxer-compact-logs": 10,
				// unknown-task will fall back to DefaultTarget
			},
			defaultTarget: 20,
			expectedSpecs: []struct {
				name   string
				target float64
			}{
				{"boxer-compact-logs-queue-depth", 10},
				{"boxer-unknown-task-queue-depth", 20},
			},
		},
		{
			name:        "non-boxer service should use original logic",
			serviceType: "ingest-logs",
			taskTargets: map[string]int{
				"ingest-logs": 25,
			},
			expectedSpecs: []struct {
				name   string
				target float64
			}{
				{"ingest-logs-queue-depth", 25},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create real scaling config with test values
			scalingConfig := &config.ScalingConfig{
				DefaultTarget: tt.defaultTarget,
			}

			// Set up the specific fields based on test requirements
			for taskServiceType, target := range tt.taskTargets {
				scaling := config.ServiceScaling{TargetQueueSize: target}
				switch taskServiceType {
				case "boxer-compact-logs":
					scalingConfig.BoxerCompactLogs = scaling
				case "boxer-compact-metrics":
					scalingConfig.BoxerCompactMetrics = scaling
				case "boxer-compact-traces":
					scalingConfig.BoxerCompactTraces = scaling
				case "boxer-rollup-metrics":
					scalingConfig.BoxerRollupMetrics = scaling
				case "ingest-logs":
					scalingConfig.IngestLogs = scaling
				case "compact-logs":
					scalingConfig.CompactLogs = scaling
				case "compact-metrics":
					scalingConfig.CompactMetrics = scaling
				case "rollup-metrics":
					scalingConfig.RollupMetrics = scaling
				}
			}

			// Create service with real config
			service := &Service{
				scalingConfig: scalingConfig,
			}

			// Create request
			req := &ScaledObjectRef{
				Name:      "test-scaler",
				Namespace: "test-namespace",
				ScalerMetadata: map[string]string{
					"serviceType": tt.serviceType,
				},
			}

			// Add boxerTasks if provided
			if tt.boxerTasks != "" {
				req.ScalerMetadata["boxerTasks"] = tt.boxerTasks
			}

			// Execute
			resp, err := service.GetMetricSpec(context.Background(), req)

			// Assert results
			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Len(t, resp.MetricSpecs, len(tt.expectedSpecs))

				// Check each expected metric spec
				for i, expectedSpec := range tt.expectedSpecs {
					assert.Equal(t, expectedSpec.name, resp.MetricSpecs[i].MetricName)
					assert.Equal(t, expectedSpec.target, resp.MetricSpecs[i].TargetSizeFloat)
				}
			}

		})
	}
}
