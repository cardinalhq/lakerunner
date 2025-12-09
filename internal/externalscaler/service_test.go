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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
)

// MockLagMonitor is a mock implementation of fly.ConsumerLagMonitor
type MockLagMonitor struct {
	mock.Mock
}

func (m *MockLagMonitor) GetQueueDepth(serviceType string) (int64, error) {
	args := m.Called(serviceType)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockLagMonitor) GetLastUpdate() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

func (m *MockLagMonitor) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockLagMonitor) GetDetailedMetrics() []fly.ConsumerGroupInfo {
	args := m.Called()
	if result := args.Get(0); result != nil {
		return result.([]fly.ConsumerGroupInfo)
	}
	return nil
}

func (m *MockLagMonitor) GetServiceMappings() []interface{} {
	return nil
}

func (m *MockLagMonitor) GetLastError() error {
	return nil
}

func (m *MockLagMonitor) Start(ctx context.Context) {
	// No-op for tests
}

// MockQueueDepthMonitor is a mock implementation of QueueDepthMonitorInterface
type MockQueueDepthMonitor struct {
	mock.Mock
}

func (m *MockQueueDepthMonitor) GetQueueDepth(taskName string) (int64, error) {
	args := m.Called(taskName)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockQueueDepthMonitor) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestService_IsActive(t *testing.T) {
	mockLagMonitor := new(MockLagMonitor)
	mockQueueDepthMonitor := new(MockQueueDepthMonitor)
	service := &Service{
		lagMonitor:        mockLagMonitor,
		queueDepthMonitor: mockQueueDepthMonitor,
	}

	// Setup mock expectations for worker service types (use queueDepthMonitor)
	// worker-ingest-logs -> calls queueDepthMonitor.GetQueueDepth("ingest-logs")
	mockQueueDepthMonitor.On("GetQueueDepth", "ingest-logs").Return(int64(5), nil)
	mockQueueDepthMonitor.On("GetQueueDepth", "ingest-metrics").Return(int64(5), nil)
	mockQueueDepthMonitor.On("GetQueueDepth", "ingest-traces").Return(int64(5), nil)
	mockQueueDepthMonitor.On("GetQueueDepth", "compact-logs").Return(int64(5), nil)
	mockQueueDepthMonitor.On("GetQueueDepth", "compact-traces").Return(int64(5), nil)
	mockQueueDepthMonitor.On("GetQueueDepth", "rollup-metrics").Return(int64(5), nil)

	// Unknown services use lagMonitor (non-worker prefix)
	mockLagMonitor.On("GetQueueDepth", "unknown-service").Return(int64(0), fmt.Errorf("unknown service type"))

	tests := []struct {
		name           string
		serviceType    string
		expectedResult bool
	}{
		{"worker-ingest-logs", "worker-ingest-logs", true},
		{"worker-ingest-metrics", "worker-ingest-metrics", true},
		{"worker-ingest-traces", "worker-ingest-traces", true},
		{"worker-compact-logs", "worker-compact-logs", true},
		{"worker-compact-traces", "worker-compact-traces", true},
		{"worker-rollup-metrics", "worker-rollup-metrics", true},
		{"unknown-service", "unknown-service", false},
		{"empty-service", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &ScaledObjectRef{
				Name:      "test-object",
				Namespace: "test-namespace",
				ScalerMetadata: map[string]string{
					"serviceType": tt.serviceType,
				},
			}

			if tt.serviceType == "" {
				req.ScalerMetadata = map[string]string{}
			}

			resp, err := service.IsActive(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, resp.Result)
		})
	}
}

func TestService_IsActive_Boxer(t *testing.T) {
	mockLagMonitor := new(MockLagMonitor)
	mockQueueDepthMonitor := new(MockQueueDepthMonitor)
	service := &Service{
		lagMonitor:        mockLagMonitor,
		queueDepthMonitor: mockQueueDepthMonitor,
	}

	tests := []struct {
		name           string
		boxerTasks     string
		mockSetup      func(*MockLagMonitor)
		expectedResult bool
	}{
		{
			name:       "boxer with active task queue",
			boxerTasks: config.BoxerTaskCompactLogs,
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactLogs).Return(int64(5), nil)
			},
			expectedResult: true,
		},
		{
			name:       "boxer with inactive task queue",
			boxerTasks: config.BoxerTaskCompactLogs,
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactLogs).Return(int64(0), fmt.Errorf("queue not found"))
			},
			expectedResult: false,
		},
		{
			name:       "boxer with multiple tasks, one active",
			boxerTasks: config.BoxerTaskCompactLogs + "," + config.BoxerTaskCompactMetrics,
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactLogs).Return(int64(0), fmt.Errorf("queue not found"))
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactMetrics).Return(int64(3), nil)
			},
			expectedResult: true,
		},
		{
			name:       "boxer with multiple tasks, all inactive",
			boxerTasks: config.BoxerTaskCompactLogs + "," + config.BoxerTaskCompactMetrics,
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactLogs).Return(int64(0), fmt.Errorf("queue not found"))
				m.On("GetQueueDepth", config.ServiceTypeBoxerCompactMetrics).Return(int64(0), fmt.Errorf("queue not found"))
			},
			expectedResult: false,
		},
		{
			name:           "boxer without boxerTasks metadata",
			boxerTasks:     "",
			mockSetup:      func(m *MockLagMonitor) {}, // No expectations
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset mock for each test
			mockLagMonitor.ExpectedCalls = nil
			mockLagMonitor.Calls = nil
			tt.mockSetup(mockLagMonitor)

			req := &ScaledObjectRef{
				Name:      "test-boxer",
				Namespace: "test-namespace",
				ScalerMetadata: map[string]string{
					"serviceType": config.ServiceTypeBoxer,
				},
			}

			if tt.boxerTasks != "" {
				req.ScalerMetadata["boxerTasks"] = tt.boxerTasks
			}

			resp, err := service.IsActive(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, resp.Result)

			mockLagMonitor.AssertExpectations(t)
		})
	}
}

func TestService_GetMetricSpec(t *testing.T) {
	scalingConfig := &config.ScalingConfig{
		DefaultTarget:       100,
		WorkerIngestLogs:    config.ServiceScaling{TargetQueueSize: 100},
		WorkerRollupMetrics: config.ServiceScaling{TargetQueueSize: 100},
		BoxerRollupMetrics:  config.ServiceScaling{TargetQueueSize: 1500},
	}

	service := &Service{
		scalingConfig: scalingConfig,
	}

	tests := []struct {
		name               string
		serviceType        string
		expectedMetricName string
		expectedTarget     float64
		expectError        bool
	}{
		{"worker-ingest-logs", "worker-ingest-logs", "worker-ingest-logs", 100.0, false},
		{"worker-rollup-metrics", "worker-rollup-metrics", "worker-rollup-metrics", 100.0, false},
		{"boxer-rollup-metrics", "boxer-rollup-metrics", "boxer-rollup-metrics", 1500.0, false},
		{"missing-service-type", "", "", 0, true},
		{"unknown-service-type", "unknown-service", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &ScaledObjectRef{
				Name:           "test-object",
				Namespace:      "test-namespace",
				ScalerMetadata: map[string]string{},
			}

			if tt.serviceType != "" {
				req.ScalerMetadata["serviceType"] = tt.serviceType
			}

			resp, err := service.GetMetricSpec(context.Background(), req)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, resp.MetricSpecs, 1)
			assert.Equal(t, tt.expectedMetricName, resp.MetricSpecs[0].MetricName)
			assert.Equal(t, tt.expectedTarget, resp.MetricSpecs[0].TargetSizeFloat)
		})
	}
}

func TestService_GetMetrics(t *testing.T) {
	tests := []struct {
		name                string
		serviceType         string
		mockLagSetup        func(*MockLagMonitor)
		mockQueueDepthSetup func(*MockQueueDepthMonitor)
		expectedValue       float64
		expectError         bool
	}{
		{
			name:        "ingest-logs success",
			serviceType: "worker-ingest-logs",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "ingest-logs").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-metrics success",
			serviceType: "worker-ingest-metrics",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "ingest-metrics").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-traces success",
			serviceType: "worker-ingest-traces",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "ingest-traces").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "rollup-metrics success",
			serviceType: "worker-rollup-metrics",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "rollup-metrics").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "compact-logs success",
			serviceType: "worker-compact-logs",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "compact-logs").Return(int64(3), nil)
			},
			expectedValue: 3.0,
			expectError:   false,
		},
		{
			name:        "compact-traces success",
			serviceType: "worker-compact-traces",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "compact-traces").Return(int64(3), nil)
			},
			expectedValue: 3.0,
			expectError:   false,
		},
		{
			name:        "unsupported service type",
			serviceType: "unknown-service",
			mockLagSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "unknown-service").Return(int64(0), fmt.Errorf("unknown service type"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLagMonitor := new(MockLagMonitor)
			mockQueueDepthMonitor := new(MockQueueDepthMonitor)

			if tt.mockLagSetup != nil {
				tt.mockLagSetup(mockLagMonitor)
			}
			if tt.mockQueueDepthSetup != nil {
				tt.mockQueueDepthSetup(mockQueueDepthMonitor)
			}

			service := &Service{
				lagMonitor:        mockLagMonitor,
				queueDepthMonitor: mockQueueDepthMonitor,
			}

			req := &GetMetricsRequest{
				ScaledObjectRef: &ScaledObjectRef{
					Name:      "test-object",
					Namespace: "test-namespace",
					ScalerMetadata: map[string]string{
						"serviceType": tt.serviceType,
					},
				},
				MetricName: tt.serviceType,
			}

			resp, err := service.GetMetrics(context.Background(), req)

			if tt.expectError {
				require.Error(t, err)
				mockLagMonitor.AssertExpectations(t)
				mockQueueDepthMonitor.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			require.Len(t, resp.MetricValues, 1)
			assert.Equal(t, req.MetricName, resp.MetricValues[0].MetricName)
			assert.Equal(t, tt.expectedValue, resp.MetricValues[0].MetricValueFloat)
			mockLagMonitor.AssertExpectations(t)
			mockQueueDepthMonitor.AssertExpectations(t)
		})
	}
}

func TestService_getQueueDepth(t *testing.T) {
	tests := []struct {
		name                string
		serviceType         string
		mockLagSetup        func(*MockLagMonitor)
		mockQueueDepthSetup func(*MockQueueDepthMonitor)
		expectedCount       int64
		expectError         bool
	}{
		{
			name:        "worker-ingest-logs",
			serviceType: "worker-ingest-logs",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "ingest-logs").Return(int64(5), nil)
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "worker-rollup-metrics",
			serviceType: "worker-rollup-metrics",
			mockQueueDepthSetup: func(m *MockQueueDepthMonitor) {
				m.On("GetQueueDepth", "rollup-metrics").Return(int64(5), nil)
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "unsupported service type",
			serviceType: "invalid-service",
			mockLagSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "invalid-service").Return(int64(0), fmt.Errorf("unknown service type"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLagMonitor := new(MockLagMonitor)
			mockQueueDepthMonitor := new(MockQueueDepthMonitor)

			if tt.mockLagSetup != nil {
				tt.mockLagSetup(mockLagMonitor)
			}
			if tt.mockQueueDepthSetup != nil {
				tt.mockQueueDepthSetup(mockQueueDepthMonitor)
			}

			service := &Service{
				lagMonitor:        mockLagMonitor,
				queueDepthMonitor: mockQueueDepthMonitor,
			}

			count, err := service.getQueueDepth(context.Background(), tt.serviceType)

			if tt.expectError {
				require.Error(t, err)
				mockLagMonitor.AssertExpectations(t)
				mockQueueDepthMonitor.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			mockLagMonitor.AssertExpectations(t)
			mockQueueDepthMonitor.AssertExpectations(t)
		})
	}
}
