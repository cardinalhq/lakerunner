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

func TestService_IsActive(t *testing.T) {
	mockMonitor := new(MockLagMonitor)
	service := &Service{
		lagMonitor: mockMonitor,
	}

	// Setup mock expectations for known service types
	knownServices := []string{
		"ingest-logs", "ingest-metrics", "ingest-traces",
		"compact-logs", "compact-traces", "rollup-metrics",
	}
	for _, svc := range knownServices {
		mockMonitor.On("GetQueueDepth", svc).Return(int64(5), nil)
	}
	// Unknown services return error
	mockMonitor.On("GetQueueDepth", "unknown-service").Return(int64(0), fmt.Errorf("unknown service type"))
	mockMonitor.On("GetQueueDepth", "").Return(int64(0), fmt.Errorf("empty service type"))

	tests := []struct {
		name           string
		serviceType    string
		expectedResult bool
	}{
		{"ingest-logs", "ingest-logs", true},
		{"ingest-metrics", "ingest-metrics", true},
		{"ingest-traces", "ingest-traces", true},
		{"compact-logs", "compact-logs", true},
		{"compact-traces", "compact-traces", true},
		{"rollup-metrics", "rollup-metrics", true},
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

func TestService_GetMetricSpec(t *testing.T) {
	scalingConfig := &config.ScalingConfig{
		DefaultTarget:      100,
		IngestLogs:         config.ServiceScaling{TargetQueueSize: 100},
		RollupMetrics:      config.ServiceScaling{TargetQueueSize: 100},
		BoxerRollupMetrics: config.ServiceScaling{TargetQueueSize: 1500},
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
		{"ingest-logs", "ingest-logs", "ingest-logs", 100.0, false},
		{"rollup-metrics", "rollup-metrics", "rollup-metrics", 100.0, false},
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
		name          string
		serviceType   string
		mockSetup     func(*MockLagMonitor)
		expectedValue float64
		expectError   bool
	}{
		{
			name:        "ingest-logs success",
			serviceType: "ingest-logs",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "ingest-logs").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-metrics success",
			serviceType: "ingest-metrics",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "ingest-metrics").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-traces success",
			serviceType: "ingest-traces",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "ingest-traces").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "rollup-metrics success",
			serviceType: "rollup-metrics",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "rollup-metrics").Return(int64(5), nil)
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "compact-logs success",
			serviceType: "compact-logs",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "compact-logs").Return(int64(3), nil)
			},
			expectedValue: 3.0, // Fixed value from service
			expectError:   false,
		},
		{
			name:        "compact-traces success",
			serviceType: "compact-traces",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "compact-traces").Return(int64(3), nil)
			},
			expectedValue: 3.0,
			expectError:   false,
		},
		// Note: ingest-* services return constant values now (no database errors possible)
		{
			name:        "unsupported service type",
			serviceType: "unknown-service",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "unknown-service").Return(int64(0), fmt.Errorf("unknown service type"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMonitor := new(MockLagMonitor)
			tt.mockSetup(mockMonitor)

			service := &Service{
				lagMonitor: mockMonitor,
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
				mockMonitor.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			require.Len(t, resp.MetricValues, 1)
			assert.Equal(t, req.MetricName, resp.MetricValues[0].MetricName)
			assert.Equal(t, tt.expectedValue, resp.MetricValues[0].MetricValueFloat)
			mockMonitor.AssertExpectations(t)
		})
	}
}

func TestService_getQueueDepth(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		mockSetup     func(*MockLagMonitor)
		expectedCount int64
		expectError   bool
	}{
		{
			name:        "ingest-logs",
			serviceType: "ingest-logs",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "ingest-logs").Return(int64(5), nil)
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "rollup-metrics",
			serviceType: "rollup-metrics",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "rollup-metrics").Return(int64(5), nil)
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "unsupported service type",
			serviceType: "invalid-service",
			mockSetup: func(m *MockLagMonitor) {
				m.On("GetQueueDepth", "invalid-service").Return(int64(0), fmt.Errorf("unknown service type"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMonitor := new(MockLagMonitor)
			tt.mockSetup(mockMonitor)

			service := &Service{
				lagMonitor: mockMonitor,
			}

			count, err := service.getQueueDepth(context.Background(), tt.serviceType)

			if tt.expectError {
				require.Error(t, err)
				mockMonitor.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			mockMonitor.AssertExpectations(t)
		})
	}
}
