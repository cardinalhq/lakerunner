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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockQueries is a mock implementation of QueriesInterface
type MockQueries struct {
	mock.Mock
}

// No methods needed for mock - external scaler uses fixed values

func TestService_IsActive(t *testing.T) {
	service := &Service{}

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
	service := &Service{}

	tests := []struct {
		name               string
		serviceType        string
		expectedMetricName string
		expectError        bool
	}{
		{"ingest-logs", "ingest-logs", "ingest-logs-queue-depth", false},
		{"rollup-metrics", "rollup-metrics", "rollup-metrics-queue-depth", false},
		{"missing-service-type", "", "", true},
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
			assert.Equal(t, float64(10.0), resp.MetricSpecs[0].TargetSizeFloat)
		})
	}
}

func TestService_GetMetrics(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		mockSetup     func(*MockQueries)
		expectedValue float64
		expectError   bool
	}{
		{
			name:        "ingest-logs success",
			serviceType: "ingest-logs",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns constant
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-metrics success",
			serviceType: "ingest-metrics",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns constant
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "ingest-traces success",
			serviceType: "ingest-traces",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns constant
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "rollup-metrics success",
			serviceType: "rollup-metrics",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns fixed value
			},
			expectedValue: 5.0,
			expectError:   false,
		},
		{
			name:        "compact-logs success",
			serviceType: "compact-logs",
			mockSetup: func(m *MockQueries) {
				// No mock setup needed - uses fixed values
			},
			expectedValue: 3.0, // Fixed value from service
			expectError:   false,
		},
		{
			name:        "compact-traces success",
			serviceType: "compact-traces",
			mockSetup: func(m *MockQueries) {
				// No mock setup needed - uses fixed values
			},
			expectedValue: 3.0, // Fixed value from service
			expectError:   false,
		},
		// Note: ingest-* services return constant values now (no database errors possible)
		{
			name:        "unsupported service type",
			serviceType: "unknown-service",
			mockSetup:   func(m *MockQueries) {},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueries := &MockQueries{}
			tt.mockSetup(mockQueries)

			service := &Service{}

			req := &GetMetricsRequest{
				ScaledObjectRef: &ScaledObjectRef{
					Name:      "test-object",
					Namespace: "test-namespace",
					ScalerMetadata: map[string]string{
						"serviceType": tt.serviceType,
					},
				},
				MetricName: tt.serviceType + "-queue-depth",
			}

			resp, err := service.GetMetrics(context.Background(), req)

			if tt.expectError {
				require.Error(t, err)
				mockQueries.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			require.Len(t, resp.MetricValues, 1)
			assert.Equal(t, req.MetricName, resp.MetricValues[0].MetricName)
			assert.Equal(t, tt.expectedValue, resp.MetricValues[0].MetricValueFloat)
			mockQueries.AssertExpectations(t)
		})
	}
}

func TestService_GetMetrics_MissingServiceType(t *testing.T) {
	service := &Service{}

	req := &GetMetricsRequest{
		ScaledObjectRef: &ScaledObjectRef{
			Name:           "test-object",
			Namespace:      "test-namespace",
			ScalerMetadata: map[string]string{}, // No serviceType
		},
		MetricName: "test-metric",
	}

	_, err := service.GetMetrics(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "serviceType not specified")
}

func TestService_getQueueDepth(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		mockSetup     func(*MockQueries)
		expectedCount int64
		expectError   bool
	}{
		{
			name:        "ingest-logs",
			serviceType: "ingest-logs",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns constant
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "rollup-metrics",
			serviceType: "rollup-metrics",
			mockSetup: func(m *MockQueries) {
				// No mock needed - returns fixed value
			},
			expectedCount: 5,
			expectError:   false,
		},
		{
			name:        "unsupported service type",
			serviceType: "invalid-service",
			mockSetup:   func(m *MockQueries) {},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueries := &MockQueries{}
			tt.mockSetup(mockQueries)

			service := &Service{}

			count, err := service.getQueueDepth(context.Background(), tt.serviceType)

			if tt.expectError {
				require.Error(t, err)
				mockQueries.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			mockQueries.AssertExpectations(t)
		})
	}
}
