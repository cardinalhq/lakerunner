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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockEcsClient implements EcsClientInterface for testing
type MockEcsClient struct {
	mock.Mock
}

func (m *MockEcsClient) ListTasks(ctx context.Context, params *ecs.ListTasksInput, optFns ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*ecs.ListTasksOutput), args.Error(1)
}

func (m *MockEcsClient) DescribeTasks(ctx context.Context, params *ecs.DescribeTasksInput, optFns ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*ecs.DescribeTasksOutput), args.Error(1)
}

func TestEcsWorkerDiscovery_InterfaceUsage(t *testing.T) {
	// Example test showing how to use the interface for mocking
	mockClient := &MockEcsClient{}

	config := EcsWorkerDiscoveryConfig{
		ServiceName: "test-service",
		ClusterName: "test-cluster",
		WorkerPort:  8081,
		Interval:    1 * time.Second,
	}

	discovery, err := NewEcsWorkerDiscoveryWithClient(config, mockClient)
	require.NoError(t, err)
	assert.NotNil(t, discovery)

	// Verify the discovery was created with the mock client
	assert.Equal(t, "test-service", discovery.serviceName)
	assert.Equal(t, "test-cluster", discovery.clusterName)
	assert.Equal(t, 8081, discovery.workerPort)
	assert.Equal(t, mockClient, discovery.ecsClient)
}

func TestEcsWorkerDiscovery_ConfigValidation(t *testing.T) {
	mockClient := &MockEcsClient{}

	tests := []struct {
		name        string
		config      EcsWorkerDiscoveryConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: EcsWorkerDiscoveryConfig{
				ServiceName: "test-service",
				ClusterName: "test-cluster",
				WorkerPort:  8081,
				Interval:    10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing service name",
			config: EcsWorkerDiscoveryConfig{
				ClusterName: "test-cluster",
			},
			expectError: true,
			errorMsg:    "ServiceName is required",
		},
		{
			name: "missing cluster name",
			config: EcsWorkerDiscoveryConfig{
				ServiceName: "test-service",
			},
			expectError: true,
			errorMsg:    "ClusterName is required",
		},
		{
			name: "defaults applied",
			config: EcsWorkerDiscoveryConfig{
				ServiceName: "test-service",
				ClusterName: "test-cluster",
				// WorkerPort and Interval not set
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery, err := NewEcsWorkerDiscoveryWithClient(tt.config, mockClient)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, discovery)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, discovery)

				// Verify defaults are applied
				if tt.config.WorkerPort == 0 {
					assert.Equal(t, 8081, discovery.workerPort)
				}
				if tt.config.Interval == 0 {
					assert.Equal(t, 10*time.Second, discovery.interval)
				}
			}
		})
	}
}

func TestEcsWorkerDiscovery_DiscoverWorkers_EmptyResult(t *testing.T) {
	mockClient := &MockEcsClient{}

	config := EcsWorkerDiscoveryConfig{
		ServiceName: "test-service",
		ClusterName: "test-cluster",
		WorkerPort:  8081,
		Interval:    1 * time.Second,
	}

	discovery, err := NewEcsWorkerDiscoveryWithClient(config, mockClient)
	require.NoError(t, err)

	// Mock empty task list
	mockClient.On("ListTasks", mock.Anything, mock.MatchedBy(func(input *ecs.ListTasksInput) bool {
		return *input.Cluster == "test-cluster" && *input.ServiceName == "test-service"
	})).Return(&ecs.ListTasksOutput{
		TaskArns: []string{},
	}, nil)

	ctx := context.Background()
	err = discovery.discoverWorkers(ctx)
	assert.NoError(t, err)

	// Should have no workers
	workers := discovery.GetWorkers()
	assert.Empty(t, workers)

	mockClient.AssertExpectations(t)
}

func TestEcsWorkerDiscovery_DiscoverWorkers_WithTasks(t *testing.T) {
	mockClient := &MockEcsClient{}

	config := EcsWorkerDiscoveryConfig{
		ServiceName: "test-service",
		ClusterName: "test-cluster",
		WorkerPort:  8081,
		Interval:    1 * time.Second,
	}

	discovery, err := NewEcsWorkerDiscoveryWithClient(config, mockClient)
	require.NoError(t, err)

	// Mock task list with one task
	taskArn := "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/abc123"
	mockClient.On("ListTasks", mock.Anything, mock.MatchedBy(func(input *ecs.ListTasksInput) bool {
		return *input.Cluster == "test-cluster" && *input.ServiceName == "test-service"
	})).Return(&ecs.ListTasksOutput{
		TaskArns: []string{taskArn},
	}, nil)

	// Mock describe tasks with running task
	status := "RUNNING"
	attachmentType := "ElasticNetworkInterface"
	detailName := "privateIPv4Address"
	detailValue := "10.0.0.100"

	mockClient.On("DescribeTasks", mock.Anything, mock.MatchedBy(func(input *ecs.DescribeTasksInput) bool {
		return *input.Cluster == "test-cluster" && len(input.Tasks) == 1 && input.Tasks[0] == taskArn
	})).Return(&ecs.DescribeTasksOutput{
		Tasks: []types.Task{
			{
				LastStatus: &status,
				Attachments: []types.Attachment{
					{
						Type: &attachmentType,
						Details: []types.KeyValuePair{
							{
								Name:  &detailName,
								Value: &detailValue,
							},
						},
					},
				},
			},
		},
	}, nil)

	ctx := context.Background()
	err = discovery.discoverWorkers(ctx)
	assert.NoError(t, err)

	// Should have one worker
	workers := discovery.GetWorkers()
	require.Len(t, workers, 1)
	assert.Equal(t, "10.0.0.100", workers[0].IP)
	assert.Equal(t, 8081, workers[0].Port)

	mockClient.AssertExpectations(t)
}
