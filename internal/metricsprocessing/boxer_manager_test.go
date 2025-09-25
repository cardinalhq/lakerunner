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

package metricsprocessing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBoxerConsumer is a mock implementation of BoxerConsumer
type MockBoxerConsumer struct {
	mock.Mock
}

func (m *MockBoxerConsumer) Run(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBoxerConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockConsumerFactory is a mock implementation of ConsumerFactory
type MockConsumerFactory struct {
	mock.Mock
}

func (m *MockConsumerFactory) CreateLogIngestConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateMetricIngestConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateTraceIngestConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateLogCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateMetricCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateTraceCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func (m *MockConsumerFactory) CreateMetricRollupConsumer(ctx context.Context) (BoxerConsumer, error) {
	args := m.Called(ctx)
	return args.Get(0).(BoxerConsumer), args.Error(1)
}

func TestNewBoxerManagerWithFactory(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		tasks       []string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty tasks should error",
			tasks:       []string{},
			expectError: true,
			errorMsg:    "at least one task must be specified",
		},
		{
			name:        "valid single task",
			tasks:       []string{"compact-logs"},
			expectError: false,
		},
		{
			name:        "valid multiple tasks",
			tasks:       []string{"compact-logs", "compact-metrics"},
			expectError: false,
		},
		{
			name:        "invalid task should error",
			tasks:       []string{"invalid-task"},
			expectError: true,
			errorMsg:    "unknown task: invalid-task",
		},
		{
			name:        "mix of valid and invalid tasks should error",
			tasks:       []string{"compact-logs", "invalid-task"},
			expectError: true,
			errorMsg:    "unknown task: invalid-task",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &MockConsumerFactory{}

			// Set up factory expectations for valid tasks
			if !tt.expectError || tt.errorMsg != "at least one task must be specified" {
				for _, task := range tt.tasks {
					switch task {
					case "compact-logs":
						consumer := &MockBoxerConsumer{}
						factory.On("CreateLogCompactionConsumer", ctx).Return(consumer, nil)
					case "compact-metrics":
						consumer := &MockBoxerConsumer{}
						factory.On("CreateMetricCompactionConsumer", ctx).Return(consumer, nil)
					case "compact-traces":
						consumer := &MockBoxerConsumer{}
						factory.On("CreateTraceCompactionConsumer", ctx).Return(consumer, nil)
					case "rollup-metrics":
						consumer := &MockBoxerConsumer{}
						factory.On("CreateMetricRollupConsumer", ctx).Return(consumer, nil)
					}
				}
			}

			manager, err := NewBoxerManagerWithFactory(ctx, factory, tt.tasks)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, manager)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, manager)
				assert.Equal(t, tt.tasks, manager.getTasks())
				assert.Len(t, manager.consumers, len(tt.tasks))
			}

			factory.AssertExpectations(t)
		})
	}
}

func TestBoxerManager_Run(t *testing.T) {
	ctx := context.Background()

	t.Run("successful run with single consumer", func(t *testing.T) {
		factory := &MockConsumerFactory{}
		consumer := &MockBoxerConsumer{}

		factory.On("CreateLogCompactionConsumer", ctx).Return(consumer, nil)
		consumer.On("Run", mock.AnythingOfType("*context.valueCtx")).Return(nil)

		manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{"compact-logs"})
		require.NoError(t, err)

		// Use a context with timeout to prevent test hanging
		runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = manager.Run(runCtx)
		assert.NoError(t, err)

		factory.AssertExpectations(t)
		consumer.AssertExpectations(t)
	})

	t.Run("successful run with multiple consumers", func(t *testing.T) {
		factory := &MockConsumerFactory{}
		logConsumer := &MockBoxerConsumer{}
		metricConsumer := &MockBoxerConsumer{}

		factory.On("CreateLogCompactionConsumer", ctx).Return(logConsumer, nil)
		factory.On("CreateMetricCompactionConsumer", ctx).Return(metricConsumer, nil)

		logConsumer.On("Run", mock.AnythingOfType("*context.valueCtx")).Return(nil)
		metricConsumer.On("Run", mock.AnythingOfType("*context.valueCtx")).Return(nil)

		manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{"compact-logs", "compact-metrics"})
		require.NoError(t, err)

		// Use a context with timeout to prevent test hanging
		runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = manager.Run(runCtx)
		assert.NoError(t, err)

		factory.AssertExpectations(t)
		logConsumer.AssertExpectations(t)
		metricConsumer.AssertExpectations(t)
	})

	t.Run("consumer failure should propagate error", func(t *testing.T) {
		factory := &MockConsumerFactory{}
		consumer := &MockBoxerConsumer{}

		factory.On("CreateLogCompactionConsumer", ctx).Return(consumer, nil)
		consumer.On("Run", mock.AnythingOfType("*context.valueCtx")).Return(errors.New("consumer failed"))

		manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{"compact-logs"})
		require.NoError(t, err)

		err = manager.Run(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task compact-logs failed")
		assert.Contains(t, err.Error(), "consumer failed")

		factory.AssertExpectations(t)
		consumer.AssertExpectations(t)
	})

	t.Run("no consumers should error", func(t *testing.T) {
		manager := &BoxerManager{
			consumers: []BoxerConsumer{},
			tasks:     []string{},
		}

		err := manager.Run(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no consumers to run")
	})
}

func TestBoxerManager_Close(t *testing.T) {
	ctx := context.Background()

	t.Run("successful close", func(t *testing.T) {
		factory := &MockConsumerFactory{}
		consumer1 := &MockBoxerConsumer{}
		consumer2 := &MockBoxerConsumer{}

		factory.On("CreateLogCompactionConsumer", ctx).Return(consumer1, nil)
		factory.On("CreateMetricCompactionConsumer", ctx).Return(consumer2, nil)

		consumer1.On("Close").Return(nil)
		consumer2.On("Close").Return(nil)

		manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{"compact-logs", "compact-metrics"})
		require.NoError(t, err)

		err = manager.Close()
		assert.NoError(t, err)

		factory.AssertExpectations(t)
		consumer1.AssertExpectations(t)
		consumer2.AssertExpectations(t)
	})

	t.Run("close errors should be collected", func(t *testing.T) {
		factory := &MockConsumerFactory{}
		consumer1 := &MockBoxerConsumer{}
		consumer2 := &MockBoxerConsumer{}

		factory.On("CreateLogCompactionConsumer", ctx).Return(consumer1, nil)
		factory.On("CreateMetricCompactionConsumer", ctx).Return(consumer2, nil)

		consumer1.On("Close").Return(errors.New("close error 1"))
		consumer2.On("Close").Return(errors.New("close error 2"))

		manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{"compact-logs", "compact-metrics"})
		require.NoError(t, err)

		err = manager.Close()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "errors closing consumers")

		factory.AssertExpectations(t)
		consumer1.AssertExpectations(t)
		consumer2.AssertExpectations(t)
	})
}

func TestBoxerManager_GetTasks(t *testing.T) {
	ctx := context.Background()
	factory := &MockConsumerFactory{}
	consumer := &MockBoxerConsumer{}

	factory.On("CreateLogCompactionConsumer", ctx).Return(consumer, nil)

	tasks := []string{"compact-logs"}
	manager, err := NewBoxerManagerWithFactory(ctx, factory, tasks)
	require.NoError(t, err)

	result := manager.getTasks()
	assert.Equal(t, tasks, result)

	// Verify that the returned slice is a copy (modifying it shouldn't affect the original)
	result[0] = "modified"
	assert.Equal(t, tasks, manager.getTasks())

	factory.AssertExpectations(t)
}

func TestBoxerManager_TaskMapping(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		task         string
		expectedCall string
	}{
		{"compact-logs", "CreateLogCompactionConsumer"},
		{"compact-metrics", "CreateMetricCompactionConsumer"},
		{"compact-traces", "CreateTraceCompactionConsumer"},
		{"rollup-metrics", "CreateMetricRollupConsumer"},
	}

	for _, tt := range tests {
		t.Run(tt.task, func(t *testing.T) {
			factory := &MockConsumerFactory{}
			consumer := &MockBoxerConsumer{}

			switch tt.expectedCall {
			case "CreateLogCompactionConsumer":
				factory.On("CreateLogCompactionConsumer", ctx).Return(consumer, nil)
			case "CreateMetricCompactionConsumer":
				factory.On("CreateMetricCompactionConsumer", ctx).Return(consumer, nil)
			case "CreateTraceCompactionConsumer":
				factory.On("CreateTraceCompactionConsumer", ctx).Return(consumer, nil)
			case "CreateMetricRollupConsumer":
				factory.On("CreateMetricRollupConsumer", ctx).Return(consumer, nil)
			}

			manager, err := NewBoxerManagerWithFactory(ctx, factory, []string{tt.task})
			assert.NoError(t, err)
			assert.NotNil(t, manager)

			factory.AssertExpectations(t)
		})
	}
}
