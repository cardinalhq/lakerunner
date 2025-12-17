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

package workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockEnqueueDB is a mock implementation of EnqueueDB for testing.
type mockEnqueueDB struct {
	addFunc     func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error)
	depthFunc   func(ctx context.Context, taskName string) (int64, error)
	cleanupFunc func(ctx context.Context, heartbeatTimeout time.Duration) error
}

func (m *mockEnqueueDB) WorkQueueAdd(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
	if m.addFunc != nil {
		return m.addFunc(ctx, arg)
	}
	return lrdb.WorkQueue{ID: 100}, nil
}

func (m *mockEnqueueDB) WorkQueueDepth(ctx context.Context, taskName string) (int64, error) {
	if m.depthFunc != nil {
		return m.depthFunc(ctx, taskName)
	}
	return 0, nil
}

func (m *mockEnqueueDB) WorkQueueCleanup(ctx context.Context, heartbeatTimeout time.Duration) error {
	if m.cleanupFunc != nil {
		return m.cleanupFunc(ctx, heartbeatTimeout)
	}
	return nil
}

func TestAdd(t *testing.T) {
	testOrgID := uuid.New()
	testSpec := map[string]any{
		"key": "value",
	}

	tests := []struct {
		name        string
		taskName    string
		orgID       uuid.UUID
		instanceNum int16
		spec        map[string]any
		mockFunc    func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error)
		expectedID  int64
		expectedErr bool
	}{
		{
			name:        "successful add",
			taskName:    "test-task",
			orgID:       testOrgID,
			instanceNum: 5,
			spec:        testSpec,
			mockFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
				assert.Equal(t, "test-task", arg.TaskName)
				assert.Equal(t, testOrgID, arg.OrganizationID)
				assert.Equal(t, int16(5), arg.InstanceNum)
				assert.Equal(t, int32(DefaultPriority), arg.Priority)
				// Verify JSON content by unmarshaling
				var decoded map[string]any
				err := json.Unmarshal(arg.Spec, &decoded)
				require.NoError(t, err)
				assert.Equal(t, testSpec, decoded)
				return lrdb.WorkQueue{ID: 123}, nil
			},
			expectedID:  123,
			expectedErr: false,
		},
		{
			name:        "database error",
			taskName:    "test-task",
			orgID:       testOrgID,
			instanceNum: 5,
			spec:        testSpec,
			mockFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
				return lrdb.WorkQueue{}, errors.New("database error")
			},
			expectedID:  0,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockEnqueueDB{
				addFunc: tt.mockFunc,
			}

			id, err := Add(context.Background(), db, tt.taskName, tt.orgID, tt.instanceNum, tt.spec)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
			}
		})
	}
}

func TestDepth(t *testing.T) {
	tests := []struct {
		name          string
		taskName      string
		mockFunc      func(ctx context.Context, taskName string) (int64, error)
		expectedDepth int64
		expectedErr   bool
	}{
		{
			name:     "queue with items",
			taskName: "test-task",
			mockFunc: func(ctx context.Context, taskName string) (int64, error) {
				assert.Equal(t, "test-task", taskName)
				return 42, nil
			},
			expectedDepth: 42,
			expectedErr:   false,
		},
		{
			name:     "empty queue",
			taskName: "empty-task",
			mockFunc: func(ctx context.Context, taskName string) (int64, error) {
				return 0, nil
			},
			expectedDepth: 0,
			expectedErr:   false,
		},
		{
			name:     "database error",
			taskName: "error-task",
			mockFunc: func(ctx context.Context, taskName string) (int64, error) {
				return 0, errors.New("database error")
			},
			expectedDepth: 0,
			expectedErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockEnqueueDB{
				depthFunc: tt.mockFunc,
			}

			depth, err := Depth(context.Background(), db, tt.taskName)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedDepth, depth)
			}
		})
	}
}

func TestCleanup(t *testing.T) {
	tests := []struct {
		name             string
		heartbeatTimeout time.Duration
		mockFunc         func(ctx context.Context, heartbeatTimeout time.Duration) error
		expectedErr      bool
	}{
		{
			name:             "successful cleanup",
			heartbeatTimeout: 5 * time.Minute,
			mockFunc: func(ctx context.Context, heartbeatTimeout time.Duration) error {
				assert.Equal(t, 5*time.Minute, heartbeatTimeout)
				return nil
			},
			expectedErr: false,
		},
		{
			name:             "database error",
			heartbeatTimeout: 5 * time.Minute,
			mockFunc: func(ctx context.Context, heartbeatTimeout time.Duration) error {
				return errors.New("database error")
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockEnqueueDB{
				cleanupFunc: tt.mockFunc,
			}

			err := Cleanup(context.Background(), db, tt.heartbeatTimeout)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAdd_NilSpec(t *testing.T) {
	testOrgID := uuid.New()

	db := &mockEnqueueDB{
		addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
			// nil map marshals to "null" JSON
			assert.Equal(t, json.RawMessage("null"), arg.Spec)
			return lrdb.WorkQueue{ID: 100}, nil
		},
	}

	id, err := Add(context.Background(), db, "test-task", testOrgID, 5, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(100), id)
}

func TestAdd_EmptySpec(t *testing.T) {
	testOrgID := uuid.New()
	emptySpec := map[string]any{}

	db := &mockEnqueueDB{
		addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
			// empty map marshals to "{}" JSON
			assert.Equal(t, json.RawMessage("{}"), arg.Spec)
			return lrdb.WorkQueue{ID: 100}, nil
		},
	}

	id, err := Add(context.Background(), db, "test-task", testOrgID, 5, emptySpec)
	require.NoError(t, err)
	assert.Equal(t, int64(100), id)
}

func TestAdd_ComplexSpec(t *testing.T) {
	testOrgID := uuid.New()
	complexSpec := map[string]any{
		"strings": "value",
		"numbers": 123,
		"bools":   true,
		"nested":  map[string]any{"inner": "value"},
		"arrays":  []any{1, 2, 3},
		"nulls":   nil,
	}

	db := &mockEnqueueDB{
		addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
			// Verify the JSON roundtrips correctly
			var decoded map[string]any
			err := json.Unmarshal(arg.Spec, &decoded)
			require.NoError(t, err)
			assert.Equal(t, "value", decoded["strings"])
			assert.Equal(t, float64(123), decoded["numbers"]) // JSON numbers decode as float64
			assert.Equal(t, true, decoded["bools"])
			assert.Equal(t, map[string]any{"inner": "value"}, decoded["nested"])
			assert.Equal(t, []any{float64(1), float64(2), float64(3)}, decoded["arrays"])
			assert.Nil(t, decoded["nulls"])
			return lrdb.WorkQueue{ID: 100}, nil
		},
	}

	id, err := Add(context.Background(), db, "test-task", testOrgID, 5, complexSpec)
	require.NoError(t, err)
	assert.Equal(t, int64(100), id)
}

func TestAddWithPriority(t *testing.T) {
	testOrgID := uuid.New()
	testSpec := map[string]any{"key": "value"}

	tests := []struct {
		name        string
		priority    int32
		expectedID  int64
		expectedErr bool
	}{
		{
			name:        "default priority",
			priority:    DefaultPriority,
			expectedID:  100,
			expectedErr: false,
		},
		{
			name:        "low priority",
			priority:    LowPriority,
			expectedID:  101,
			expectedErr: false,
		},
		{
			name:        "high priority (negative)",
			priority:    -100,
			expectedID:  102,
			expectedErr: false,
		},
		{
			name:        "custom priority",
			priority:    500,
			expectedID:  103,
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockEnqueueDB{
				addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
					assert.Equal(t, tt.priority, arg.Priority)
					return lrdb.WorkQueue{ID: tt.expectedID}, nil
				},
			}

			id, err := AddWithPriority(context.Background(), db, "test-task", testOrgID, 5, testSpec, tt.priority)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
			}
		})
	}
}

func TestAddBundleWithPriority(t *testing.T) {
	testOrgID := uuid.New()
	testBundle := []byte(`{"key": "value"}`)

	tests := []struct {
		name        string
		priority    int32
		expectedID  int64
		expectedErr bool
	}{
		{
			name:        "default priority via AddBundle",
			priority:    DefaultPriority,
			expectedID:  200,
			expectedErr: false,
		},
		{
			name:        "low priority",
			priority:    LowPriority,
			expectedID:  201,
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockEnqueueDB{
				addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
					assert.Equal(t, tt.priority, arg.Priority)
					assert.Equal(t, json.RawMessage(testBundle), arg.Spec)
					return lrdb.WorkQueue{ID: tt.expectedID}, nil
				},
			}

			id, err := AddBundleWithPriority(context.Background(), db, "test-task", testOrgID, 5, testBundle, tt.priority)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
			}
		})
	}
}

func TestAddBundle_UsesDefaultPriority(t *testing.T) {
	testOrgID := uuid.New()
	testBundle := []byte(`{"key": "value"}`)

	db := &mockEnqueueDB{
		addFunc: func(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
			assert.Equal(t, int32(DefaultPriority), arg.Priority)
			return lrdb.WorkQueue{ID: 300}, nil
		},
	}

	id, err := AddBundle(context.Background(), db, "test-task", testOrgID, 5, testBundle)
	require.NoError(t, err)
	assert.Equal(t, int64(300), id)
}
