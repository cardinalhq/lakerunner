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

package workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockDB is a mock implementation of the DB interface for testing.
type mockDB struct {
	mu               sync.Mutex
	claimFunc        func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error)
	completeFunc     func(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error
	failFunc         func(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error)
	heartbeatFunc    func(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error
	heartbeatCallCnt int
	completedWorkIDs []int64
	failedWorkIDs    []int64
	heartbeatedIDs   [][]int64
}

func (m *mockDB) WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx, arg)
	}
	return lrdb.WorkQueue{}, pgx.ErrNoRows
}

func (m *mockDB) WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completedWorkIDs = append(m.completedWorkIDs, arg.ID)
	if m.completeFunc != nil {
		return m.completeFunc(ctx, arg)
	}
	return nil
}

func (m *mockDB) WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedWorkIDs = append(m.failedWorkIDs, arg.ID)
	if m.failFunc != nil {
		return m.failFunc(ctx, arg)
	}
	return 1, nil
}

func (m *mockDB) WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCallCnt++
	// Make a copy of the IDs slice
	idsCopy := make([]int64, len(arg.Ids))
	copy(idsCopy, arg.Ids)
	m.heartbeatedIDs = append(m.heartbeatedIDs, idsCopy)
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, arg)
	}
	return nil
}

func (m *mockDB) WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error) {
	// Not needed for manager tests, return empty
	return []lrdb.WorkQueueDepthAllRow{}, nil
}

func TestNewManager(t *testing.T) {
	db := &mockDB{}
	mgr := NewManager(db, 123, "test-task")

	assert.NotNil(t, mgr)
	assert.Equal(t, int64(123), mgr.workerID)
	assert.Equal(t, "test-task", mgr.taskName)
	assert.Equal(t, time.Minute, mgr.heartbeatInterval)
}

func TestNewManagerWithOptions(t *testing.T) {
	db := &mockDB{}
	mgr := NewManager(db, 123, "test-task", WithHeartbeatInterval(30*time.Second))

	assert.NotNil(t, mgr)
	assert.Equal(t, 30*time.Second, mgr.heartbeatInterval)
}

func TestManagerRequestWork_NoWorkAvailable(t *testing.T) {
	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mgr.WaitForOutstandingWork(ctx)
	}()

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	assert.Nil(t, work)
}

func TestManagerRequestWork_WorkAvailable(t *testing.T) {
	testOrgID := uuid.New()
	testSpec := json.RawMessage(`{"key": "value"}`)

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			assert.Equal(t, int64(123), arg.WorkerID)
			assert.Equal(t, "test-task", arg.TaskName)
			return lrdb.WorkQueue{
				ID:             100,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           testSpec,
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mgr.WaitForOutstandingWork(ctx)
	}()

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	assert.Equal(t, int64(100), work.ID())
	assert.Equal(t, "test-task", work.TaskName())
	assert.Equal(t, testOrgID, work.OrganizationID())
	assert.Equal(t, int16(5), work.InstanceNum())
	assert.Equal(t, testSpec, work.Spec())
	assert.Equal(t, int32(0), work.Tries())

	// Complete the work to clean up
	err = work.Complete()
	require.NoError(t, err)
}

func TestManagerRequestWork_ContextCanceled(t *testing.T) {
	db := &mockDB{}
	mgr := NewManager(db, 123, "test-task")

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Run(ctx)
	cancel()

	time.Sleep(100 * time.Millisecond) // Give time for manager to shut down

	work, err := mgr.RequestWork(context.Background())
	assert.Error(t, err)
	assert.Nil(t, work)
	assert.Contains(t, err.Error(), "shut down")
}

func TestManagerRequestWork_ClaimError(t *testing.T) {
	testErr := errors.New("database error")
	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{}, testErr
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mgr.WaitForOutstandingWork(ctx)
	}()

	work, err := mgr.RequestWork(ctx)
	assert.Error(t, err)
	assert.Nil(t, work)
	assert.Equal(t, testErr, err)
}

func TestManagerHeartbeat(t *testing.T) {
	testOrgID := uuid.New()
	workID := int64(100)

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             workID,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task", WithHeartbeatInterval(100*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Run(ctx)

	// Request work to get an item to heartbeat
	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	// Wait for at least one heartbeat
	time.Sleep(250 * time.Millisecond)

	db.mu.Lock()
	heartbeatCount := db.heartbeatCallCnt
	db.mu.Unlock()

	assert.Greater(t, heartbeatCount, 0, "Expected at least one heartbeat")

	// Verify the heartbeat contained our work ID
	db.mu.Lock()
	found := false
	for _, ids := range db.heartbeatedIDs {
		for _, id := range ids {
			if id == workID {
				found = true
				break
			}
		}
	}
	db.mu.Unlock()

	assert.True(t, found, "Expected work ID %d to be heartbeated", workID)

	// Complete the work
	err = work.Complete()
	require.NoError(t, err)
}

func TestManagerHeartbeat_EmptyQueue(t *testing.T) {
	db := &mockDB{}

	mgr := NewManager(db, 123, "test-task", WithHeartbeatInterval(50*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Run(ctx)

	// Wait for potential heartbeats (should not happen with empty queue)
	time.Sleep(150 * time.Millisecond)

	db.mu.Lock()
	heartbeatCount := db.heartbeatCallCnt
	db.mu.Unlock()

	assert.Equal(t, 0, heartbeatCount, "Expected no heartbeats with empty queue")
}

func TestManagerHeartbeat_Error(t *testing.T) {
	testOrgID := uuid.New()
	heartbeatErr := errors.New("heartbeat error")

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             100,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
		heartbeatFunc: func(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
			return heartbeatErr
		},
	}

	mgr := NewManager(db, 123, "test-task", WithHeartbeatInterval(50*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Run(ctx)

	// Request work
	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	// Wait for heartbeat attempt (should not panic on error)
	time.Sleep(150 * time.Millisecond)

	db.mu.Lock()
	heartbeatCount := db.heartbeatCallCnt
	db.mu.Unlock()

	assert.Greater(t, heartbeatCount, 0, "Expected heartbeat attempts despite errors")

	// Complete the work
	err = work.Complete()
	require.NoError(t, err)
}

func TestManagerCompleteWork(t *testing.T) {
	testOrgID := uuid.New()
	workID := int64(100)

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             workID,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	err = work.Complete()
	require.NoError(t, err)

	db.mu.Lock()
	completedIDs := db.completedWorkIDs
	db.mu.Unlock()

	assert.Contains(t, completedIDs, workID)
	assert.Empty(t, mgr.acquiredIDs, "Expected work ID to be removed from acquired list")

	// Wait for outstanding work to complete
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = mgr.WaitForOutstandingWork(ctx)
	require.NoError(t, err)
}

func TestManagerFailWork(t *testing.T) {
	testOrgID := uuid.New()
	workID := int64(100)

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             workID,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	err = work.Fail(nil)
	require.NoError(t, err)

	db.mu.Lock()
	failedIDs := db.failedWorkIDs
	db.mu.Unlock()

	assert.Contains(t, failedIDs, workID)
	assert.Empty(t, mgr.acquiredIDs, "Expected work ID to be removed from acquired list")

	// Wait for outstanding work to complete
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = mgr.WaitForOutstandingWork(ctx)
	require.NoError(t, err)
}

func TestManagerWaitForOutstandingWork(t *testing.T) {
	testOrgID := uuid.New()

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             100,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
		completeFunc: func(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
			// Simulate slow completion
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	// Start completing work in background
	go func() {
		_ = work.Complete()
	}()

	// Wait for outstanding work
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = mgr.WaitForOutstandingWork(ctx)
	require.NoError(t, err)
}

func TestManagerWaitForOutstandingWork_Timeout(t *testing.T) {
	testOrgID := uuid.New()

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{
				ID:             100,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work)

	// Don't complete the work, just wait with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = mgr.WaitForOutstandingWork(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Clean up
	_ = work.Complete()
}

func TestManagerMultipleWorkItems(t *testing.T) {
	testOrgID := uuid.New()
	nextID := int64(100)
	var idMutex sync.Mutex

	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			idMutex.Lock()
			defer idMutex.Unlock()
			id := nextID
			nextID++
			return lrdb.WorkQueue{
				ID:             id,
				TaskName:       "test-task",
				OrganizationID: testOrgID,
				InstanceNum:    5,
				Spec:           json.RawMessage(`{}`),
				Tries:          0,
				ClaimedBy:      123,
			}, nil
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)

	// Request multiple work items
	work1, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work1)

	work2, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work2)

	work3, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	require.NotNil(t, work3)

	assert.Equal(t, int64(100), work1.ID())
	assert.Equal(t, int64(101), work2.ID())
	assert.Equal(t, int64(102), work3.ID())

	// Verify all are tracked
	assert.Len(t, mgr.acquiredIDs, 3)

	// Complete them
	err = work1.Complete()
	require.NoError(t, err)
	err = work2.Complete()
	require.NoError(t, err)
	err = work3.Complete()
	require.NoError(t, err)

	// Wait for outstanding work
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = mgr.WaitForOutstandingWork(ctx)
	require.NoError(t, err)

	assert.Empty(t, mgr.acquiredIDs)
}

func TestManagerRequestWork_ConstraintViolation(t *testing.T) {
	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{}, errors.New("ERROR: 23P01: constraint violation")
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mgr.WaitForOutstandingWork(ctx)
	}()

	work, err := mgr.RequestWork(ctx)
	require.NoError(t, err)
	assert.Nil(t, work, "Expected nil work for constraint violation")
}

func TestManagerRequestWork_RequestContextCanceled(t *testing.T) {
	db := &mockDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			// Slow claim that will allow context cancellation
			time.Sleep(200 * time.Millisecond)
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}

	mgr := NewManager(db, 123, "test-task")
	ctx := context.Background()
	mgr.Run(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mgr.WaitForOutstandingWork(ctx)
	}()

	// Create request context with short timeout
	reqCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	work, err := mgr.RequestWork(reqCtx)
	assert.Error(t, err)
	assert.Nil(t, work)
	assert.Equal(t, context.DeadlineExceeded, err)
}
