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
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockWorkQueueDB is a mock implementation of the workqueue.DB interface for testing.
type mockWorkQueueDB struct {
	mu               sync.Mutex
	claimFunc        func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error)
	completeFunc     func(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error
	failFunc         func(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error)
	heartbeatFunc    func(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error
	completedWorkIDs []int64
	failedWorkIDs    []int64
}

func (m *mockWorkQueueDB) WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx, arg)
	}
	return lrdb.WorkQueue{}, pgx.ErrNoRows
}

func (m *mockWorkQueueDB) WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completedWorkIDs = append(m.completedWorkIDs, arg.ID)
	if m.completeFunc != nil {
		return m.completeFunc(ctx, arg)
	}
	return nil
}

func (m *mockWorkQueueDB) WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedWorkIDs = append(m.failedWorkIDs, arg.ID)
	if m.failFunc != nil {
		return m.failFunc(ctx, arg)
	}
	return 1, nil
}

func (m *mockWorkQueueDB) WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, arg)
	}
	return nil
}

func (m *mockWorkQueueDB) WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error) {
	return []lrdb.WorkQueueDepthAllRow{}, nil
}

// mockBundleProcessor is a mock implementation of the BundleProcessor interface.
type mockBundleProcessor struct {
	mu            sync.Mutex
	processFunc   func(ctx context.Context, workItem workqueue.Workable) error
	processedIDs  []int64
	processCalled int32
}

func (m *mockBundleProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	atomic.AddInt32(&m.processCalled, 1)
	m.mu.Lock()
	m.processedIDs = append(m.processedIDs, workItem.ID())
	m.mu.Unlock()

	if m.processFunc != nil {
		return m.processFunc(ctx, workItem)
	}
	return nil
}

func TestNewQueueWorkerConsumer(t *testing.T) {
	db := &mockWorkQueueDB{}
	mgr := workqueue.NewManager(db, 123, "test-task")
	processor := &mockBundleProcessor{}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	assert.NotNil(t, consumer)
	assert.Equal(t, mgr, consumer.manager)
	assert.Equal(t, processor, consumer.processor)
	assert.Equal(t, "test-task", consumer.taskName)
}

func TestQueueWorkerConsumer_Close(t *testing.T) {
	db := &mockWorkQueueDB{}
	mgr := workqueue.NewManager(db, 123, "test-task")
	processor := &mockBundleProcessor{}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	err := consumer.Close()
	assert.NoError(t, err)
}

func TestQueueWorkerConsumer_Run_ContextCancellation(t *testing.T) {
	db := &mockWorkQueueDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}
	mgr := workqueue.NewManager(db, 123, "test-task")
	processor := &mockBundleProcessor{}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-done:
		// Should return nil or context error
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

func TestQueueWorkerConsumer_Run_ProcessesWorkItem(t *testing.T) {
	testOrgID := uuid.New()
	workID := int64(100)
	claimCount := int32(0)

	db := &mockWorkQueueDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			count := atomic.AddInt32(&claimCount, 1)
			if count == 1 {
				return lrdb.WorkQueue{
					ID:             workID,
					TaskName:       "test-task",
					OrganizationID: testOrgID,
					InstanceNum:    5,
					Spec:           json.RawMessage(`{"key": "value"}`),
					Tries:          0,
					ClaimedBy:      123,
				}, nil
			}
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}
	mgr := workqueue.NewManager(db, 123, "test-task")

	processed := make(chan int64, 1)
	processor := &mockBundleProcessor{
		processFunc: func(ctx context.Context, workItem workqueue.Workable) error {
			processed <- workItem.ID()
			return nil
		},
	}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for work to be processed
	select {
	case id := <-processed:
		assert.Equal(t, workID, id)
	case <-time.After(2 * time.Second):
		t.Fatal("Work item was not processed")
	}

	// Cancel and wait for shutdown
	cancel()
	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	// Verify work was completed
	db.mu.Lock()
	assert.Contains(t, db.completedWorkIDs, workID)
	db.mu.Unlock()
}

func TestQueueWorkerConsumer_Run_FailsWorkItemOnProcessError(t *testing.T) {
	testOrgID := uuid.New()
	workID := int64(100)
	claimCount := int32(0)

	db := &mockWorkQueueDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			count := atomic.AddInt32(&claimCount, 1)
			if count == 1 {
				return lrdb.WorkQueue{
					ID:             workID,
					TaskName:       "test-task",
					OrganizationID: testOrgID,
					InstanceNum:    5,
					Spec:           json.RawMessage(`{}`),
					Tries:          0,
					ClaimedBy:      123,
				}, nil
			}
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}
	mgr := workqueue.NewManager(db, 123, "test-task")

	processErr := errors.New("processing failed")
	processed := make(chan struct{}, 1)
	processor := &mockBundleProcessor{
		processFunc: func(ctx context.Context, workItem workqueue.Workable) error {
			processed <- struct{}{}
			return processErr
		},
	}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for work to be processed
	select {
	case <-processed:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Work item was not processed")
	}

	// Give time for failure to be recorded
	time.Sleep(100 * time.Millisecond)

	// Cancel and wait for shutdown
	cancel()
	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	// Verify work was failed (not completed)
	db.mu.Lock()
	assert.Contains(t, db.failedWorkIDs, workID)
	assert.NotContains(t, db.completedWorkIDs, workID)
	db.mu.Unlock()
}

func TestQueueWorkerConsumer_Run_NoWorkAvailable(t *testing.T) {
	db := &mockWorkQueueDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}
	mgr := workqueue.NewManager(db, 123, "test-task")
	processor := &mockBundleProcessor{}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Let it poll for a bit
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-done:
		// Should return nil or context error
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	// Processor should not have been called
	assert.Equal(t, int32(0), atomic.LoadInt32(&processor.processCalled))
}

func TestQueueWorkerConsumer_waitForShutdown_UsesFreshContext(t *testing.T) {
	// This test verifies that waitForShutdown uses a fresh context with timeout
	// rather than the cancelled context, which would cause immediate return.
	db := &mockWorkQueueDB{}
	mgr := workqueue.NewManager(db, 123, "test-task")
	processor := &mockBundleProcessor{}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	// Start the manager
	ctx := context.Background()
	mgr.Run(ctx)

	// Call waitForShutdown - it should return quickly since no outstanding work
	start := time.Now()
	err := consumer.waitForShutdown()
	elapsed := time.Since(start)

	// Should return quickly (not wait for 30s timeout) and without error
	assert.NoError(t, err)
	assert.Less(t, elapsed, 1*time.Second, "waitForShutdown should return quickly when no outstanding work")
}

func TestQueueWorkerConsumer_Run_MultipleWorkItems(t *testing.T) {
	testOrgID := uuid.New()
	claimCount := int32(0)

	db := &mockWorkQueueDB{
		claimFunc: func(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
			count := atomic.AddInt32(&claimCount, 1)
			if count <= 3 {
				return lrdb.WorkQueue{
					ID:             int64(100 + count),
					TaskName:       "test-task",
					OrganizationID: testOrgID,
					InstanceNum:    5,
					Spec:           json.RawMessage(`{}`),
					Tries:          0,
					ClaimedBy:      123,
				}, nil
			}
			return lrdb.WorkQueue{}, pgx.ErrNoRows
		},
	}
	mgr := workqueue.NewManager(db, 123, "test-task")

	processedCount := int32(0)
	processor := &mockBundleProcessor{
		processFunc: func(ctx context.Context, workItem workqueue.Workable) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	}

	consumer := NewQueueWorkerConsumer(mgr, processor, "test-task")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for all work to be processed
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedCount) >= 3
	}, 2*time.Second, 10*time.Millisecond)

	// Cancel and wait for shutdown
	cancel()
	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	// Verify all work was completed
	db.mu.Lock()
	assert.Len(t, db.completedWorkIDs, 3)
	db.mu.Unlock()
}
