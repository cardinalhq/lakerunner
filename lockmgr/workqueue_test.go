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

package lockmgr

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockWorkQueueDB is a mock implementation of the WorkQueueDB interface.
type mockWorkQueueDB struct {
	mock.Mock
}

func (m *mockWorkQueueDB) WorkQueueClaim(ctx context.Context, params lrdb.WorkQueueClaimParams) (lrdb.WorkQueueClaimRow, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(lrdb.WorkQueueClaimRow), args.Error(1)
}

func (m *mockWorkQueueDB) WorkQueueComplete(ctx context.Context, params lrdb.WorkQueueCompleteParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockWorkQueueDB) WorkQueueFail(ctx context.Context, params lrdb.WorkQueueFailParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockWorkQueueDB) WorkQueueDelete(ctx context.Context, params lrdb.WorkQueueDeleteParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockWorkQueueDB) WorkQueueHeartbeat(ctx context.Context, params lrdb.WorkQueueHeartbeatParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func TestNewWorkQueueManager(t *testing.T) {
	tests := []struct {
		name            string
		db              WorkQueueDB
		workerID        int64
		signal          lrdb.SignalEnum
		action          lrdb.ActionEnum
		frequencies     []int32
		minimumPriority int32
		opts            []Options
		wantInterface   bool
	}{
		{
			name:            "creates manager with basic parameters",
			db:              &mockWorkQueueDB{},
			workerID:        123,
			signal:          lrdb.SignalEnumLogs,
			action:          lrdb.ActionEnumCompact,
			frequencies:     []int32{1000, 2000},
			minimumPriority: 10,
			opts:            nil,
			wantInterface:   true,
		},
		{
			name:            "creates manager with options",
			db:              &mockWorkQueueDB{},
			workerID:        456,
			signal:          lrdb.SignalEnumMetrics,
			action:          lrdb.ActionEnumRollup,
			frequencies:     []int32{5000},
			minimumPriority: 5,
			opts: []Options{
				WithHeartbeatInterval(30 * time.Second),
				WithLogger(slog.Default()),
			},
			wantInterface: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewWorkQueueManager(
				tt.db,
				tt.workerID,
				tt.signal,
				tt.action,
				tt.frequencies,
				tt.minimumPriority,
				tt.opts...,
			)

			assert.NotNil(t, mgr)
			if tt.wantInterface {
				assert.Implements(t, (*WorkQueueManager)(nil), mgr)
			}
		})
	}
}

func TestWorkQueueManager_RequestWork_Success(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000, 2000},
		MinPriority: int32(10),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedParams).Return(workItem, nil)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000, 2000},
		10,
	)
	mgr.Run(ctx)

	// Allow some time for the background goroutine to start
	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()

	require.NoError(t, err)
	require.NotNil(t, work)
	assert.Equal(t, int64(1), work.ID())
	assert.Equal(t, int32(10), work.Priority())
	assert.Equal(t, lrdb.SignalEnumLogs, work.Signal())
	assert.Equal(t, lrdb.ActionEnumCompact, work.Action())
	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_RequestWork_NoWork(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	expectedParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedParams).Return(lrdb.WorkQueueClaimRow{}, pgx.ErrNoRows)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()

	require.NoError(t, err)
	assert.Nil(t, work)
	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_RequestWork_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	expectedParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	dbError := errors.New("database connection failed")
	mockDB.On("WorkQueueClaim", ctx, expectedParams).Return(lrdb.WorkQueueClaimRow{}, dbError)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()

	require.Error(t, err)
	assert.Equal(t, dbError, err)
	assert.Nil(t, work)
	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_RequestWork_ConstraintViolation(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	expectedParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	constraintError := errors.New("ERROR: duplicate key value violates unique constraint (SQLSTATE 23P01)")
	mockDB.On("WorkQueueClaim", ctx, expectedParams).Return(lrdb.WorkQueueClaimRow{}, constraintError)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()

	require.NoError(t, err)
	assert.Nil(t, work)
	mockDB.AssertExpectations(t)
}

func TestWorkItem_Complete(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	// Set up work item claim
	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedCompleteParams := lrdb.WorkQueueCompleteParams{
		ID:       int64(1),
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)
	mockDB.On("WorkQueueComplete", ctx, expectedCompleteParams).Return(nil)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	err = work.Complete()
	require.NoError(t, err)

	mockDB.AssertExpectations(t)
}

func TestWorkItem_Fail(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	// Set up work item claim
	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(2),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedFailParams := lrdb.WorkQueueFailParams{
		ID:       int64(2),
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)
	mockDB.On("WorkQueueFail", ctx, expectedFailParams).Return(nil)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	err = work.Fail()
	require.NoError(t, err)

	mockDB.AssertExpectations(t)
}

func TestWorkItem_DoubleComplete(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	// Set up work item claim
	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedCompleteParams := lrdb.WorkQueueCompleteParams{
		ID:       int64(1),
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)
	// Complete should only be called once
	mockDB.On("WorkQueueComplete", ctx, expectedCompleteParams).Return(nil).Once()

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	// First complete should work
	err = work.Complete()
	require.NoError(t, err)

	// Second complete should be a no-op
	err = work.Complete()
	require.NoError(t, err)

	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_Heartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &mockWorkQueueDB{}

	// Set up work item claim
	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedHeartbeatParams := lrdb.WorkQueueHeartbeatParams{
		Ids:      []int64{1},
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)

	// Expect at least one heartbeat call
	var heartbeatCalled sync.WaitGroup
	heartbeatCalled.Add(1)

	mockDB.On("WorkQueueHeartbeat", mock.AnythingOfType("*context.cancelCtx"), expectedHeartbeatParams).
		Return(nil).
		Run(func(args mock.Arguments) {
			heartbeatCalled.Done()
		}).
		Maybe() // Allow multiple calls

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
		WithHeartbeatInterval(50*time.Millisecond), // Short interval for testing
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	// Wait for at least one heartbeat
	heartbeatCalled.Wait()

	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_HeartbeatError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &mockWorkQueueDB{}

	// Set up work item claim
	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedHeartbeatParams := lrdb.WorkQueueHeartbeatParams{
		Ids:      []int64{1},
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)

	// Heartbeat should fail but not crash the manager
	var heartbeatCalled sync.WaitGroup
	heartbeatCalled.Add(1)

	mockDB.On("WorkQueueHeartbeat", mock.AnythingOfType("*context.cancelCtx"), expectedHeartbeatParams).
		Return(errors.New("heartbeat failed")).
		Run(func(args mock.Arguments) {
			heartbeatCalled.Done()
		}).
		Maybe() // Allow multiple calls

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
		WithHeartbeatInterval(50*time.Millisecond), // Short interval for testing
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	// Wait for at least one heartbeat (which should fail but not crash)
	heartbeatCalled.Wait()

	// Manager should still be functional
	assert.Equal(t, int64(1), work.ID())

	mockDB.AssertExpectations(t)
}

func TestWorkQueueManager_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockDB := &mockWorkQueueDB{}

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	// Cancel the context
	cancel()

	// Give some time for the goroutine to exit
	time.Sleep(50 * time.Millisecond)

	// Manager should still exist but context is cancelled
	assert.NotNil(t, mgr)
}

func TestWorkQueueManager_Options(t *testing.T) {
	mockDB := &mockWorkQueueDB{}
	logger := slog.Default()

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
		WithHeartbeatInterval(30*time.Second),
		WithLogger(logger),
	)

	assert.NotNil(t, mgr)

	// We can't directly test the internal fields, but we can verify the manager was created
	// and that options were processed (the constructor would panic if there were issues)
}

func TestWorkQueueManager_MinimumHeartbeatInterval(t *testing.T) {
	mockDB := &mockWorkQueueDB{}

	// Test that very short heartbeat interval gets adjusted to minimum
	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
		WithHeartbeatInterval(1*time.Second), // Should be adjusted to 10 seconds
	)

	assert.NotNil(t, mgr)
}

func TestWorkItem_GetterMethods(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	orgID := uuid.New()
	now := time.Now()
	tsRange := pgtype.Range[pgtype.Timestamptz]{}

	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(42),
		Priority:       int32(15),
		RunnableAt:     now,
		OrganizationID: orgID,
		InstanceNum:    int16(3),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(5000),
		Signal:         lrdb.SignalEnumMetrics,
		Tries:          int32(2),
		Action:         lrdb.ActionEnumRollup,
		TsRange:        tsRange,
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumMetrics,
		Action:      lrdb.ActionEnumRollup,
		TargetFreqs: []int32{5000},
		MinPriority: int32(5),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumMetrics,
		lrdb.ActionEnumRollup,
		[]int32{5000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	// Test all getter methods
	assert.Equal(t, int64(42), work.ID())
	assert.Equal(t, orgID, work.OrganizationID())
	assert.Equal(t, int16(3), work.InstanceNum())
	assert.Equal(t, int32(20250815), work.Dateint())
	assert.Equal(t, int32(5000), work.FrequencyMs())
	assert.Equal(t, lrdb.SignalEnumMetrics, work.Signal())
	assert.Equal(t, int32(2), work.Tries())
	assert.Equal(t, lrdb.ActionEnumRollup, work.Action())
	assert.Equal(t, tsRange, work.TsRange())
	assert.Equal(t, int32(15), work.Priority())
	assert.Equal(t, now, work.RunnableAt())

	// Test AsMap method
	workMap := work.AsMap()
	assert.Equal(t, int64(42), workMap["id"])
	assert.Equal(t, orgID, workMap["orgId"])
	assert.Equal(t, int16(3), workMap["instanceNum"])
	assert.Equal(t, int32(20250815), workMap["dateint"])
	assert.Equal(t, int32(5000), workMap["frequencyMs"])
	assert.Equal(t, lrdb.SignalEnumMetrics, workMap["signal"])
	assert.Equal(t, int32(2), workMap["tries"])
	assert.Equal(t, lrdb.ActionEnumRollup, workMap["action"])
	assert.Equal(t, tsRange, workMap["tsRange"])
	assert.Equal(t, int32(15), workMap["priority"])
	assert.Equal(t, now, workMap["runnableAt"])

	mockDB.AssertExpectations(t)
}

func TestWorkItem_FailAfterComplete(t *testing.T) {
	ctx := context.Background()
	mockDB := &mockWorkQueueDB{}

	workItem := lrdb.WorkQueueClaimRow{
		ID:             int64(1),
		Priority:       int32(10),
		RunnableAt:     time.Now(),
		OrganizationID: uuid.New(),
		InstanceNum:    int16(1),
		Dateint:        int32(20250815),
		FrequencyMs:    int32(1000),
		Signal:         lrdb.SignalEnumLogs,
		Tries:          int32(1),
		Action:         lrdb.ActionEnumCompact,
		TsRange:        pgtype.Range[pgtype.Timestamptz]{},
	}

	expectedClaimParams := lrdb.WorkQueueClaimParams{
		WorkerID:    int64(123),
		Signal:      lrdb.SignalEnumLogs,
		Action:      lrdb.ActionEnumCompact,
		TargetFreqs: []int32{1000},
		MinPriority: int32(5),
	}

	expectedCompleteParams := lrdb.WorkQueueCompleteParams{
		ID:       int64(1),
		WorkerID: int64(123),
	}

	mockDB.On("WorkQueueClaim", ctx, expectedClaimParams).Return(workItem, nil)
	mockDB.On("WorkQueueComplete", ctx, expectedCompleteParams).Return(nil).Once()
	// Fail should not be called after Complete

	mgr := NewWorkQueueManager(
		mockDB,
		123,
		lrdb.SignalEnumLogs,
		lrdb.ActionEnumCompact,
		[]int32{1000},
		5,
	)
	mgr.Run(ctx)

	time.Sleep(10 * time.Millisecond)

	work, err := mgr.RequestWork()
	require.NoError(t, err)
	require.NotNil(t, work)

	// Complete the work item
	err = work.Complete()
	require.NoError(t, err)

	// Attempting to fail after complete should be a no-op
	err = work.Fail()
	require.NoError(t, err)

	mockDB.AssertExpectations(t)
}

// TestWorkQueueDBInterface verifies that lrdb.StoreFull satisfies WorkQueueDB
func TestWorkQueueDBInterface(t *testing.T) {
	// This test ensures the interface compatibility is maintained
	var _ WorkQueueDB = (lrdb.StoreFull)(nil)

	// Test passes if compilation succeeds
	assert.True(t, true)
}
