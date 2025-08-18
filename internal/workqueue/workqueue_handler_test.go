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
	"errors"
	"testing"
	"time"
)

// MockWorkQueueStore is a test double for WorkQueueStore
type MockWorkQueueStore struct {
	CompleteWorkCalled bool
	CompleteWorkError  error
	LastID             int64
	LastWorkerID       int64

	FailWorkCalled bool
	FailWorkError  error
}

func (m *MockWorkQueueStore) CompleteWork(ctx context.Context, id, workerID int64) error {
	m.CompleteWorkCalled = true
	m.LastID = id
	m.LastWorkerID = workerID
	return m.CompleteWorkError
}

func (m *MockWorkQueueStore) FailWork(ctx context.Context, id, workerID int64, maxRetries int32, requeueTTL time.Duration) error {
	m.FailWorkCalled = true
	m.LastID = id
	m.LastWorkerID = workerID
	return m.FailWorkError
}

func TestWorkqueueHandler_CompleteWork_Success(t *testing.T) {
	mockStore := &MockWorkQueueStore{}
	workItem := WorkItem{ID: 123, WorkerID: 456}
	handler := NewWorkqueueHandler(workItem, mockStore)

	ctx := context.Background()
	err := handler.CompleteWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !mockStore.CompleteWorkCalled {
		t.Error("Expected CompleteWork to be called")
	}
	if mockStore.LastID != 123 {
		t.Errorf("Expected ID 123, got %d", mockStore.LastID)
	}
	if mockStore.LastWorkerID != 456 {
		t.Errorf("Expected WorkerID 456, got %d", mockStore.LastWorkerID)
	}
}

func TestWorkqueueHandler_CompleteWork_Error(t *testing.T) {
	mockStore := &MockWorkQueueStore{
		CompleteWorkError: errors.New("database error"),
	}
	workItem := WorkItem{ID: 123, WorkerID: 456}
	handler := NewWorkqueueHandler(workItem, mockStore)

	ctx := context.Background()
	err := handler.CompleteWork(ctx)

	if err == nil {
		t.Error("Expected an error, got nil")
	}
	if err.Error() != "database error" {
		t.Errorf("Expected 'database error', got %v", err)
	}
	if !mockStore.CompleteWorkCalled {
		t.Error("Expected CompleteWork to be called")
	}
}

func TestWorkqueueHandler_RetryWork_Success(t *testing.T) {
	mockStore := &MockWorkQueueStore{}
	workItem := WorkItem{ID: 789, WorkerID: 101}
	handler := NewWorkqueueHandler(workItem, mockStore)

	ctx := context.Background()
	err := handler.RetryWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !mockStore.FailWorkCalled {
		t.Error("Expected FailWork to be called")
	}
	if mockStore.LastID != 789 {
		t.Errorf("Expected ID 789, got %d", mockStore.LastID)
	}
	if mockStore.LastWorkerID != 101 {
		t.Errorf("Expected WorkerID 101, got %d", mockStore.LastWorkerID)
	}
}

func TestWorkqueueHandler_RetryWork_Error(t *testing.T) {
	mockStore := &MockWorkQueueStore{
		FailWorkError: errors.New("retry failed"),
	}
	workItem := WorkItem{ID: 789, WorkerID: 101}
	handler := NewWorkqueueHandler(workItem, mockStore)

	ctx := context.Background()
	err := handler.RetryWork(ctx)

	if err == nil {
		t.Error("Expected an error, got nil")
	}
	if err.Error() != "retry failed" {
		t.Errorf("Expected 'retry failed', got %v", err)
	}
	if !mockStore.FailWorkCalled {
		t.Error("Expected FailWork to be called")
	}
}
