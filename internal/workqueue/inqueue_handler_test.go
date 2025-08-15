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

	"github.com/google/uuid"
)

// MockInqueueStore is a test double for InqueueStore
type MockInqueueStore struct {
	ReleaseWorkCalled bool
	ReleaseWorkError  error

	DeleteWorkCalled bool
	DeleteWorkError  error

	DeleteJournalCalled bool
	DeleteJournalError  error

	UpsertJournalCalled bool
	UpsertJournalError  error
	UpsertJournalResult bool

	LastID        uuid.UUID
	LastClaimedBy int64
	LastOrgID     uuid.UUID
	LastBucket    string
	LastObjectID  string
}

func (m *MockInqueueStore) ReleaseWork(ctx context.Context, id uuid.UUID, claimedBy int64) error {
	m.ReleaseWorkCalled = true
	m.LastID = id
	m.LastClaimedBy = claimedBy
	return m.ReleaseWorkError
}

func (m *MockInqueueStore) DeleteWork(ctx context.Context, id uuid.UUID, claimedBy int64) error {
	m.DeleteWorkCalled = true
	m.LastID = id
	m.LastClaimedBy = claimedBy
	return m.DeleteWorkError
}

func (m *MockInqueueStore) DeleteJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) error {
	m.DeleteJournalCalled = true
	m.LastOrgID = orgID
	m.LastBucket = bucket
	m.LastObjectID = objectID
	return m.DeleteJournalError
}

func (m *MockInqueueStore) UpsertJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) (bool, error) {
	m.UpsertJournalCalled = true
	m.LastOrgID = orgID
	m.LastBucket = bucket
	m.LastObjectID = objectID
	return m.UpsertJournalResult, m.UpsertJournalError
}

func TestInqueueHandler_CompleteWork_Success(t *testing.T) {
	mockStore := &MockInqueueStore{}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          1,
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	err := handler.CompleteWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !mockStore.DeleteWorkCalled {
		t.Error("Expected DeleteWork to be called")
	}
	if mockStore.LastID != itemID {
		t.Errorf("Expected ID %v, got %v", itemID, mockStore.LastID)
	}
	if mockStore.LastClaimedBy != 456 {
		t.Errorf("Expected ClaimedBy 456, got %d", mockStore.LastClaimedBy)
	}
}

func TestInqueueHandler_CompleteWork_Error(t *testing.T) {
	mockStore := &MockInqueueStore{
		DeleteWorkError: errors.New("delete failed"),
	}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          1,
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	err := handler.CompleteWork(ctx)

	if err == nil {
		t.Error("Expected an error, got nil")
	}
	if err.Error() != "delete failed" {
		t.Errorf("Expected 'delete failed', got %v", err)
	}
}

func TestInqueueHandler_RetryWork_BelowRetryLimit(t *testing.T) {
	mockStore := &MockInqueueStore{}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          2, // Below limit of 5
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	err := handler.RetryWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !mockStore.DeleteJournalCalled {
		t.Error("Expected DeleteJournal to be called")
	}
	if !mockStore.ReleaseWorkCalled {
		t.Error("Expected ReleaseWork to be called")
	}
	if mockStore.DeleteWorkCalled {
		t.Error("Expected DeleteWork NOT to be called")
	}
}

func TestInqueueHandler_RetryWork_ExceedsRetryLimit(t *testing.T) {
	mockStore := &MockInqueueStore{}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          5, // At limit of 5, so 5+1 > 5
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	err := handler.RetryWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !mockStore.DeleteJournalCalled {
		t.Error("Expected DeleteJournal to be called")
	}
	if !mockStore.DeleteWorkCalled {
		t.Error("Expected DeleteWork to be called")
	}
	if mockStore.ReleaseWorkCalled {
		t.Error("Expected ReleaseWork NOT to be called")
	}
}

func TestInqueueHandler_IsNewWork_Success(t *testing.T) {
	mockStore := &MockInqueueStore{
		UpsertJournalResult: true,
	}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          1,
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	isNew, err := handler.IsNewWork(ctx)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !isNew {
		t.Error("Expected isNew to be true")
	}
	if !mockStore.UpsertJournalCalled {
		t.Error("Expected UpsertJournal to be called")
	}
	if mockStore.LastOrgID != orgID {
		t.Errorf("Expected OrgID %v, got %v", orgID, mockStore.LastOrgID)
	}
	if mockStore.LastBucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got %s", mockStore.LastBucket)
	}
	if mockStore.LastObjectID != "test-object" {
		t.Errorf("Expected objectID 'test-object', got %s", mockStore.LastObjectID)
	}
}

func TestInqueueHandler_IsNewWork_Error(t *testing.T) {
	mockStore := &MockInqueueStore{
		UpsertJournalError: errors.New("upsert failed"),
	}
	itemID := uuid.New()
	orgID := uuid.New()
	workItem := InqueueItem{
		ID:             itemID,
		OrganizationID: orgID,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		ClaimedBy:      456,
		Tries:          1,
		InstanceNum:    10,
	}
	handler := NewInqueueHandler(workItem, mockStore, 5)

	ctx := context.Background()
	isNew, err := handler.IsNewWork(ctx)

	if err == nil {
		t.Error("Expected an error, got nil")
	}
	if isNew {
		t.Error("Expected isNew to be false when error occurs")
	}
	if err.Error() != "upsert journal failed: upsert failed" {
		t.Errorf("Expected wrapped error message, got %v", err)
	}
}
