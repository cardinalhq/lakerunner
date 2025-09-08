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

package accumulation

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestValidateGroupConsistency_ValidGroup(t *testing.T) {
	orgID := uuid.New()
	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101, // Different segment ID is OK
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.NoError(t, err)
}

func TestValidateGroupConsistency_EmptyGroup(t *testing.T) {
	group := &AccumulationGroup[messages.CompactionKey]{
		Key:      messages.CompactionKey{},
		Messages: []*AccumulatedMessage{},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "message_count", validationErr.Field)
	assert.Contains(t, validationErr.Message, "group cannot be empty")
}

func TestValidateGroupConsistency_InconsistentOrganizationID(t *testing.T) {
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	key := messages.CompactionKey{
		OrganizationID: orgID1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID1,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID2, // Different org ID
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "organization_id", validationErr.Field)
	assert.Equal(t, orgID1, validationErr.Expected)
	assert.Equal(t, orgID2, validationErr.Got)
	assert.Contains(t, validationErr.Message, "message 1")
}

func TestValidateGroupConsistency_InconsistentInstanceNum(t *testing.T) {
	orgID := uuid.New()

	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    2, // Different instance
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "instance_num", validationErr.Field)
	assert.Equal(t, int16(1), validationErr.Expected)
	assert.Equal(t, int16(2), validationErr.Got)
}

func TestValidateGroupConsistency_InconsistentDateInt(t *testing.T) {
	orgID := uuid.New()

	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250107, // Different date
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "date_int", validationErr.Field)
	assert.Equal(t, int32(20250108), validationErr.Expected)
	assert.Equal(t, int32(20250107), validationErr.Got)
}

func TestValidateGroupConsistency_InconsistentFrequencyMs(t *testing.T) {
	orgID := uuid.New()

	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    30000, // Different frequency
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "frequency_ms", validationErr.Field)
	assert.Equal(t, int32(60000), validationErr.Expected)
	assert.Equal(t, int32(30000), validationErr.Got)
}

func TestValidateGroupConsistency_FirstMessageInconsistent(t *testing.T) {
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	key := messages.CompactionKey{
		OrganizationID: orgID1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{
					OrganizationID: orgID2, // First message is inconsistent
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "organization_id", validationErr.Field)
	assert.Contains(t, validationErr.Message, "message 0")
}

// MockMessage implements GroupableMessage but isn't a MetricCompactionMessage
type MockMessage struct {
	key messages.CompactionKey
}

func (m *MockMessage) GroupingKey() any {
	return m.key
}

func (m *MockMessage) RecordCount() int64 {
	return 1
}

func TestValidateGroupConsistency_WrongMessageType(t *testing.T) {
	key := messages.CompactionKey{
		OrganizationID: uuid.New(),
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[messages.CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &MockMessage{key: key}, // Wrong message type
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "message_type", validationErr.Field)
	assert.Contains(t, validationErr.Message, "message 0 is not a MetricCompactionMessage")
}

func TestGroupValidationError_Error(t *testing.T) {
	err := &GroupValidationError{
		Field:    "test_field",
		Expected: "expected_value",
		Got:      "actual_value",
		Message:  "test message",
	}

	expectedErrorString := "group validation failed - test_field: expected expected_value, got actual_value (test message)"
	assert.Equal(t, expectedErrorString, err.Error())
}

// Mock implementations for testing writeFromReader

type mockReader struct {
	mock.Mock
}

func (m *mockReader) Next(ctx context.Context) (*pipeline.Batch, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pipeline.Batch), args.Error(1)
}

func (m *mockReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockReader) TotalRowsReturned() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) WriteBatch(batch *pipeline.Batch) error {
	args := m.Called(batch)
	return args.Error(0)
}

func (m *mockWriter) Close(ctx context.Context) ([]parquetwriter.Result, error) {
	args := m.Called(ctx)
	return args.Get(0).([]parquetwriter.Result), args.Error(1)
}

func (m *mockWriter) Abort() {
	m.Called()
}

func (m *mockWriter) Config() parquetwriter.WriterConfig {
	args := m.Called()
	return args.Get(0).(parquetwriter.WriterConfig)
}

func (m *mockWriter) GetCurrentStats() parquetwriter.WriterStats {
	args := m.Called()
	return args.Get(0).(parquetwriter.WriterStats)
}

func TestWriteFromReader_Success(t *testing.T) {
	compactor := &MetricCompactorProcessor{cfg: &config.Config{}}

	mockReader := &mockReader{}
	mockWriter := &mockWriter{}

	// Create mock batches
	batch1 := &pipeline.Batch{}
	batch2 := &pipeline.Batch{}

	// Set up expectations
	mockReader.On("Next", mock.Anything).Return(batch1, nil).Once()
	mockReader.On("Next", mock.Anything).Return(batch2, nil).Once()
	mockReader.On("Next", mock.Anything).Return(nil, io.EOF).Once()

	mockWriter.On("WriteBatch", batch1).Return(nil).Once()
	mockWriter.On("WriteBatch", batch2).Return(nil).Once()

	// Execute
	err := compactor.writeFromReader(context.Background(), mockReader, mockWriter)

	// Verify
	assert.NoError(t, err)
	mockReader.AssertExpectations(t)
	mockWriter.AssertExpectations(t)
}

func TestWriteFromReader_ReaderError(t *testing.T) {
	compactor := &MetricCompactorProcessor{cfg: &config.Config{}}

	mockReader := &mockReader{}
	mockWriter := &mockWriter{}

	expectedError := fmt.Errorf("reader error")

	// Set up expectations
	mockReader.On("Next", mock.Anything).Return(nil, expectedError).Once()

	// Execute
	err := compactor.writeFromReader(context.Background(), mockReader, mockWriter)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read batch: reader error")
	mockReader.AssertExpectations(t)
	mockWriter.AssertExpectations(t)
}

func TestWriteFromReader_WriterError(t *testing.T) {
	compactor := &MetricCompactorProcessor{cfg: &config.Config{}}

	mockReader := &mockReader{}
	mockWriter := &mockWriter{}

	batch := &pipeline.Batch{}
	expectedError := fmt.Errorf("writer error")

	// Set up expectations
	mockReader.On("Next", mock.Anything).Return(batch, nil).Once()
	mockWriter.On("WriteBatch", batch).Return(expectedError).Once()

	// Execute
	err := compactor.writeFromReader(context.Background(), mockReader, mockWriter)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "write batch: writer error")
	mockReader.AssertExpectations(t)
	mockWriter.AssertExpectations(t)
}

func TestWriteFromReader_EmptyReader(t *testing.T) {
	compactor := &MetricCompactorProcessor{cfg: &config.Config{}}

	mockReader := &mockReader{}
	mockWriter := &mockWriter{}

	// Set up expectations - reader immediately returns EOF
	mockReader.On("Next", mock.Anything).Return(nil, io.EOF).Once()

	// Execute
	err := compactor.writeFromReader(context.Background(), mockReader, mockWriter)

	// Verify
	assert.NoError(t, err)
	mockReader.AssertExpectations(t)
	mockWriter.AssertExpectations(t)
}
