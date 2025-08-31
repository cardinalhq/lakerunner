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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// MockCompactionWorkQueuer is a mock implementation of CompactionWorkQueuer
type MockCompactionWorkQueuer struct {
	mock.Mock
}

func (m *MockCompactionWorkQueuer) PutMetricCompactionWork(ctx context.Context, arg lrdb.PutMetricCompactionWorkParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestQueueMetricCompaction(t *testing.T) {
	tests := []struct {
		name           string
		organizationID uuid.UUID
		dateint        int32
		frequencyMs    int32
		instanceNum    int16
		segmentID      int64
		recordCount    int64
		startTs        int64
		endTs          int64
		mockError      error
		expectedError  string
	}{
		{
			name:           "successful queuing",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    10000,
			instanceNum:    1,
			segmentID:      12345,
			recordCount:    1000,
			startTs:        1703174400000, // 2023-12-21 12:00:00 UTC in milliseconds
			endTs:          1703174410000, // 2023-12-21 12:00:10 UTC in milliseconds
			mockError:      nil,
			expectedError:  "",
		},
		{
			name:           "database error",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    10000,
			instanceNum:    1,
			segmentID:      12345,
			recordCount:    1000,
			startTs:        1703174400000,
			endTs:          1703174410000,
			mockError:      errors.New("database connection error"),
			expectedError:  "failed to queue metric compaction work: database connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := new(MockCompactionWorkQueuer)

			// Set up expectation
			mockDB.On("PutMetricCompactionWork", mock.Anything, mock.MatchedBy(func(params lrdb.PutMetricCompactionWorkParams) bool {
				// Verify the parameters passed to the database call
				return params.OrganizationID == tt.organizationID &&
					params.Dateint == tt.dateint &&
					params.FrequencyMs == int64(tt.frequencyMs) &&
					params.InstanceNum == tt.instanceNum &&
					params.SegmentID == tt.segmentID &&
					params.RecordCount == tt.recordCount &&
					params.Priority == GetCompactionPriority(tt.frequencyMs) &&
					params.TsRange.Valid
			})).Return(tt.mockError)

			// Call the function
			err := QueueMetricCompaction(
				context.Background(),
				mockDB,
				tt.organizationID,
				tt.dateint,
				tt.frequencyMs,
				tt.instanceNum,
				tt.segmentID,
				tt.recordCount,
				tt.startTs,
				tt.endTs,
			)

			// Verify results
			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
			}

			// Verify mock expectations
			mockDB.AssertExpectations(t)
		})
	}
}

func TestQueueMetricCompaction_PriorityCalculation(t *testing.T) {
	// Test that different frequencies get different priorities
	testCases := []struct {
		frequencyMs      int32
		expectedPriority int32
	}{
		{10000, 1000},  // 800 + 200
		{60000, 800},   // 600 + 200
		{300000, 600},  // 400 + 200
		{1200000, 400}, // 200 + 200
		{3600000, 200}, // 0 + 200
		{999999, 200},  // Unknown frequency defaults to 0 + 200
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("frequency_%d", tc.frequencyMs), func(t *testing.T) {
			mockDB := new(MockCompactionWorkQueuer)

			mockDB.On("PutMetricCompactionWork", mock.Anything, mock.MatchedBy(func(params lrdb.PutMetricCompactionWorkParams) bool {
				return params.Priority == tc.expectedPriority
			})).Return(nil)

			err := QueueMetricCompaction(
				context.Background(),
				mockDB,
				uuid.New(),
				20231201,
				tc.frequencyMs,
				1,
				12345,
				1000,
				1703174400000,
				1703174410000,
			)

			assert.NoError(t, err)
			mockDB.AssertExpectations(t)
		})
	}
}
