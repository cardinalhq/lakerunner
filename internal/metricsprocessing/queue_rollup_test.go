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

// MockRollupWorkQueuer is a mock implementation of RollupWorkQueuer
type MockRollupWorkQueuer struct {
	mock.Mock
}

func (m *MockRollupWorkQueuer) MrqQueueWork(ctx context.Context, arg lrdb.MrqQueueWorkParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestQueueMetricRollup(t *testing.T) {
	tests := []struct {
		name           string
		organizationID uuid.UUID
		dateint        int32
		frequencyMs    int32
		instanceNum    int16
		slotID         int32
		slotCount      int32
		startTs        int64
		endTs          int64
		mockError      error
		expectedError  string
		shouldQueue    bool
	}{
		{
			name:           "successful queuing - 10s to 1min rollup",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    10000, // 10 seconds -> rolls up to 60 seconds
			instanceNum:    1,
			slotID:         5,
			slotCount:      10,
			startTs:        1703174400000, // 2023-12-21 12:00:00 UTC in milliseconds
			endTs:          1703174410000, // 2023-12-21 12:00:10 UTC in milliseconds
			mockError:      nil,
			expectedError:  "",
			shouldQueue:    true,
		},
		{
			name:           "successful queuing - 1min to 5min rollup",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    60000, // 1 minute -> rolls up to 5 minutes
			instanceNum:    2,
			slotID:         3,
			slotCount:      8,
			startTs:        1703174400000,
			endTs:          1703174460000,
			mockError:      nil,
			expectedError:  "",
			shouldQueue:    true,
		},
		{
			name:           "no rollup needed - 1 hour frequency",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    3600000, // 1 hour has no next rollup level
			instanceNum:    1,
			slotID:         1,
			slotCount:      1,
			startTs:        1703174400000,
			endTs:          1703178000000,
			mockError:      nil,
			expectedError:  "",
			shouldQueue:    false,
		},
		{
			name:           "database error",
			organizationID: uuid.MustParse("12345678-1234-5678-9012-123456789012"),
			dateint:        20231201,
			frequencyMs:    10000,
			instanceNum:    1,
			slotID:         1,
			slotCount:      1,
			startTs:        1703174400000,
			endTs:          1703174410000,
			mockError:      errors.New("database connection error"),
			expectedError:  "failed to queue metric rollup work: database connection error",
			shouldQueue:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := new(MockRollupWorkQueuer)

			if tt.shouldQueue {
				// Calculate expected next frequency
				nextFreq := RollupTo[tt.frequencyMs]
				expectedPriority := int32(0)

				// Set up expectation
				expectedRollupGroup := tt.startTs / int64(nextFreq)
				mockDB.On("MrqQueueWork", mock.Anything, mock.MatchedBy(func(params lrdb.MrqQueueWorkParams) bool {
					return params.OrganizationID == tt.organizationID &&
						params.Dateint == tt.dateint &&
						params.FrequencyMs == tt.frequencyMs &&
						params.InstanceNum == tt.instanceNum &&
						params.SlotID == tt.slotID &&
						params.SlotCount == tt.slotCount &&
						params.RecordCount == 1000 &&
						params.RollupGroup == expectedRollupGroup &&
						params.Priority == expectedPriority
				})).Return(tt.mockError)
			}

			// Call the function
			err := QueueMetricRollup(
				context.Background(),
				mockDB,
				tt.organizationID,
				tt.dateint,
				tt.frequencyMs,
				tt.instanceNum,
				tt.slotID,
				tt.slotCount,
				12345, // segmentID
				1000,  // recordCount
				tt.startTs,
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

func TestQueueMetricRollup_FrequencyMapping(t *testing.T) {
	// Test the rollup frequency mapping
	testCases := []struct {
		sourceFreq int32
		targetFreq int32
		priority   int32
	}{
		{10000, 60000, 0},     // 10s -> 1min
		{60000, 300000, 0},    // 1min -> 5min
		{300000, 1200000, 0},  // 5min -> 20min
		{1200000, 3600000, 0}, // 20min -> 1hour
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("frequency_%d_to_%d", tc.sourceFreq, tc.targetFreq), func(t *testing.T) {
			mockDB := new(MockRollupWorkQueuer)

			expectedRollupGroup := int64(1703174400000) / int64(tc.targetFreq) // startTs / targetFreq
			mockDB.On("MrqQueueWork", mock.Anything, mock.MatchedBy(func(params lrdb.MrqQueueWorkParams) bool {
				return params.FrequencyMs == tc.sourceFreq &&
					params.RecordCount == 2000 &&
					params.RollupGroup == expectedRollupGroup &&
					params.Priority == tc.priority
			})).Return(nil)

			err := QueueMetricRollup(
				context.Background(),
				mockDB,
				uuid.New(),
				20231201,
				tc.sourceFreq,
				1,
				1,
				1,
				67890, // segmentID
				2000,  // recordCount
				1703174400000,
			)

			assert.NoError(t, err)
			mockDB.AssertExpectations(t)
		})
	}
}

func TestQueueMetricRollup_NoRollupForUnknownFrequency(t *testing.T) {
	// Test that unknown frequencies don't queue rollup work
	mockDB := new(MockRollupWorkQueuer)
	// No expectations set - should not call MrqQueueWork

	err := QueueMetricRollup(
		context.Background(),
		mockDB,
		uuid.New(),
		20231201,
		999999, // Unknown frequency
		1,
		1,
		1,
		11111, // segmentID
		3000,  // recordCount
		1703174400000,
	)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t) // Should pass with no expectations
}
