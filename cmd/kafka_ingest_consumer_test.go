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

package cmd

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockKafkaJournalDB is a mock implementation that only implements the KafkaJournal methods
type mockKafkaJournalDB struct {
	mock.Mock
}

func (m *mockKafkaJournalDB) KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockKafkaJournalDB) KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

// We no longer need mockIngestLoopContext since we use focused interfaces

func TestKafkaIngestConsumer_shouldProcessMessage(t *testing.T) {
	// Create a context with a logger attached
	logger := slog.Default().With("test", "KafkaIngestConsumer_shouldProcessMessage")
	ctx := logctx.WithLogger(context.Background(), logger)
	consumerGroup := "test-group"
	topic := "test-topic"

	tests := []struct {
		name           string
		partition      int
		offset         int64
		dbOffset       int64
		dbError        error
		expectedResult bool
		expectedError  string
		// State setup
		existingState  *PartitionState
		expectedDBCall bool
	}{
		{
			name:           "first message for partition - no DB record",
			partition:      0,
			offset:         100,
			dbError:        sql.ErrNoRows,
			expectedResult: true,
			expectedDBCall: true,
		},
		{
			name:           "first message for partition - with DB record",
			partition:      0,
			offset:         150,
			dbOffset:       100,
			expectedResult: true,
			expectedDBCall: true,
		},
		{
			name:           "message already processed",
			partition:      0,
			offset:         100,
			dbOffset:       150,
			expectedResult: false,
			expectedDBCall: true,
		},
		{
			name:           "sequential message - no DB check needed",
			partition:      0,
			offset:         101,
			expectedResult: true,
			existingState:  &PartitionState{lastSeenOffset: 100, lastKnownOffset: 90, needsDBCheck: false},
			expectedDBCall: false,
		},
		{
			name:           "gap detected - triggers DB check",
			partition:      0,
			offset:         105,
			dbOffset:       100,
			expectedResult: true,
			existingState:  &PartitionState{lastSeenOffset: 100, lastKnownOffset: 90, needsDBCheck: false},
			expectedDBCall: true,
		},
		{
			name:           "DB error - not no rows",
			partition:      0,
			offset:         100,
			dbError:        assert.AnError,
			expectedResult: false,
			expectedError:  "failed to get last processed offset",
			expectedDBCall: true,
		},
		{
			name:           "partition needs DB check flag set",
			partition:      0,
			offset:         100,
			dbOffset:       50,
			expectedResult: true,
			existingState:  &PartitionState{lastSeenOffset: 99, lastKnownOffset: 0, needsDBCheck: true},
			expectedDBCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock DB
			mockDB := new(mockKafkaJournalDB)

			// Setup expectations
			if tt.expectedDBCall {
				call := mockDB.On("KafkaJournalGetLastProcessed", ctx, lrdb.KafkaJournalGetLastProcessedParams{
					ConsumerGroup: consumerGroup,
					Topic:         topic,
					Partition:     int32(tt.partition),
				})

				if tt.dbError != nil {
					call.Return(int64(0), tt.dbError)
				} else {
					call.Return(tt.dbOffset, nil)
				}
			}

			// Create consumer with mock using focused interface
			consumer := &KafkaIngestConsumer{
				consumerGroup:   consumerGroup,
				topic:           topic,
				partitionStates: make(map[int]*PartitionState),
				kafkaJournalDB:  mockDB,
				loop:            &IngestLoopContext{}, // Empty loop context since we're using focused interface
			}

			// Set up existing state if provided
			if tt.existingState != nil {
				consumer.partitionStates[tt.partition] = tt.existingState
			}

			// Execute test
			result, err := consumer.shouldProcessMessage(ctx, tt.partition, tt.offset)

			// Verify results
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}

			// Verify state updates
			state := consumer.partitionStates[tt.partition]
			require.NotNil(t, state)

			if tt.expectedError == "" {
				// Only check state updates if there was no error
				assert.Equal(t, tt.offset, state.lastSeenOffset)
			}

			if tt.expectedDBCall {
				if tt.dbError == sql.ErrNoRows {
					assert.Equal(t, int64(-1), state.lastKnownOffset)
					assert.False(t, state.needsDBCheck)
				} else if tt.dbError == nil {
					assert.Equal(t, tt.dbOffset, state.lastKnownOffset)
					assert.False(t, state.needsDBCheck)
				} else {
					// For database errors, state should not be fully updated
					assert.True(t, state.needsDBCheck) // Should remain true since DB check failed
				}
			}

			// Verify mock expectations
			mockDB.AssertExpectations(t)
		})
	}
}

func TestKafkaIngestConsumer_shouldProcessMessage_Concurrency(t *testing.T) {
	// Create a context with a logger attached
	logger := slog.Default().With("test", "KafkaIngestConsumer_shouldProcessMessage_Concurrency")
	ctx := logctx.WithLogger(context.Background(), logger)
	consumerGroup := "test-group"
	topic := "test-topic"
	partition := 0

	// Setup mock DB that returns no previous record
	mockDB := new(mockKafkaJournalDB)
	mockDB.On("KafkaJournalGetLastProcessed", ctx, lrdb.KafkaJournalGetLastProcessedParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		Partition:     int32(partition),
	}).Return(int64(0), sql.ErrNoRows)

	consumer := &KafkaIngestConsumer{
		consumerGroup:   consumerGroup,
		topic:           topic,
		partitionStates: make(map[int]*PartitionState),
		kafkaJournalDB:  mockDB,
		loop:            &IngestLoopContext{}, // Empty loop context since we're using focused interface
	}

	// Test concurrent access to the same partition
	const numGoroutines = 10
	const startOffset = 100

	results := make([]bool, numGoroutines)
	errors := make([]error, numGoroutines)

	// Use channels to synchronize goroutine starts
	startChan := make(chan struct{})
	doneChan := make(chan int, numGoroutines)

	// Launch goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			<-startChan // Wait for signal to start
			result, err := consumer.shouldProcessMessage(ctx, partition, int64(startOffset+index))
			results[index] = result
			errors[index] = err
			doneChan <- index
		}(i)
	}

	// Signal all goroutines to start at once
	close(startChan)

	// Wait for all to complete
	for i := 0; i < numGoroutines; i++ {
		<-doneChan
	}

	// Verify all calls succeeded (no race conditions)
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errors[i], "Goroutine %d had error", i)
		assert.True(t, results[i], "Goroutine %d should process message", i)
	}

	// Verify final state
	state := consumer.partitionStates[partition]
	require.NotNil(t, state)
	// The final lastSeenOffset should be one of the processed offsets
	assert.Contains(t, []int64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}, state.lastSeenOffset)

	mockDB.AssertExpectations(t)
}
