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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

// MockKafkaProducer is a mock implementation of fly.Producer
type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) Send(ctx context.Context, topic string, msg fly.Message) error {
	args := m.Called(ctx, topic, msg)
	return args.Error(0)
}

func (m *MockKafkaProducer) BatchSend(ctx context.Context, topic string, msgs []fly.Message) error {
	args := m.Called(ctx, topic, msgs)
	return args.Error(0)
}

func (m *MockKafkaProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockKafkaProducer) GetPartitionCount(topic string) (int, error) {
	args := m.Called(topic)
	return args.Int(0), args.Error(1)
}

func (m *MockKafkaProducer) SendToPartition(ctx context.Context, topic string, partition int, msg fly.Message) error {
	args := m.Called(ctx, topic, partition, msg)
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
		segmentID      int64
		recordCount    int64
		fileSize       int64
		expectSend     bool
	}{
		{
			name:           "10s frequency should queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    10_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      123,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     true,
		},
		{
			name:           "60s frequency should queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    60_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      124,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     true,
		},
		{
			name:           "300s frequency should queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    300_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      125,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     true,
		},
		{
			name:           "1200s frequency should queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    1_200_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      126,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     true,
		},
		{
			name:           "3600s frequency should not queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    3_600_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      127,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     false,
		},
		{
			name:           "unknown frequency should not queue rollup",
			organizationID: uuid.New(),
			dateint:        20240101,
			frequencyMs:    7_200_000,
			instanceNum:    1,
			slotID:         0,
			slotCount:      1,
			segmentID:      128,
			recordCount:    1000,
			fileSize:       10000,
			expectSend:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := new(MockKafkaProducer)

			if tt.expectSend {
				mockProducer.On("Send", mock.Anything, "lakerunner.segments.metrics.rollup", mock.Anything).Return(nil)
			}

			ctx := context.Background()
			segmentStartTime := time.Now() // Use current time for test
			err := queueMetricRollup(ctx, mockProducer, tt.organizationID, tt.dateint, tt.frequencyMs,
				tt.instanceNum, tt.slotID, tt.slotCount, tt.segmentID, tt.recordCount, tt.fileSize, segmentStartTime)

			assert.NoError(t, err)
			mockProducer.AssertExpectations(t)
		})
	}
}

func TestQueueMetricRollupWithMultipleSegments(t *testing.T) {
	mockProducer := new(MockKafkaProducer)
	ctx := context.Background()
	orgID := uuid.New()

	// Expect 3 sends for the rollup-eligible frequencies
	mockProducer.On("Send", mock.Anything, "lakerunner.segments.metrics.rollup", mock.Anything).Return(nil).Times(3)

	// Queue multiple segments with different frequencies
	frequencies := []int32{10_000, 60_000, 300_000, 3_600_000} // Only first 3 should send
	segmentStartTime := time.Now()
	for i, freq := range frequencies {
		err := queueMetricRollup(ctx, mockProducer, orgID, 20240101, freq, 1, 0, 1, int64(i+100), 1000, 10000, segmentStartTime)
		assert.NoError(t, err)
	}

	mockProducer.AssertExpectations(t)
}

func TestQueueMetricRollupBatch(t *testing.T) {
	mockProducer := new(MockKafkaProducer)
	ctx := context.Background()
	orgID := uuid.New()

	// Set up expectation for each individual send
	mockProducer.On("Send", mock.Anything, "lakerunner.segments.metrics.rollup", mock.Anything).Return(nil).Times(5)

	// Queue a batch of segments with rollup-eligible frequency
	segmentStartTime := time.Now()
	for i := range 5 {
		err := queueMetricRollup(ctx, mockProducer, orgID, 20240101, 10_000, 1, int32(i), 5, int64(i+100), 1000, 10000, segmentStartTime)
		assert.NoError(t, err)
	}

	mockProducer.AssertExpectations(t)
}
