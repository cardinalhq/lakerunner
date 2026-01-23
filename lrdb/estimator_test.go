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

package lrdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockEstimatorStore implements estimatorStore for testing
type MockEstimatorStore struct {
	mock.Mock
}

func (m *MockEstimatorStore) GetAllPackEstimates(ctx context.Context) ([]GetAllPackEstimatesRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]GetAllPackEstimatesRow), args.Error(1)
}

func (m *MockEstimatorStore) GetAllBySignal(ctx context.Context, signal string) ([]GetAllBySignalRow, error) {
	args := m.Called(ctx, signal)
	return args.Get(0).([]GetAllBySignalRow), args.Error(1)
}

func (m *MockEstimatorStore) GetMetricPackEstimates(ctx context.Context) ([]GetMetricPackEstimatesRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]GetMetricPackEstimatesRow), args.Error(1)
}

func TestNewMetricPackEstimator(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)

	assert.NotNil(t, estimator)
	assert.NotNil(t, estimator.cache)
	assert.Equal(t, mockStore, estimator.db)

	// Clean up
	estimator.Stop()
}

func TestMetricPackEstimator_Get_HardcodedFallback(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	// Setup mock to return error
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return([]GetAllBySignalRow{}, errors.New("database error"))

	orgID := uuid.New()
	result := estimator.Get(context.Background(), orgID, 60000)

	// Should return hardcoded fallback
	assert.Equal(t, int64(40000), result)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_Get_OrganizationSpecificEstimate(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	expectedEstimate := int64(1500000)
	frequency := int32(60000)

	// Setup mock data with org-specific estimate
	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  &expectedEstimate,
			UpdatedAt:      time.Now(),
		},
	}
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	result := estimator.Get(context.Background(), orgID, frequency)

	assert.Equal(t, expectedEstimate, result)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_Get_UUIDZeroFallback(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	zeroUUID := uuid.UUID{}
	defaultEstimate := int64(2000000)
	frequency := int32(60000)

	// Setup mock data with only UUID zero default (no org-specific)
	estimates := []GetAllBySignalRow{
		{
			OrganizationID: zeroUUID, // Default for all orgs
			FrequencyMs:    frequency,
			TargetRecords:  &defaultEstimate,
			UpdatedAt:      time.Now(),
		},
	}
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	result := estimator.Get(context.Background(), orgID, frequency)

	assert.Equal(t, defaultEstimate, result)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_Get_UltimateFallback(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	frequency := int32(60000)

	// Setup mock data with no matching estimates
	estimates := []GetAllBySignalRow{
		{
			OrganizationID: uuid.New(), // Different org
			FrequencyMs:    300000,     // Different frequency
			TargetRecords:  func() *int64 { v := int64(1000000); return &v }(),
			UpdatedAt:      time.Now(),
		},
	}
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	result := estimator.Get(context.Background(), orgID, frequency)

	// Should return ultimate fallback
	assert.Equal(t, int64(40000), result)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_Get_NilTargetRecords(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	frequency := int32(60000)

	// Setup mock data with nil TargetRecords (should be ignored)
	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  nil, // Should be ignored
			UpdatedAt:      time.Now(),
		},
	}
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	result := estimator.Get(context.Background(), orgID, frequency)

	// Should return ultimate fallback since nil TargetRecords is ignored
	assert.Equal(t, int64(40000), result)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_Caching(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	expectedEstimate := int64(1500000)
	frequency := int32(60000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  &expectedEstimate,
			UpdatedAt:      time.Now(),
		},
	}

	// Mock should be called only once due to caching
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil).Once()

	// First call - should hit database
	result1 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result1)

	// Second call - should hit cache
	result2 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result2)

	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_CachingMultipleOrganizations(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	org1 := uuid.New()
	org2 := uuid.New()
	zeroUUID := uuid.UUID{}

	estimate1 := int64(1000000)
	estimate2 := int64(2000000)
	defaultEstimate := int64(500000)
	frequency := int32(60000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: org1,
			FrequencyMs:    frequency,
			TargetRecords:  &estimate1,
			UpdatedAt:      time.Now(),
		},
		{
			OrganizationID: org2,
			FrequencyMs:    frequency,
			TargetRecords:  &estimate2,
			UpdatedAt:      time.Now(),
		},
		{
			OrganizationID: zeroUUID,
			FrequencyMs:    frequency,
			TargetRecords:  &defaultEstimate,
			UpdatedAt:      time.Now(),
		},
	}

	// Should be called once since we cache all estimates for the signal
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil).Once()

	// First org - should cache org1, org2, and zero UUID estimates
	result1 := estimator.Get(context.Background(), org1, frequency)
	assert.Equal(t, estimate1, result1)

	// Same org, different call - should hit cache
	result1Again := estimator.Get(context.Background(), org1, frequency)
	assert.Equal(t, estimate1, result1Again)

	// Different org - should hit database again but cache zero UUID estimate
	result2 := estimator.Get(context.Background(), org2, frequency)
	assert.Equal(t, estimate2, result2)

	// Same second org - should hit cache now
	result2Again := estimator.Get(context.Background(), org2, frequency)
	assert.Equal(t, estimate2, result2Again)

	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_ConcurrentAccess(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	expectedEstimate := int64(1500000)
	frequency := int32(60000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  &expectedEstimate,
			UpdatedAt:      time.Now(),
		},
	}

	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	// Test concurrent access
	const numGoroutines = 10
	var wg sync.WaitGroup
	results := make([]int64, numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			defer wg.Done()
			results[index] = estimator.Get(context.Background(), orgID, frequency)
		}(i)
	}

	wg.Wait()

	// All results should be the same
	for i, result := range results {
		assert.Equal(t, expectedEstimate, result, "Result %d should match expected estimate", i)
	}

	// Verify that the mock was called at least once (due to caching, might be called fewer times than goroutines)
	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_ClearCache(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	expectedEstimate := int64(1500000)
	frequency := int32(60000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  &expectedEstimate,
			UpdatedAt:      time.Now(),
		},
	}

	// Mock should be called twice - once before clear, once after
	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil).Times(2)

	// First call - populates cache
	result1 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result1)

	// Clear cache
	estimator.ClearCache()

	// Second call after clear - should hit database again
	result2 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result2)

	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_TTLExpiration(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()
	expectedEstimate := int64(1500000)
	frequency := int32(60000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    frequency,
			TargetRecords:  &expectedEstimate,
			UpdatedAt:      time.Now(),
		},
	}

	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil)

	// First call - populates cache
	result1 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result1)

	// Check that item is cached by trying to get it immediately
	result2 := estimator.Get(context.Background(), orgID, frequency)
	assert.Equal(t, expectedEstimate, result2)

	// Note: Testing actual TTL expiration would require waiting 5 minutes or
	// manipulating time, which is complex and slow. The TTL functionality
	// is provided by the ttlcache library which has its own tests.

	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_MultipleFrequencies(t *testing.T) {
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	orgID := uuid.New()

	freq1 := int32(60000)
	freq2 := int32(300000)
	estimate1 := int64(1000000)
	estimate2 := int64(2000000)

	estimates := []GetAllBySignalRow{
		{
			OrganizationID: orgID,
			FrequencyMs:    freq1,
			TargetRecords:  &estimate1,
			UpdatedAt:      time.Now(),
		},
		{
			OrganizationID: orgID,
			FrequencyMs:    freq2,
			TargetRecords:  &estimate2,
			UpdatedAt:      time.Now(),
		},
	}

	mockStore.On("GetAllBySignal", mock.Anything, "metrics").Return(estimates, nil).Once()

	// First frequency
	result1 := estimator.Get(context.Background(), orgID, freq1)
	assert.Equal(t, estimate1, result1)

	// Second frequency - should hit cache since we cache all estimates for the signal at once
	result2 := estimator.Get(context.Background(), orgID, freq2)
	assert.Equal(t, estimate2, result2)

	mockStore.AssertExpectations(t)
}

func TestMetricPackEstimator_InterfaceCompliance(t *testing.T) {
	// Test that packEstimator implements metricEstimator interface
	mockStore := &MockEstimatorStore{}
	estimator := newMetricPackEstimator(mockStore)
	defer estimator.Stop()

	var _ metricEstimator = estimator // Compile-time interface check

	// Runtime interface check
	require.Implements(t, (*metricEstimator)(nil), estimator)
}
