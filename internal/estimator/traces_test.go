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

package estimator

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestTraceEstimator_Get(t *testing.T) {
	// Create a trace estimator with some test data
	e := &traceEstimator{
		currentEstimates: map[traceEstimatorKey]int64{
			{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000001")}: 50000,
			{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000002")}: 75000,
		},
		defaultGuess: 40000,
	}

	tests := []struct {
		name           string
		organizationID uuid.UUID
		instanceNum    int16
		expected       int64
	}{
		{
			name:           "exact match",
			organizationID: uuid.MustParse("12340000-0000-4000-8000-000000000001"),
			instanceNum:    1,
			expected:       50000,
		},
		{
			name:           "fallback to average of all",
			organizationID: uuid.MustParse("12340000-0000-4000-8000-000000000099"),
			instanceNum:    1,
			expected:       62500, // (50000 + 75000) / 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := e.Get(tt.organizationID, tt.instanceNum)
			if result != tt.expected {
				t.Errorf("Get() = %d, want %d", result, tt.expected)
			}
		})
	}
}

type mockTraceQuerier struct {
	traceRows []lrdb.TraceSegEstimatorRow
	traceErr  error
}

func (m *mockTraceQuerier) MetricSegEstimator(ctx context.Context, params lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error) {
	return nil, nil
}

func (m *mockTraceQuerier) LogSegEstimator(ctx context.Context, params lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error) {
	return nil, nil
}

func (m *mockTraceQuerier) TraceSegEstimator(ctx context.Context, params lrdb.TraceSegEstimatorParams) ([]lrdb.TraceSegEstimatorRow, error) {
	return m.traceRows, m.traceErr
}

func TestTraceEstimator_updateEstimates(t *testing.T) {
	e := &traceEstimator{
		currentEstimates: map[traceEstimatorKey]int64{},
		defaultGuess:     40000,
	}

	querier := &mockTraceQuerier{
		traceRows: []lrdb.TraceSegEstimatorRow{
			{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000001"), EstimatedRecords: 50000},
			{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000002"), EstimatedRecords: 75000},
		},
	}

	err := e.updateEstimates(context.Background(), querier)
	if err != nil {
		t.Fatalf("updateEstimates() failed: %v", err)
	}

	// Check that estimates were updated
	if len(e.currentEstimates) != 2 {
		t.Errorf("Expected 2 estimates, got %d", len(e.currentEstimates))
	}

	key1 := traceEstimatorKey{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000001")}
	if e.currentEstimates[key1] != 50000 {
		t.Errorf("Expected estimate 50000 for org 1, got %d", e.currentEstimates[key1])
	}

	key2 := traceEstimatorKey{OrganizationID: uuid.MustParse("12340000-0000-4000-8000-000000000002")}
	if e.currentEstimates[key2] != 75000 {
		t.Errorf("Expected estimate 75000 for org 2, got %d", e.currentEstimates[key2])
	}
}
