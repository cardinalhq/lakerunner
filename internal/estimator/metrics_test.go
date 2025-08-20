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
	"errors"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func newBareMetricEstimator() *metricEstimator {
	return &metricEstimator{
		currentEstimates: map[metricEstimatorKey]int64{},
		updateEvery:      time.Hour,
		lookback:         time.Hour,
		timeout:          time.Second,
		defaultGuess:     40_000,
	}
}

type fakeMetricQuerier struct {
	metricRows []lrdb.MetricSegEstimatorRow
	logRows    []lrdb.LogSegEstimatorRow
	errMetrics error
	errLogs    error
}

func (f *fakeMetricQuerier) MetricSegEstimator(ctx context.Context, _ lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error) {
	return f.metricRows, f.errMetrics
}

func (f *fakeMetricQuerier) LogSegEstimator(ctx context.Context, _ lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error) {
	return f.logRows, f.errLogs
}

func TestMetricEstimator_Get_ExactMatch(t *testing.T) {
	orgA, _ := mk()
	e := newBareMetricEstimator()

	key := metricEstimatorKey{OrganizationID: orgA, FrequencyMs: 60000}
	e.currentEstimates[key] = 123

	got := e.Get(orgA, 1, 60000)
	if got != 123 {
		t.Fatalf("want 123, got %d", got)
	}
}

func TestMetricEstimator_Get_AllPaths(t *testing.T) {
	orgA, orgB := mk()

	// Test Path 1: Exact match (org + instance + frequency)
	t.Run("ExactMatch", func(t *testing.T) {
		e := newBareMetricEstimator()
		e.currentEstimates[metricEstimatorKey{orgA, 60000}] = 1234

		got := e.Get(orgA, 1, 60000)
		if got != 1234 {
			t.Fatalf("exact match: want 1234, got %d", got)
		}
	})

	// Test Path 2: Org + frequency average (across instances)
	t.Run("OrgFrequencyAverage", func(t *testing.T) {
		e := newBareMetricEstimator()
		// Same org/frequency - there's only one estimate per org+frequency now
		e.currentEstimates[metricEstimatorKey{orgA, 60000}] = 150

		// Query should return the exact estimate
		got := e.Get(orgA, 1, 60000)
		expected := int64(150)
		if got != int64(expected) {
			t.Fatalf("org+frequency average: want %d, got %d", expected, got)
		}
	})

	// Test Path 3: Org average (across instances and frequencies)
	t.Run("OrgAverage", func(t *testing.T) {
		e := newBareMetricEstimator()
		// Same org but different instances and frequencies
		e.currentEstimates[metricEstimatorKey{orgA, 120000}] = 300
		e.currentEstimates[metricEstimatorKey{orgA, 180000}] = 500

		// Query for non-existing instance and frequency but same org
		got := e.Get(orgA, 1, 60000)
		expected := (300 + 500) / 2
		if got != int64(expected) {
			t.Fatalf("org average: want %d, got %d", expected, got)
		}
	})

	// Test Path 4: Frequency average (across organizations and instances)
	t.Run("FrequencyAverage", func(t *testing.T) {
		e := newBareMetricEstimator()
		// Same frequency but different orgs
		e.currentEstimates[metricEstimatorKey{orgB, 60000}] = 700

		// Query for non-existing org but same frequency
		got := e.Get(orgA, 1, 60000)
		expected := int64(700)
		if got != int64(expected) {
			t.Fatalf("frequency average: want %d, got %d", expected, got)
		}
	})

	// Test Path 5: No match found, falls back to frequency-agnostic
	t.Run("NoMatchFound", func(t *testing.T) {
		e := newBareMetricEstimator() // Fresh estimator with no data

		got := e.Get(orgA, 1, 60000)
		// Should fall back to frequency-agnostic logic, which returns default guess when no data
		if got != e.defaultGuess {
			t.Fatalf("no match should fallback to default: want %d, got %d", e.defaultGuess, got)
		}
	})

	// Test invalid frequency handling
	t.Run("InvalidFrequency", func(t *testing.T) {
		e := newBareMetricEstimator()
		// Set up some data for fallback
		e.currentEstimates[metricEstimatorKey{orgA, 60000}] = 1000

		// Test invalid frequency (0 and negative)
		got1 := e.Get(orgA, 1, 0)
		got2 := e.Get(orgA, 1, -60000)

		// Should fall back to frequency-agnostic estimate
		if got1 != 1000 {
			t.Fatalf("invalid frequency_ms=0 should fallback: want 1000, got %d", got1)
		}
		if got2 != 1000 {
			t.Fatalf("invalid frequency_ms=-60000 should fallback: want 1000, got %d", got2)
		}
	})
}

func TestMetricEstimator_UpdateEstimates_CornerCases(t *testing.T) {
	orgA, orgB := mk()

	// Test case: MetricSegEstimator fails
	t.Run("EstimatorFails", func(t *testing.T) {
		fq := &fakeMetricQuerier{
			errMetrics: errors.New("metric query failed"),
		}
		e := newBareMetricEstimator()

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail when query fails: %v", err)
		}

		// Should keep existing estimates unchanged
		if len(e.currentEstimates) != 0 {
			t.Fatalf("estimates should remain empty when query fails")
		}
	})

	// Test case: No estimates found (empty results)
	t.Run("NoEstimatesFound", func(t *testing.T) {
		fq := &fakeMetricQuerier{
			metricRows: []lrdb.MetricSegEstimatorRow{}, // empty
		}
		e := newBareMetricEstimator()

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail when no estimates found: %v", err)
		}

		// Should keep existing estimates unchanged
		if len(e.currentEstimates) != 0 {
			t.Fatalf("estimates should remain empty when no data found")
		}
	})

	// Test case: All estimates are non-positive
	t.Run("AllEstimatesNonPositive", func(t *testing.T) {
		fq := &fakeMetricQuerier{
			metricRows: []lrdb.MetricSegEstimatorRow{
				{OrganizationID: orgA, FrequencyMs: 60000, EstimatedRecords: 0},
				{OrganizationID: orgB, FrequencyMs: 60000, EstimatedRecords: -100},
			},
		}
		e := newBareMetricEstimator()
		// Add some existing data that should be preserved
		e.currentEstimates[metricEstimatorKey{orgA, 30000}] = 999

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail when all estimates are non-positive: %v", err)
		}

		// Should keep previous estimates unchanged
		if len(e.currentEstimates) != 1 {
			t.Fatalf("should preserve existing estimates when all new ones are non-positive, got %d", len(e.currentEstimates))
		}
		key := metricEstimatorKey{orgA, 30000}
		if est, ok := e.currentEstimates[key]; !ok || est != 999 {
			t.Fatalf("existing estimate should be preserved")
		}
	})

	// Test case: Mix of positive and non-positive estimates
	t.Run("MixedPositiveNonPositive", func(t *testing.T) {
		fq := &fakeMetricQuerier{
			metricRows: []lrdb.MetricSegEstimatorRow{
				{OrganizationID: orgA, FrequencyMs: 60000, EstimatedRecords: 0},    // dropped
				{OrganizationID: orgB, FrequencyMs: 60000, EstimatedRecords: 500},  // kept (changed to orgB to avoid duplicate)
				{OrganizationID: orgB, FrequencyMs: 30000, EstimatedRecords: -100}, // dropped
			},
		}
		e := newBareMetricEstimator()

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail: %v", err)
		}

		// Should have only the positive estimates (1 total)
		if len(e.currentEstimates) != 1 {
			t.Fatalf("should have 1 positive estimate, got %d", len(e.currentEstimates))
		}

		// Check metric estimate - should be orgB, 60000 now
		metricKey := metricEstimatorKey{orgB, 60000}
		if est, ok := e.currentEstimates[metricKey]; !ok || est != 500 {
			t.Fatalf("positive metric estimate not found or incorrect")
		}
	})
}

func TestNewMetricEstimator(t *testing.T) {
	orgA, _ := mk()
	fq := &fakeMetricQuerier{
		metricRows: []lrdb.MetricSegEstimatorRow{
			{OrganizationID: orgA, FrequencyMs: 60000, EstimatedRecords: 555},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	est, err := NewMetricEstimator(ctx, fq)
	if err != nil {
		t.Fatalf("NewMetricEstimator error: %v", err)
	}

	// Test that it loads initial data
	got := est.Get(orgA, 5, 60000)
	if got != 555 {
		t.Fatalf("initial load missing: want 555, got %d", got)
	}
}
