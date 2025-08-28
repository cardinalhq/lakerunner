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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func mk() (uuid.UUID, uuid.UUID) {
	return uuid.New(), uuid.New()
}

func newBareLogEstimator() *logEstimator {
	return &logEstimator{
		currentEstimates: map[logEstimatorKey]int64{},
		updateEvery:      time.Hour,
		lookback:         time.Hour,
		timeout:          time.Second,
		defaultGuess:     40_000,
	}
}

type fakeLogQuerier struct {
	metricRows []lrdb.MetricSegEstimatorRow
	logRows    []lrdb.LogSegEstimatorRow
	traceRows  []lrdb.TraceSegEstimatorRow
	errMetrics error
	errLogs    error
	errTraces  error
}

func (f *fakeLogQuerier) MetricSegEstimator(ctx context.Context, _ lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error) {
	return f.metricRows, f.errMetrics
}

func (f *fakeLogQuerier) LogSegEstimator(ctx context.Context, _ lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error) {
	return f.logRows, f.errLogs
}

func (f *fakeLogQuerier) TraceSegEstimator(ctx context.Context, _ lrdb.TraceSegEstimatorParams) ([]lrdb.TraceSegEstimatorRow, error) {
	return f.traceRows, f.errTraces
}

func TestLogEstimator_Get_ExactMatch(t *testing.T) {
	orgA, _ := mk()
	e := newBareLogEstimator()

	key := logEstimatorKey{OrganizationID: orgA}
	e.currentEstimates[key] = 123

	got := e.Get(orgA, 1)
	if got != 123 {
		t.Fatalf("want 123, got %d", got)
	}
}

func TestLogEstimator_Get_FallbackTiers(t *testing.T) {
	orgA, orgB := mk()

	// Test exact match takes priority
	t.Run("ExactMatch", func(t *testing.T) {
		e := newBareLogEstimator()
		e.currentEstimates[logEstimatorKey{orgA}] = 100

		got := e.Get(orgA, 1)
		if got != 100 {
			t.Fatalf("exact match: want 100, got %d", got)
		}
	})

	// Test org average (across instances)
	t.Run("OrgAverage", func(t *testing.T) {
		e := newBareLogEstimator()
		// Same org but different instances
		e.currentEstimates[logEstimatorKey{orgA}] = 100

		// Query should return the estimate for orgA
		got := e.Get(orgA, 1)
		expected := int64(100)
		if got != int64(expected) {
			t.Fatalf("org average: want %d, got %d", expected, got)
		}
	})

	// Test global average (across organizations and instances)
	t.Run("GlobalAverage", func(t *testing.T) {
		e := newBareLogEstimator()
		// Different orgs and instances
		e.currentEstimates[logEstimatorKey{orgB}] = 700

		// Query for non-existing org
		got := e.Get(orgA, 1)
		expected := int64(700)
		if got != int64(expected) {
			t.Fatalf("global average: want %d, got %d", expected, got)
		}
	})

	// Test default guess when no data
	t.Run("DefaultGuess", func(t *testing.T) {
		e := newBareLogEstimator() // Fresh estimator with no data

		got := e.Get(orgA, 1)
		if got != e.defaultGuess {
			t.Fatalf("default guess: want %d, got %d", e.defaultGuess, got)
		}
	})

	// Test non-positive estimates are ignored
	t.Run("IgnoreNonPositive", func(t *testing.T) {
		e := newBareLogEstimator()
		// Mix of positive and non-positive estimates
		e.currentEstimates[logEstimatorKey{orgA}] = 300 // used
		e.currentEstimates[logEstimatorKey{orgB}] = 500 // used for global average

		// Should return the orgA value directly
		got := e.Get(orgA, 9)
		if got != 300 {
			t.Fatalf("should ignore non-positive: want 400, got %d", got)
		}
	})
}

func TestLogEstimator_UpdateEstimates_CornerCases(t *testing.T) {
	orgA, orgB := mk()

	// Test case: LogSegEstimator fails
	t.Run("EstimatorFails", func(t *testing.T) {
		fq := &fakeLogQuerier{
			errLogs: errors.New("log query failed"),
		}
		e := newBareLogEstimator()

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
		fq := &fakeLogQuerier{
			logRows: []lrdb.LogSegEstimatorRow{}, // empty
		}
		e := newBareLogEstimator()

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
		fq := &fakeLogQuerier{
			logRows: []lrdb.LogSegEstimatorRow{
				{OrganizationID: orgA, EstimatedRecords: 0},
				{OrganizationID: orgB, EstimatedRecords: -50},
			},
		}
		e := newBareLogEstimator()
		// Add some existing data that should be preserved
		e.currentEstimates[logEstimatorKey{orgA}] = 999

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail when all estimates are non-positive: %v", err)
		}

		// Should keep previous estimates unchanged
		if len(e.currentEstimates) != 1 {
			t.Fatalf("should preserve existing estimates when all new ones are non-positive, got %d", len(e.currentEstimates))
		}
		key := logEstimatorKey{orgA}
		if est, ok := e.currentEstimates[key]; !ok || est != 999 {
			t.Fatalf("existing estimate should be preserved")
		}
	})

	// Test case: Mix of positive and non-positive estimates
	t.Run("MixedPositiveNonPositive", func(t *testing.T) {
		fq := &fakeLogQuerier{
			logRows: []lrdb.LogSegEstimatorRow{
				{OrganizationID: orgA, EstimatedRecords: -25}, // dropped
				{OrganizationID: orgB, EstimatedRecords: 300}, // kept
				{OrganizationID: orgA, EstimatedRecords: 0},   // dropped (second org A will overwrite first)
			},
		}
		e := newBareLogEstimator()

		err := e.updateEstimates(context.Background(), fq)
		if err != nil {
			t.Fatalf("updateEstimates should not fail: %v", err)
		}

		// Should have only the positive estimates (1 total)
		if len(e.currentEstimates) != 1 {
			t.Fatalf("should have 1 positive estimate, got %d", len(e.currentEstimates))
		}

		// Check log estimate
		logKey := logEstimatorKey{orgB}
		if est, ok := e.currentEstimates[logKey]; !ok || est != 300 {
			t.Fatalf("positive log estimate not found or incorrect")
		}
	})
}

func TestNewLogEstimator(t *testing.T) {
	orgA, _ := mk()
	fq := &fakeLogQuerier{
		logRows: []lrdb.LogSegEstimatorRow{
			{OrganizationID: orgA, EstimatedRecords: 555},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	est, err := NewLogEstimator(ctx, fq)
	if err != nil {
		t.Fatalf("NewLogEstimator error: %v", err)
	}

	// Test that it loads initial data
	got := est.Get(orgA, 5)
	if got != 555 {
		t.Fatalf("initial load missing: want 555, got %d", got)
	}
}
