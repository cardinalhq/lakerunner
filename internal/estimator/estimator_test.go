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
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// ---- Test doubles ----

type fakeQuerier struct {
	metricRows []lrdb.MetricSegEstimatorRow
	logRows    []lrdb.LogSegEstimatorRow
	errMetrics error
	errLogs    error
}

func (f *fakeQuerier) MetricSegEstimator(ctx context.Context, _ lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error) {
	return f.metricRows, f.errMetrics
}
func (f *fakeQuerier) LogSegEstimator(ctx context.Context, _ lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error) {
	return f.logRows, f.errLogs
}

// ---- Helpers ----

func mk() (orgA, orgB uuid.UUID) {
	return uuid.New(), uuid.New()
}

func newBareEstimator() *estimator {
	return &estimator{
		currentEstimates: make(map[estimatorKey]Estimate),
		defaultGuess:     40_000,
	}
}

// ---- Tests for Get() fallback behavior ----

func TestGet_ExactMatchPositive(t *testing.T) {
	orgA, _ := mk()
	e := newBareEstimator()
	key := estimatorKey{OrganizationID: orgA, InstanceNum: 1, Signal: lrdb.SignalEnumMetrics}
	e.currentEstimates[key] = Estimate{EstimatedRecordCount: 123}

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != 123 {
		t.Fatalf("want 123, got %d", got.EstimatedRecordCount)
	}
}

func TestGet_ExactMatchZero_FallsBackToBroaderTiers(t *testing.T) {
	orgA, _ := mk()
	e := newBareEstimator()
	// exact is zero
	e.currentEstimates[estimatorKey{orgA, 1, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 0}
	// org+sig has positives
	e.currentEstimates[estimatorKey{orgA, 2, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 100}
	e.currentEstimates[estimatorKey{orgA, 3, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 200}

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != 150 {
		t.Fatalf("want org+sig avg 150, got %d", got.EstimatedRecordCount)
	}
}

func TestGet_FallsBackToSigAverage(t *testing.T) {
	orgA, orgB := mk()
	e := newBareEstimator()
	// no orgA data for metrics; but signal-wide exists for other org
	e.currentEstimates[estimatorKey{orgB, 7, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 300}
	e.currentEstimates[estimatorKey{orgB, 8, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 100}

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != 200 {
		t.Fatalf("want sig avg 200, got %d", got.EstimatedRecordCount)
	}
}

func TestGet_FallsBackToGlobalAverage(t *testing.T) {
	orgA, orgB := mk()
	e := newBareEstimator()
	// Only logs exist globally; no metrics at all → should use global (logs+metrics combined)
	e.currentEstimates[estimatorKey{orgB, 2, lrdb.SignalEnumLogs}] = Estimate{EstimatedRecordCount: 90}
	e.currentEstimates[estimatorKey{orgA, 3, lrdb.SignalEnumLogs}] = Estimate{EstimatedRecordCount: 110}

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != 100 {
		t.Fatalf("want global avg 100, got %d", got.EstimatedRecordCount)
	}
}

func TestGet_DefaultGuess_WhenEmpty(t *testing.T) {
	orgA, _ := mk()
	e := newBareEstimator() // empty snapshot

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != e.defaultGuess {
		t.Fatalf("want defaultGuess %d, got %d", e.defaultGuess, got.EstimatedRecordCount)
	}
}

func TestGet_DefaultGuess_WhenAllAveragesZeroOrNegative(t *testing.T) {
	orgA, orgB := mk()
	e := newBareEstimator()
	// exact missing; org+sig zeros
	e.currentEstimates[estimatorKey{orgA, 2, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 0}
	// sig zeros
	e.currentEstimates[estimatorKey{orgB, 3, lrdb.SignalEnumMetrics}] = Estimate{EstimatedRecordCount: 0}
	// global negative + zero (defensive case)
	e.currentEstimates[estimatorKey{orgB, 4, lrdb.SignalEnumLogs}] = Estimate{EstimatedRecordCount: -10}

	got := e.Get(orgA, 1, lrdb.SignalEnumMetrics)
	if got.EstimatedRecordCount != e.defaultGuess {
		t.Fatalf("want defaultGuess %d, got %d", e.defaultGuess, got.EstimatedRecordCount)
	}
}

// ---- Tests for updateEstimates + NewEstimator ----

func TestUpdateEstimates_LoadsMetricAndLogRows(t *testing.T) {
	orgA, orgB := mk()
	fq := &fakeQuerier{
		metricRows: []lrdb.MetricSegEstimatorRow{
			{OrganizationID: orgA, InstanceNum: 1, EstimatedRecords: 111},
		},
		logRows: []lrdb.LogSegEstimatorRow{
			{OrganizationID: orgB, InstanceNum: 2, EstimatedRecords: 222},
		},
	}
	e := &estimator{
		currentEstimates: make(map[estimatorKey]Estimate),
		lookback:         time.Hour,   // keep small to avoid time flake
		timeout:          time.Second, // quick timeout
		updateEvery:      time.Hour,   // unused here
		defaultGuess:     40_000,
	}
	ctx := context.Background()
	if err := e.updateEstimates(ctx, fq); err != nil {
		t.Fatalf("updateEstimates error: %v", err)
	}

	if got := e.currentEstimates[estimatorKey{orgA, 1, lrdb.SignalEnumMetrics}].EstimatedRecordCount; got != 111 {
		t.Fatalf("metrics row not loaded: want 111, got %d", got)
	}
	if got := e.currentEstimates[estimatorKey{orgB, 2, lrdb.SignalEnumLogs}].EstimatedRecordCount; got != 222 {
		t.Fatalf("logs row not loaded: want 222, got %d", got)
	}
}

func TestNewEstimator_PerformsInitialLoad(t *testing.T) {
	orgA, _ := mk()
	fq := &fakeQuerier{
		metricRows: []lrdb.MetricSegEstimatorRow{
			{OrganizationID: orgA, InstanceNum: 5, EstimatedRecords: 555},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	est, err := NewEstimator(ctx, fq)
	if err != nil {
		t.Fatalf("NewEstimator error: %v", err)
	}

	// We need to inspect the concrete type to peek at the map
	impl, ok := est.(*estimator)
	if !ok {
		t.Fatalf("unexpected estimator type")
	}
	if got := impl.currentEstimates[estimatorKey{orgA, 5, lrdb.SignalEnumMetrics}].EstimatedRecordCount; got != 555 {
		t.Fatalf("initial load missing: want 555, got %d", got)
	}
}

// ---- dateint sanity ----

func TestDateint_UTCStable(t *testing.T) {
	// Jan 02, 2025
	ts := time.Date(2025, 1, 2, 15, 4, 5, 0, time.UTC)
	if got := dateint(ts); got != 20250102 {
		t.Fatalf("want 20250102, got %d", got)
	}
}
