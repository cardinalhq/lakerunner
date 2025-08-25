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
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type MetricQuerier interface {
	MetricSegEstimator(ctx context.Context, params lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error)
}

type MetricEstimator interface {
	// Get returns an estimate of the number of records that will likely be enough to
	// hit the target size for the output file.
	Get(organizationID uuid.UUID, instanceNum int16, frequencyMs int32) int64
}

type metricEstimatorKey struct {
	OrganizationID uuid.UUID
	FrequencyMs    int32
}

type metricEstimator struct {
	sync.RWMutex
	currentEstimates map[metricEstimatorKey]int64
	updateEvery      time.Duration
	lookback         time.Duration
	timeout          time.Duration
	defaultGuess     int64
}

func NewMetricEstimator(doneCtx context.Context, querier EstimationQuerier) (MetricEstimator, error) {
	e := &metricEstimator{
		currentEstimates: map[metricEstimatorKey]int64{},
		updateEvery:      30 * time.Minute,
		lookback:         6 * time.Hour,
		timeout:          30 * time.Second,
		defaultGuess:     40_000,
	}
	if err := e.updateEstimates(doneCtx, querier); err != nil {
		return nil, err
	}
	go RunUpdateLoop(doneCtx, e, querier, e.updateEvery)
	return e, nil
}

func (e *metricEstimator) Get(org uuid.UUID, inst int16, frequencyMs int32) int64 {
	e.RLock()
	snap := e.currentEstimates
	e.RUnlock()

	// Validate frequency_ms - should always be positive when specified
	if frequencyMs <= 0 {
		slog.Warn("invalid frequency_ms for metrics estimate, falling back to frequency-agnostic estimate",
			"org", org, "inst", inst, "frequencyMs", frequencyMs)
		return e.getFrequencyAgnosticEstimate(snap, org, inst)
	}

	// Try frequency-specific lookup first
	if est := e.getFrequencySpecificEstimate(snap, org, inst, frequencyMs); est > 0 {
		return est
	}

	// Fallback to frequency-agnostic logic when no frequency-specific data is available
	return e.getFrequencyAgnosticEstimate(snap, org, inst)
}

func (e *metricEstimator) getFrequencySpecificEstimate(snap map[metricEstimatorKey]int64, org uuid.UUID, inst int16, frequencyMs int32) int64 {
	key := metricEstimatorKey{org, frequencyMs}

	// 1. Try exact match for this organization + frequency.
	if est, ok := snap[key]; ok && est > 0 {
		return est
	}

	// avg runs over all entries in the snapshot, selecting only those
	// that match the filter. Non-positive estimates are ignored entirely.
	avg := func(filter func(k metricEstimatorKey) bool) (int64, bool) {
		var sum, n int64
		for k, v := range snap {
			if filter(k) && v > 0 {
				sum += v
				n++
			}
		}
		if n == 0 {
			return 0, false
		}
		return sum / n, true
	}

	// 2. Try all entries for the same organization + frequency, across ANY instance number.
	if a, ok := avg(func(k metricEstimatorKey) bool {
		return k.OrganizationID == org && k.FrequencyMs == frequencyMs
	}); ok {
		return a
	}

	// 3. Try all entries for the same organization, across ANY instance and frequency.
	if a, ok := avg(func(k metricEstimatorKey) bool {
		return k.OrganizationID == org
	}); ok {
		return a
	}

	// 4. Try all entries for the same frequency, across ALL organizations and instances.
	if a, ok := avg(func(k metricEstimatorKey) bool {
		return k.FrequencyMs == frequencyMs
	}); ok {
		return a
	}

	return 0
}

func (e *metricEstimator) getFrequencyAgnosticEstimate(snap map[metricEstimatorKey]int64, org uuid.UUID, inst int16) int64 {
	// avg runs over all entries in the snapshot, selecting only those
	// that match the filter. Non-positive estimates are ignored entirely.
	avg := func(filter func(k metricEstimatorKey) bool) (int64, bool) {
		var sum, n int64
		for k, v := range snap {
			if filter(k) && v > 0 {
				sum += v
				n++
			}
		}
		if n == 0 {
			return 0, false
		}
		return sum / n, true
	}

	// 1. Try all entries for the same organization, across ANY instance number and frequency.
	if a, ok := avg(func(k metricEstimatorKey) bool {
		return k.OrganizationID == org
	}); ok {
		return a
	}

	// 2. Try all entries in the snapshot, regardless of organization, instance, or frequency.
	if a, ok := avg(func(_ metricEstimatorKey) bool { return true }); ok {
		return a
	}

	// If no data was available at any tier, return the static default guess.
	return e.defaultGuess
}

func (e *metricEstimator) updateEstimates(parent context.Context, querier EstimationQuerier) error {
	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()

	now := time.Now().UTC()
	low := now.Add(-e.lookback)

	mp := lrdb.MetricSegEstimatorParams{
		DateintLow:  dateint(low),
		DateintHigh: dateint(now),
		MsLow:       low.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}
	metricRows, err := querier.MetricSegEstimator(ctx, mp)
	if err != nil {
		slog.Warn("MetricSegEstimator failed", "error", err)
		return nil
	}

	if len(metricRows) == 0 {
		slog.Warn("no metric estimates found in database, will keep trying")
		return nil
	}

	// Build new map with only positive estimates.
	next := make(map[metricEstimatorKey]int64, len(metricRows))
	var kept, dropped int

	for _, r := range metricRows {
		if r.EstimatedRecords <= 0 {
			dropped++
			slog.Warn("dropping non-positive metric estimate", "org", r.OrganizationID, "freq", r.FrequencyMs, "est", r.EstimatedRecords)
			continue
		}
		next[metricEstimatorKey{r.OrganizationID, r.FrequencyMs}] = r.EstimatedRecords
		kept++
	}

	if kept == 0 {
		slog.Warn("all metric estimates were non-positive; keeping previous snapshot", "dropped", dropped)
		return nil
	}

	e.Lock()
	e.currentEstimates = next
	e.Unlock()

	return nil
}
