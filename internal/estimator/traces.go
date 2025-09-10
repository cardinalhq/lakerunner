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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type TraceQuerier interface {
	TraceSegEstimator(ctx context.Context, params lrdb.TraceSegEstimatorParams) ([]lrdb.TraceSegEstimatorRow, error)
}

type TraceEstimator interface {
	Get(organizationID uuid.UUID, instanceNum int16) int64
}

type traceEstimatorKey struct {
	OrganizationID uuid.UUID
}

type traceEstimator struct {
	sync.RWMutex
	currentEstimates map[traceEstimatorKey]int64
	updateEvery      time.Duration
	lookback         time.Duration
	timeout          time.Duration
	defaultGuess     int64
}

func NewTraceEstimator(doneCtx context.Context, querier EstimationQuerier) (TraceEstimator, error) {
	e := &traceEstimator{
		currentEstimates: map[traceEstimatorKey]int64{},
		updateEvery:      30 * time.Minute,
		lookback:         2 * time.Hour,
		timeout:          30 * time.Second,
		defaultGuess:     40_000,
	}
	if err := e.updateEstimates(doneCtx, querier); err != nil {
		return nil, err
	}
	go RunUpdateLoop(doneCtx, e, querier, e.updateEvery)
	return e, nil
}

func (e *traceEstimator) Get(org uuid.UUID, inst int16) int64 {
	e.RLock()
	snap := e.currentEstimates
	e.RUnlock()

	key := traceEstimatorKey{org}

	// 1. Try exact match for this organization.
	if est, ok := snap[key]; ok && est > 0 {
		return est
	}

	// avg runs over all entries in the snapshot, selecting only those
	// that match the filter. Non-positive estimates are ignored entirely.
	avg := func(filter func(k traceEstimatorKey) bool) (int64, bool) {
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

	// 2. Try all entries for the same organization, across ANY instance number.
	if a, ok := avg(func(k traceEstimatorKey) bool {
		return k.OrganizationID == org
	}); ok {
		return a
	}

	// 3. Try all entries in the snapshot, regardless of organization or instance.
	if a, ok := avg(func(_ traceEstimatorKey) bool { return true }); ok {
		return a
	}

	// If no data was available at any tier, return the static default guess.
	return e.defaultGuess
}

func (e *traceEstimator) updateEstimates(parent context.Context, querier EstimationQuerier) error {
	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()

	now := time.Now().UTC()
	low := now.Add(-e.lookback)

	tp := lrdb.TraceSegEstimatorParams{
		TargetBytes: float64(config.TargetFileSize),
		DateintLow:  dateint(low),
		DateintHigh: dateint(now),
		MsLow:       low.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}
	traceRows, err := querier.TraceSegEstimator(ctx, tp)
	if err != nil {
		slog.Warn("TraceSegEstimator failed", "error", err)
		return nil
	}

	if len(traceRows) == 0 {
		slog.Warn("no trace estimates found in database, will keep trying")
		return nil
	}

	// Build new map with only positive estimates.
	next := make(map[traceEstimatorKey]int64, len(traceRows))
	var kept, dropped int

	for _, r := range traceRows {
		if r.EstimatedRecords <= 0 {
			dropped++
			slog.Warn("dropping non-positive trace estimate", "org", r.OrganizationID, "est", r.EstimatedRecords)
			continue
		}
		next[traceEstimatorKey{r.OrganizationID}] = r.EstimatedRecords
		kept++
	}

	if kept == 0 {
		slog.Warn("all trace estimates were non-positive; keeping previous snapshot", "dropped", dropped)
		return nil
	}

	e.Lock()
	e.currentEstimates = next
	e.Unlock()

	return nil
}
