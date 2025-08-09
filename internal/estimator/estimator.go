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

type EstimationQuerier interface {
	MetricSegEstimator(ctx context.Context, params lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error)
	LogSegEstimator(ctx context.Context, params lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error)
}

type Estimator interface {
	Get(organizationID uuid.UUID, instanceNum int16, signal lrdb.SignalEnum) Estimate
}

type Estimate struct {
	EstimatedRecordCount int64
}

type estimatorKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	Signal         lrdb.SignalEnum
}

type estimator struct {
	sync.RWMutex
	currentEstimates map[estimatorKey]Estimate
	updateEvery      time.Duration
	lookback         time.Duration
	timeout          time.Duration
	defaultGuess     int64
}

// NewEstimator creates a new Estimator instance that periodically updates its estimates
// based on the data from the database.
// It will stop updating when the doneCtx is canceled.
// NewEstimator also initializes the estimates based on the current data in the database,
// returning an error if the initial fetch fails.
func NewEstimator(doneCtx context.Context, mdb EstimationQuerier) (Estimator, error) {
	e := &estimator{
		currentEstimates: map[estimatorKey]Estimate{},
		updateEvery:      30 * time.Minute,
		lookback:         6 * time.Hour,
		timeout:          30 * time.Second,
		defaultGuess:     40_000,
	}
	if err := e.updateEstimates(doneCtx, mdb); err != nil {
		return nil, err
	}
	go e.run(doneCtx, mdb)
	return e, nil
}

func (e *estimator) Get(org uuid.UUID, inst int16, sig lrdb.SignalEnum) Estimate {
	e.RLock()
	snap := e.currentEstimates
	e.RUnlock()

	key := estimatorKey{org, inst, sig}

	// 1. Try exact match for this organization + instance + signal.
	//    If we have a positive estimate for this exact key, return it immediately.
	if est, ok := snap[key]; ok && est.EstimatedRecordCount > 0 {
		return est
	}

	// If exact exists but is non-positive (shouldn't happen after filtering),
	// mark it so we can explicitly exclude it from all broader fallback tiers.
	excludeExact := false
	if est, ok := snap[key]; ok && est.EstimatedRecordCount <= 0 {
		excludeExact = true
	}

	// avg runs over all entries in the snapshot, selecting only those
	// that match the filter. Non-positive estimates are ignored entirely.
	// If excludeExact is true, the exact key is skipped even if it would
	// otherwise match the filter.
	avg := func(filter func(k estimatorKey) bool) (int64, bool) {
		var sum, n int64
		for k, v := range snap {
			if excludeExact && k == key {
				continue
			}
			if filter(k) && v.EstimatedRecordCount > 0 {
				sum += v.EstimatedRecordCount
				n++
			}
		}
		if n == 0 {
			return 0, false
		}
		return sum / n, true
	}

	// Try all entries for the same organization and signal, across ANY instance number.
	if a, ok := avg(func(k estimatorKey) bool {
		return k.OrganizationID == org && k.Signal == sig
	}); ok {
		return Estimate{EstimatedRecordCount: a}
	}

	// Try all entries for the same signal, across ALL organizations and instances.
	if a, ok := avg(func(k estimatorKey) bool {
		return k.Signal == sig
	}); ok {
		return Estimate{EstimatedRecordCount: a}
	}

	// Try all entries in the snapshot, regardless of organization, instance, or signal.
	if a, ok := avg(func(_ estimatorKey) bool { return true }); ok {
		return Estimate{EstimatedRecordCount: a}
	}

	// If no data was available at any tier, return the static default guess.
	return Estimate{EstimatedRecordCount: e.defaultGuess}
}

func (e *estimator) run(doneCtx context.Context, mdb EstimationQuerier) {
	ticker := time.NewTicker(e.updateEvery)
	defer ticker.Stop()
	for {
		select {
		case <-doneCtx.Done():
			return
		case <-ticker.C:
			if err := e.updateEstimates(doneCtx, mdb); err != nil {
				slog.Error("failed to update estimates", "error", err)
			}
		}
	}
}

func (e *estimator) updateEstimates(parent context.Context, mdb EstimationQuerier) error {
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
	metricRows, err := mdb.MetricSegEstimator(ctx, mp)
	if err != nil {
		slog.Warn("MetricSegEstimator failed", "error", err)
		metricRows = nil
	}

	lp := lrdb.LogSegEstimatorParams{
		DateintLow:  dateint(low),
		DateintHigh: dateint(now),
		MsLow:       low.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}
	logRows, err := mdb.LogSegEstimator(ctx, lp)
	if err != nil {
		slog.Warn("LogSegEstimator failed", "error", err)
		logRows = nil
	}

	if len(metricRows) == 0 && len(logRows) == 0 {
		slog.Warn("no estimates found in database, will keep trying")
		return nil
	}

	// Build new map with only positive estimates.
	next := make(map[estimatorKey]Estimate, len(metricRows)+len(logRows))
	var kept, dropped int

	for _, r := range metricRows {
		if r.EstimatedRecords <= 0 {
			dropped++
			slog.Warn("dropping non-positive metric estimate", "org", r.OrganizationID, "inst", r.InstanceNum, "est", r.EstimatedRecords)
			continue
		}
		next[estimatorKey{r.OrganizationID, r.InstanceNum, lrdb.SignalEnumMetrics}] =
			Estimate{EstimatedRecordCount: r.EstimatedRecords}
		kept++
	}

	for _, r := range logRows {
		if r.EstimatedRecords <= 0 {
			dropped++
			slog.Warn("dropping non-positive log estimate", "org", r.OrganizationID, "inst", r.InstanceNum, "est", r.EstimatedRecords)
			continue
		}
		next[estimatorKey{r.OrganizationID, r.InstanceNum, lrdb.SignalEnumLogs}] =
			Estimate{EstimatedRecordCount: r.EstimatedRecords}
		kept++
	}

	if kept == 0 {
		slog.Warn("all estimates were non-positive; keeping previous snapshot", "dropped", dropped)
		return nil
	}

	e.Lock()
	e.currentEstimates = next
	e.Unlock()

	return nil
}

func dateint(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}
