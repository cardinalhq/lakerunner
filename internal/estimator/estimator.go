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
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
	currentMetricEstimates map[estimatorKey]Estimate
}

// NewEstimator creates a new Estimator instance that periodically updates its estimates
// based on the data from the database.
// It will stop updating when the doneCtx is canceled.
// NewEstimator also initializes the estimates based on the current data in the database,
// returning an error if the initial fetch fails.
func NewEstimator(doneCtx context.Context, mdb EstimationQuerier) (Estimator, error) {
	e := &estimator{
		currentMetricEstimates: map[estimatorKey]Estimate{},
	}
	if err := e.updateEstimates(mdb); err != nil {
		return nil, err
	}
	go e.run(doneCtx, mdb)
	return e, nil
}

func (e *estimator) Get(organizationID uuid.UUID, instanceNum int16, signal lrdb.SignalEnum) Estimate {
	e.RLock()
	defer e.RUnlock()
	key := estimatorKey{
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
		Signal:         signal,
	}
	if estimate, ok := e.currentMetricEstimates[key]; ok {
		return estimate
	}

	// If the estimate is not found, average up across all the data we have and make a guess.
	if len(e.currentMetricEstimates) > 0 {
		estimate := Estimate{}
		for _, estimate := range e.currentMetricEstimates {
			estimate.EstimatedRecordCount += estimate.EstimatedRecordCount
		}
		div := len(e.currentMetricEstimates)
		estimate.EstimatedRecordCount /= int64(div)
		return estimate
	}

	// If we have no estimates at all, return a best guess.
	guess := Estimate{EstimatedRecordCount: 40_000}
	return guess
}

func (e *estimator) run(doneCtx context.Context, mdb EstimationQuerier) {
	for {
		select {
		case <-doneCtx.Done():
			return
		case <-time.Tick(30 * time.Minute):
			if err := e.updateEstimates(mdb); err != nil {
				slog.Error("failed to update estimates", "error", err)
			}
		}
	}
}

func (e *estimator) updateEstimates(mdb EstimationQuerier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now().UTC()

	// Look back 6 hours for estimates.
	lowTime := now.Add(-6 * time.Hour)
	mp := lrdb.MetricSegEstimatorParams{
		DateintLow:  int32(lowTime.Year()*10000 + int(lowTime.Month())*100 + lowTime.Day()),
		DateintHigh: int32(now.Year()*10000 + int(now.Month())*100 + now.Day()),
		MsLow:       lowTime.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}

	metricRows, err := mdb.MetricSegEstimator(ctx, mp)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	lp := lrdb.LogSegEstimatorParams{
		DateintLow:  int32(lowTime.Year()*10000 + int(lowTime.Month())*100 + lowTime.Day()),
		DateintHigh: int32(now.Year()*10000 + int(now.Month())*100 + now.Day()),
		MsLow:       lowTime.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}

	logRows, err := mdb.LogSegEstimator(ctx, lp)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	if len(metricRows) == 0 && len(logRows) == 0 {
		slog.Warn("no estimates found in database, will keep trying")
		return nil
	}

	e.Lock()
	defer e.Unlock()

	// always reset to track only the latest estimates
	e.currentMetricEstimates = map[estimatorKey]Estimate{}

	for _, row := range metricRows {
		key := estimatorKey{
			OrganizationID: row.OrganizationID,
			InstanceNum:    row.InstanceNum,
			Signal:         lrdb.SignalEnumMetrics,
		}
		e.currentMetricEstimates[key] = Estimate{EstimatedRecordCount: row.EstimatedRecords}
	}

	for _, row := range logRows {
		key := estimatorKey{
			OrganizationID: row.OrganizationID,
			InstanceNum:    row.InstanceNum,
			Signal:         lrdb.SignalEnumLogs,
		}
		e.currentMetricEstimates[key] = Estimate{EstimatedRecordCount: row.EstimatedRecords}
	}

	return nil
}
