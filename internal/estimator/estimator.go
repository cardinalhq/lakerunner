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

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

type EstimationQuerier interface {
	MetricSegEstimator(ctx context.Context) ([]lrdb.MetricSegEstimatorRow, error)
	LogSegEstimator(ctx context.Context) ([]lrdb.LogSegEstimatorRow, error)
}

type Estimator interface {
	Get(organizationID uuid.UUID, instanceNum int16, signal lrdb.SignalEnum) Estimate
}

type Estimate struct {
	AvgBytesPerRecord float64
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
			estimate.AvgBytesPerRecord += estimate.AvgBytesPerRecord
		}
		div := float64(len(e.currentMetricEstimates))
		estimate.AvgBytesPerRecord /= div
		return estimate
	}

	// If we have no estimates at all, return a best guess.
	guess := Estimate{
		AvgBytesPerRecord: 100,
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metricRows, err := mdb.MetricSegEstimator(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	logRows, err := mdb.LogSegEstimator(ctx)
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
		e.currentMetricEstimates[key] = Estimate{
			AvgBytesPerRecord: row.AvgBpr,
		}
	}

	for _, row := range logRows {
		key := estimatorKey{
			OrganizationID: row.OrganizationID,
			InstanceNum:    row.InstanceNum,
			Signal:         lrdb.SignalEnumLogs,
		}
		e.currentMetricEstimates[key] = Estimate{
			AvgBytesPerRecord: row.AvgBpr,
		}
	}

	return nil
}
