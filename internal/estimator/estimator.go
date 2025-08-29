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
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// EstimationQuerier interface that can handle metric, log, and trace queries.
type EstimationQuerier interface {
	MetricSegEstimator(ctx context.Context, params lrdb.MetricSegEstimatorParams) ([]lrdb.MetricSegEstimatorRow, error)
	LogSegEstimator(ctx context.Context, params lrdb.LogSegEstimatorParams) ([]lrdb.LogSegEstimatorRow, error)
	TraceSegEstimator(ctx context.Context, params lrdb.TraceSegEstimatorParams) ([]lrdb.TraceSegEstimatorRow, error)
}

// Updater defines the interface for estimators that can be updated.
type Updater interface {
	updateEstimates(ctx context.Context, querier EstimationQuerier) error
}

// RunUpdateLoop starts a background update loop for an estimator.
func RunUpdateLoop(doneCtx context.Context, updater Updater, querier EstimationQuerier, updateEvery time.Duration) {
	ticker := time.NewTicker(updateEvery)
	defer ticker.Stop()
	for {
		select {
		case <-doneCtx.Done():
			return
		case <-ticker.C:
			if err := updater.updateEstimates(doneCtx, querier); err != nil {
				slog.Error("failed to update estimates", "error", err)
			}
		}
	}
}

// dateint converts a time to a dateint format (YYYYMMDD).
func dateint(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}
