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

package sweeper

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func runMetricEstimateUpdate(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Constants for EWMA calculation
	const (
		alpha                = 0.2 // EWMA smoothing factor (20% new data, 80% old data)
		lookbackMinutes      = 10
		defaultTargetRecords = 40000
	)

	now := time.Now().UTC()
	lookbackTime := now.Add(-time.Duration(lookbackMinutes) * time.Minute)

	ll.Info("Starting metric estimate update", slog.Time("lookback_start", lookbackTime))

	// Load existing estimates
	existingEstimates, err := mdb.GetAllMetricPackEstimates(ctx)
	if err != nil {
		ll.Error("Failed to get existing metric pack estimates", slog.Any("error", err))
		return err
	}

	// Create map of existing estimates for quick lookup
	existingMap := make(map[string]lrdb.MetricPackEstimate)
	for _, est := range existingEstimates {
		key := fmt.Sprintf("%s-%d", est.OrganizationID.String(), est.FrequencyMs)
		existingMap[key] = est
	}

	// Get fresh data from metric segments
	params := lrdb.MetricSegEstimatorParams{
		DateintLow:  dateintFromTime(lookbackTime),
		DateintHigh: dateintFromTime(now),
		MsLow:       lookbackTime.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}

	segmentData, err := mdb.MetricSegEstimator(ctx, params)
	if err != nil {
		ll.Error("Failed to get metric segment estimator data", slog.Any("error", err))
		return err
	}

	if len(segmentData) == 0 {
		ll.Info("No metric segment data found for estimate update")
		return nil
	}

	var updateCount int64

	// Process each segment data point
	for _, data := range segmentData {
		if data.EstimatedRecords <= 0 {
			ll.Warn("Skipping non-positive estimated records",
				slog.String("org_id", data.OrganizationID.String()),
				slog.Int("frequency_ms", int(data.FrequencyMs)),
				slog.Int64("estimated_records", data.EstimatedRecords))
			continue
		}

		key := fmt.Sprintf("%s-%d", data.OrganizationID.String(), data.FrequencyMs)

		var newTargetRecords int64

		if existing, exists := existingMap[key]; exists {
			// Apply EWMA: new = alpha * current + (1 - alpha) * old
			if existing.TargetRecords != nil && *existing.TargetRecords > 0 {
				oldValue := float64(*existing.TargetRecords)
				newValue := float64(data.EstimatedRecords)
				ewma := alpha*newValue + (1-alpha)*oldValue
				newTargetRecords = int64(math.Round(ewma))
			} else {
				// First valid value for this key
				newTargetRecords = data.EstimatedRecords
			}
		} else {
			// New organization+frequency combination
			newTargetRecords = data.EstimatedRecords
		}

		// Update the estimate in the database
		if err := mdb.UpsertMetricPackEstimate(ctx, lrdb.UpsertMetricPackEstimateParams{
			OrganizationID: data.OrganizationID,
			FrequencyMs:    data.FrequencyMs,
			TargetRecords:  &newTargetRecords,
		}); err != nil {
			ll.Error("Failed to upsert metric pack estimate",
				slog.Any("error", err),
				slog.String("org_id", data.OrganizationID.String()),
				slog.Int("frequency_ms", int(data.FrequencyMs)))
			continue
		}

		updateCount++

		ll.Debug("Updated metric pack estimate",
			slog.String("org_id", data.OrganizationID.String()),
			slog.Int("frequency_ms", int(data.FrequencyMs)),
			slog.Int64("old_target", func() int64 {
				if existing, exists := existingMap[key]; exists && existing.TargetRecords != nil {
					return *existing.TargetRecords
				}
				return 0
			}()),
			slog.Int64("new_estimate", data.EstimatedRecords),
			slog.Int64("new_target", newTargetRecords))
	}

	metricEstimateCounter.Add(ctx, updateCount, metric.WithAttributes(
		attribute.String("status", "success"),
	))

	ll.Info("Completed metric estimate update",
		slog.Int64("updates_applied", updateCount),
		slog.Int("data_points_processed", len(segmentData)))

	return nil
}

// dateintFromTime converts a time to a dateint format (YYYYMMDD).
func dateintFromTime(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}
