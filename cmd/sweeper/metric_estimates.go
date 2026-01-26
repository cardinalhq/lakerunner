// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	packEstimateGauge   metric.Int64ObservableGauge
	estimateGaugeValues sync.Map // key -> gaugeEntry
)

type gaugeEntry struct {
	value int64
	attrs []attribute.KeyValue
}

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")
	var err error
	packEstimateGauge, err = meter.Int64ObservableGauge(
		"lakerunner.sweeper.pack_estimate_target_records",
		metric.WithDescription("Target record estimates for pack tables"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create pack_estimate_target_records gauge: %w", err))
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		estimateGaugeValues.Range(func(_, v any) bool {
			ge := v.(gaugeEntry)
			o.ObserveInt64(packEstimateGauge, ge.value, metric.WithAttributes(ge.attrs...))
			return true
		})
		return nil
	}, packEstimateGauge)
	if err != nil {
		panic(fmt.Errorf("failed to register pack_estimate_target_records callback: %w", err))
	}
}

func recordPackEstimate(orgID uuid.UUID, freqMs int32, signal string, target int64) {
	attrs := []attribute.KeyValue{
		attribute.String("organization_id", orgID.String()),
		attribute.String("signal", signal),
	}
	if freqMs >= 0 {
		attrs = append(attrs, attribute.Int("frequency_ms", int(freqMs)))
	}
	key := orgID.String() + "-" + signal + fmt.Sprintf("-%d", freqMs)
	estimateGaugeValues.Store(key, gaugeEntry{value: target, attrs: attrs})
}

func runMetricEstimateUpdate(ctx context.Context, mdb lrdb.StoreFull) error {
	ll := logctx.FromContext(ctx)

	// Constants for EWMA calculation
	const (
		alpha           = 0.2 // EWMA smoothing factor (20% new data, 80% old data)
		lookbackMinutes = 10
	)

	now := time.Now().UTC()
	lookbackTime := now.Add(-time.Duration(lookbackMinutes) * time.Minute)

	ll.Info("Starting metric estimate update", slog.Time("lookback_start", lookbackTime))

	// Load existing metric estimates
	existingEstimates, err := mdb.GetAllBySignal(ctx, "metrics")
	if err != nil {
		ll.Error("Failed to get existing metric estimates", slog.Any("error", err))
		return err
	}

	// Create map of existing metric estimates for quick lookup
	existingMap := make(map[string]lrdb.GetAllBySignalRow)
	for _, est := range existingEstimates {
		key := fmt.Sprintf("%s-%d", est.OrganizationID.String(), est.FrequencyMs)
		existingMap[key] = est
	}

	// Get fresh data from metric segments
	params := lrdb.MetricSegEstimatorParams{
		TargetBytes: float64(config.TargetFileSize),
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

		recordPackEstimate(data.OrganizationID, data.FrequencyMs, "metrics", newTargetRecords)
	}

	metricEstimateCounter.Add(ctx, updateCount, metric.WithAttributes(
		attribute.String("status", "success"),
	))

	ll.Info("Completed metric estimate update",
		slog.Int64("updates_applied", updateCount),
		slog.Int("data_points_processed", len(segmentData)))

	return nil
}

func runLogEstimateUpdate(ctx context.Context, mdb lrdb.StoreFull) error {
	ll := logctx.FromContext(ctx)

	// Constants for EWMA calculation
	const (
		alpha           = 0.2 // EWMA smoothing factor (20% new data, 80% old data)
		lookbackMinutes = 10
	)

	now := time.Now().UTC()
	lookbackTime := now.Add(-time.Duration(lookbackMinutes) * time.Minute)

	ll.Info("Starting log estimate update", slog.Time("lookback_start", lookbackTime))

	// Load existing log estimates
	existingEstimates, err := mdb.GetAllBySignal(ctx, "logs")
	if err != nil {
		ll.Error("Failed to get existing log estimates", slog.Any("error", err))
		return err
	}

	// Create map of existing log estimates for quick lookup (logs use frequency_ms=-1)
	existingMap := make(map[string]lrdb.GetAllBySignalRow)
	for _, est := range existingEstimates {
		if est.FrequencyMs == -1 {
			key := est.OrganizationID.String()
			existingMap[key] = est
		}
	}

	// Get fresh data from log segments
	params := lrdb.LogSegEstimatorParams{
		TargetBytes: float64(config.TargetFileSize),
		DateintLow:  dateintFromTime(lookbackTime),
		DateintHigh: dateintFromTime(now),
		MsLow:       lookbackTime.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}

	segmentData, err := mdb.LogSegEstimator(ctx, params)
	if err != nil {
		ll.Error("Failed to get log segment estimator data", slog.Any("error", err))
		return err
	}

	if len(segmentData) == 0 {
		ll.Info("No log segment data found for estimate update")
		return nil
	}

	var updateCount int64

	// Process each segment data point
	for _, data := range segmentData {
		if data.EstimatedRecords <= 0 {
			ll.Warn("Skipping non-positive estimated records",
				slog.String("org_id", data.OrganizationID.String()),
				slog.String("signal", "logs"),
				slog.Int64("estimated_records", data.EstimatedRecords))
			continue
		}

		key := data.OrganizationID.String()

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
			// New organization for logs
			newTargetRecords = data.EstimatedRecords
		}

		// Update the estimate in the database
		if err := mdb.UpsertPackEstimate(ctx, lrdb.UpsertPackEstimateParams{
			OrganizationID: data.OrganizationID,
			FrequencyMs:    -1,
			Signal:         "logs",
			TargetRecords:  &newTargetRecords,
		}); err != nil {
			ll.Error("Failed to upsert log pack estimate",
				slog.Any("error", err),
				slog.String("org_id", data.OrganizationID.String()))
			continue
		}

		updateCount++

		ll.Debug("Updated log pack estimate",
			slog.String("org_id", data.OrganizationID.String()),
			slog.String("signal", "logs"),
			slog.Int64("old_target", func() int64 {
				if existing, exists := existingMap[key]; exists && existing.TargetRecords != nil {
					return *existing.TargetRecords
				}
				return 0
			}()),
			slog.Int64("new_estimate", data.EstimatedRecords),
			slog.Int64("new_target", newTargetRecords))

		recordPackEstimate(data.OrganizationID, -1, "logs", newTargetRecords)
	}

	metricEstimateCounter.Add(ctx, updateCount, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("signal", "logs"),
	))

	ll.Info("Completed log estimate update",
		slog.Int64("updates_applied", updateCount),
		slog.Int("data_points_processed", len(segmentData)))

	return nil
}

func runTraceEstimateUpdate(ctx context.Context, mdb lrdb.StoreFull) error {
	ll := logctx.FromContext(ctx)

	// Constants for EWMA calculation
	const (
		alpha           = 0.2 // EWMA smoothing factor (20% new data, 80% old data)
		lookbackMinutes = 10
	)

	now := time.Now().UTC()
	lookbackTime := now.Add(-time.Duration(lookbackMinutes) * time.Minute)

	ll.Info("Starting trace estimate update", slog.Time("lookback_start", lookbackTime))

	// Load existing trace estimates
	existingEstimates, err := mdb.GetAllBySignal(ctx, "traces")
	if err != nil {
		ll.Error("Failed to get existing trace estimates", slog.Any("error", err))
		return err
	}

	// Create map of existing trace estimates for quick lookup (traces use frequency_ms=-1)
	existingMap := make(map[string]lrdb.GetAllBySignalRow)
	for _, est := range existingEstimates {
		if est.FrequencyMs == -1 {
			key := est.OrganizationID.String()
			existingMap[key] = est
		}
	}

	// Get fresh data from trace segments
	params := lrdb.TraceSegEstimatorParams{
		TargetBytes: float64(config.TargetFileSize),
		DateintLow:  dateintFromTime(lookbackTime),
		DateintHigh: dateintFromTime(now),
		MsLow:       lookbackTime.UnixMilli(),
		MsHigh:      now.UnixMilli(),
	}

	segmentData, err := mdb.TraceSegEstimator(ctx, params)
	if err != nil {
		ll.Error("Failed to get trace segment estimator data", slog.Any("error", err))
		return err
	}

	if len(segmentData) == 0 {
		ll.Info("No trace segment data found for estimate update")
		return nil
	}

	var updateCount int64

	// Process each segment data point
	for _, data := range segmentData {
		if data.EstimatedRecords <= 0 {
			ll.Warn("Skipping non-positive estimated records",
				slog.String("org_id", data.OrganizationID.String()),
				slog.String("signal", "traces"),
				slog.Int64("estimated_records", data.EstimatedRecords))
			continue
		}

		key := data.OrganizationID.String()

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
			// New organization for traces
			newTargetRecords = data.EstimatedRecords
		}

		// Update the estimate in the database
		if err := mdb.UpsertPackEstimate(ctx, lrdb.UpsertPackEstimateParams{
			OrganizationID: data.OrganizationID,
			FrequencyMs:    -1,
			Signal:         "traces",
			TargetRecords:  &newTargetRecords,
		}); err != nil {
			ll.Error("Failed to upsert trace pack estimate",
				slog.Any("error", err),
				slog.String("org_id", data.OrganizationID.String()))
			continue
		}

		updateCount++

		ll.Debug("Updated trace pack estimate",
			slog.String("org_id", data.OrganizationID.String()),
			slog.String("signal", "traces"),
			slog.Int64("old_target", func() int64 {
				if existing, exists := existingMap[key]; exists && existing.TargetRecords != nil {
					return *existing.TargetRecords
				}
				return 0
			}()),
			slog.Int64("new_estimate", data.EstimatedRecords),
			slog.Int64("new_target", newTargetRecords))

		recordPackEstimate(data.OrganizationID, -1, "traces", newTargetRecords)
	}

	metricEstimateCounter.Add(ctx, updateCount, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("signal", "traces"),
	))

	ll.Info("Completed trace estimate update",
		slog.Int64("updates_applied", updateCount),
		slog.Int("data_points_processed", len(segmentData)))

	return nil
}

// dateintFromTime converts a time to a dateint format (YYYYMMDD).
func dateintFromTime(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}
