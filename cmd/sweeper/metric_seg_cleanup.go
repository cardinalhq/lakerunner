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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	metricCleanupCounter metric.Int64Counter
	metricCleanupBytes   metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")

	var err error
	metricCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.metric_cleanup_total",
		metric.WithDescription("Count of metric segments processed during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metric_cleanup_total counter: %w", err))
	}

	metricCleanupBytes, err = meter.Int64Counter(
		"lakerunner.sweeper.metric_cleanup_bytes_total",
		metric.WithDescription("Total bytes of metric segments deleted during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metric_cleanup_bytes_total counter: %w", err))
	}
}

type metricCleanupManager struct {
	ageThreshold  time.Duration
	tracker       *orgDateintTracker
	lastRefresh   time.Time
	refreshPeriod time.Duration
}

func newMetricCleanupManager() *metricCleanupManager {
	return &metricCleanupManager{
		ageThreshold:  time.Hour,
		tracker:       newOrgDateintTracker(),
		refreshPeriod: time.Hour,
	}
}

func metricCleanupLoop(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cdb configdb.QuerierFull, cmgr cloudstorage.ClientProvider) error {
	manager := newMetricCleanupManager()
	ll := logctx.FromContext(ctx).With(slog.String("signal_type", "metric"))
	ctx = logctx.WithLogger(ctx, ll)

	ll.Info("Starting metric segment cleanup loop")

	// Initial refresh
	if err := manager.tracker.refreshOrgDateints(ctx, mdb, cdb, "metric"); err != nil {
		ll.Error("Failed initial org-dateint refresh", slog.Any("error", err))
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Periodic refresh of org-dateint list
		if time.Since(manager.lastRefresh) > manager.refreshPeriod {
			if err := manager.tracker.refreshOrgDateints(ctx, mdb, cdb, "metric"); err != nil {
				ll.Error("Failed to refresh org-dateint list", slog.Any("error", err))
			} else {
				manager.lastRefresh = time.Now()
			}
		}

		// Process cleanup for all org-dateints
		processedAny := manager.processAllOrgDateints(ctx, sp, mdb, cmgr)

		// Sleep for a short time if no work was done
		if !processedAny {
			if stop := sleepCtx(ctx, time.Minute); stop {
				return ctx.Err()
			}
		}
	}
}

func (m *metricCleanupManager) processAllOrgDateints(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider) bool {
	processedAny := false
	candidates := m.tracker.getCandidates()

	for _, state := range candidates {
		processed := m.processOrgDateint(ctx, sp, mdb, cmgr, state)
		if processed {
			processedAny = true
		}

		// Update state using the tracker
		m.tracker.updateState(state.OrganizationID, state.DateInt, processed)
	}

	return processedAny
}

func (m *metricCleanupManager) processOrgDateint(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider, state *orgDateintState) bool {
	ll := logctx.FromContext(ctx).With(
		slog.String("org_id", state.OrganizationID.String()),
		slog.Int("dateint", int(state.DateInt)))

	ageThreshold := time.Now().Add(-m.ageThreshold)

	segments, err := mdb.MetricSegmentCleanupGet(ctx, lrdb.MetricSegmentCleanupGetParams{
		OrganizationID: state.OrganizationID,
		Dateint:        state.DateInt,
		AgeThreshold:   ageThreshold,
		MaxRows:        1000,
	})
	if err != nil {
		ll.Error("Failed to get metric segments for cleanup", slog.Any("error", err))
		return false
	}

	if len(segments) == 0 {
		return false
	}

	ll.Debug("Found segments for cleanup", slog.Int("count", len(segments)))

	successCount := 0
	totalBytes := int64(0)

	for _, segment := range segments {
		if m.processMetricSegment(ctx, sp, mdb, cmgr, segment) {
			successCount++
			totalBytes += segment.FileSize
		}
	}

	if successCount > 0 {
		ll.Info("Processed metric segments for cleanup",
			slog.Int("count", successCount),
			slog.Int64("total_bytes", totalBytes))
	}

	return successCount > 0
}

func (m *metricCleanupManager) processMetricSegment(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider, segment lrdb.MetricSegmentCleanupGetRow) bool {
	ll := logctx.FromContext(ctx).With(
		slog.Int64("segment_id", segment.SegmentID),
		slog.Int("instance_num", int(segment.InstanceNum)))

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, segment.OrganizationID, segment.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return false
	}

	objectKey := m.generateMetricObjectKey(segment, profile.CollectorName)

	if err := deleteSegmentObject(ctx, sp, cmgr, segment.OrganizationID, segment.InstanceNum, objectKey); err != nil {
		return false
	}

	if err := mdb.MetricSegmentCleanupDelete(ctx, lrdb.MetricSegmentCleanupDeleteParams{
		OrganizationID: segment.OrganizationID,
		Dateint:        segment.Dateint,
		FrequencyMs:    segment.FrequencyMs,
		SegmentID:      segment.SegmentID,
		InstanceNum:    segment.InstanceNum,
	}); err != nil {
		ll.Error("Failed to delete metric segment from database", slog.Any("error", err))
		return false
	}

	metricCleanupCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("organization_id", segment.OrganizationID.String()),
		attribute.Int("instance_num", int(segment.InstanceNum)),
	))

	metricCleanupBytes.Add(ctx, segment.FileSize, metric.WithAttributes(
		attribute.String("organization_id", segment.OrganizationID.String()),
		attribute.Int("instance_num", int(segment.InstanceNum)),
	))

	return true
}

func (m *metricCleanupManager) generateMetricObjectKey(segment lrdb.MetricSegmentCleanupGetRow, collectorName string) string {
	hour := helpers.HourFromMillis(segment.TsRangeLower)

	return helpers.MakeDBObjectID(
		segment.OrganizationID,
		collectorName,
		segment.Dateint,
		hour,
		segment.SegmentID,
		"metrics",
	)
}
