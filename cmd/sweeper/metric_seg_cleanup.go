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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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

type orgDateintState struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	LastScanned      time.Time
	ConsecutiveEmpty int
	ScanInterval     time.Duration
}

type metricCleanupManager struct {
	ageThreshold  time.Duration
	orgDateints   map[string]*orgDateintState
	mu            sync.RWMutex
	lastRefresh   time.Time
	refreshPeriod time.Duration
}

func newMetricCleanupManager() *metricCleanupManager {
	return &metricCleanupManager{
		ageThreshold:  time.Minute,
		orgDateints:   make(map[string]*orgDateintState),
		refreshPeriod: time.Hour,
	}
}

func metricCleanupLoop(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cdb configdb.QuerierFull, cmgr cloudstorage.ClientProvider) error {
	manager := newMetricCleanupManager()
	ll := logctx.FromContext(ctx).With(slog.String("signal_type", "metric"))
	ctx = logctx.WithLogger(ctx, ll)

	ll.Info("Starting metric segment cleanup loop")

	// Initial refresh
	if err := manager.refreshOrgDateints(ctx, mdb, cdb); err != nil {
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
			if err := manager.refreshOrgDateints(ctx, mdb, cdb); err != nil {
				ll.Error("Failed to refresh org-dateint list", slog.Any("error", err))
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

func (m *metricCleanupManager) refreshOrgDateints(ctx context.Context, mdb lrdb.StoreFull, cdb configdb.QuerierFull) error {
	ll := logctx.FromContext(ctx)

	// Use partition discovery to get org-dateint combinations directly from partition metadata
	orgDateints, err := mdb.ParseMetricPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to parse metric partitions: %w", err)
	}

	// Filter to only enabled organizations
	orgs, err := cdb.ListEnabledOrganizations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get enabled organizations: %w", err)
	}

	enabledOrgs := make(map[string]bool)
	for _, org := range orgs {
		enabledOrgs[org.ID.String()] = true
	}

	newOrgDateints := make(map[string]*orgDateintState)
	currentTime := time.Now()

	// Group by organization and sort dateints (most recent first)
	orgDateintMap := make(map[string][]int32)
	for _, od := range orgDateints {
		// Skip disabled organizations
		if !enabledOrgs[od.OrganizationID.String()] {
			continue
		}
		orgDateintMap[od.OrganizationID.String()] = append(orgDateintMap[od.OrganizationID.String()], od.Dateint)
	}

	// Sort dateints for each org (most recent first)
	for orgStr, dateints := range orgDateintMap {
		slices.SortFunc(dateints, func(a, b int32) int {
			return int(b - a) // Reverse order: larger dateint first
		})

		orgUUID, _ := uuid.Parse(orgStr)

		for i, dateInt := range dateints {
			key := fmt.Sprintf("%s-%d", orgStr, dateInt)

			m.mu.RLock()
			existing := m.orgDateints[key]
			m.mu.RUnlock()

			state := &orgDateintState{
				OrganizationID: orgUUID,
				DateInt:        dateInt,
				ScanInterval:   time.Minute,
			}

			if existing != nil {
				// Preserve existing state
				state.LastScanned = existing.LastScanned
				state.ConsecutiveEmpty = existing.ConsecutiveEmpty
				state.ScanInterval = existing.ScanInterval
			} else {
				// New org-dateint, prioritize recent ones
				if i < 2 {
					// Most recent 2 dateints start with shorter interval
					state.LastScanned = currentTime.Add(-30 * time.Second)
				} else {
					// Older dateints start with longer interval
					state.LastScanned = currentTime.Add(-10 * time.Minute)
					state.ScanInterval = 10 * time.Minute
				}
			}

			newOrgDateints[key] = state
		}
	}

	m.mu.Lock()
	m.orgDateints = newOrgDateints
	m.lastRefresh = currentTime
	m.mu.Unlock()

	ll.Info("Refreshed org-dateint list via partition discovery", slog.Int("total_combinations", len(newOrgDateints)))
	return nil
}

func (m *metricCleanupManager) processAllOrgDateints(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider) bool {
	processedAny := false
	currentTime := time.Now()

	m.mu.RLock()
	var candidates []*orgDateintState
	for _, state := range m.orgDateints {
		if currentTime.Sub(state.LastScanned) >= state.ScanInterval {
			candidates = append(candidates, state)
		}
	}
	m.mu.RUnlock()

	for _, state := range candidates {
		processed := m.processOrgDateint(ctx, sp, mdb, cmgr, state)
		if processed {
			processedAny = true
		}

		// Update state
		m.mu.Lock()
		key := fmt.Sprintf("%s-%d", state.OrganizationID.String(), state.DateInt)
		if currentState := m.orgDateints[key]; currentState != nil {
			currentState.LastScanned = currentTime
			if !processed {
				currentState.ConsecutiveEmpty++
				// Exponential backoff: 1m -> 5m -> 15m -> 30m -> 1h (max)
				switch {
				case currentState.ConsecutiveEmpty <= 2:
					currentState.ScanInterval = time.Minute
				case currentState.ConsecutiveEmpty <= 5:
					currentState.ScanInterval = 5 * time.Minute
				case currentState.ConsecutiveEmpty <= 10:
					currentState.ScanInterval = 15 * time.Minute
				case currentState.ConsecutiveEmpty <= 20:
					currentState.ScanInterval = 30 * time.Minute
				default:
					currentState.ScanInterval = time.Hour
				}
			} else {
				// Reset backoff on activity
				currentState.ConsecutiveEmpty = 0
				currentState.ScanInterval = time.Minute
			}
		}
		m.mu.Unlock()
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

	storageClient, err := cloudstorage.NewClient(ctx, cmgr, profile)
	if err != nil {
		ll.Error("Failed to get storage client", slog.Any("error", err))
		return false
	}

	objectKey := m.generateMetricObjectKey(segment, &profile)

	if err := storageClient.DeleteObject(ctx, profile.Bucket, objectKey); err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404") {
			ll.Debug("S3 object already deleted", slog.String("object_key", objectKey))
		} else {
			ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("object_key", objectKey))
			return false
		}
	} else {
		ll.Debug("Successfully deleted S3 object", slog.String("object_key", objectKey))
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
	))

	metricCleanupBytes.Add(ctx, segment.FileSize, metric.WithAttributes(
		attribute.String("organization_id", segment.OrganizationID.String()),
	))

	return true
}

func (m *metricCleanupManager) generateMetricObjectKey(segment lrdb.MetricSegmentCleanupGetRow, profile *storageprofile.StorageProfile) string {
	hour := helpers.HourFromMillis(segment.TsRangeLower)

	return helpers.MakeDBObjectID(
		segment.OrganizationID,
		profile.CollectorName,
		segment.Dateint,
		hour,
		segment.SegmentID,
		"metrics",
	)
}
