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

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CleanupManager manages the domain-specific cleanup logic for a signal type
type CleanupManager struct {
	scheduler     *WorkScheduler
	knownDateints map[string]bool // Track known org-dateint combinations
	sp            storageprofile.StorageProfileProvider
	mdb           lrdb.StoreFull
	cmgr          cloudstorage.ClientProvider
	signalType    string
	mu            sync.Mutex
}

// newCleanupManager creates a new cleanup manager for a signal type
func newCleanupManager(sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider, signalType string) *CleanupManager {
	return &CleanupManager{
		scheduler:     newWorkScheduler(),
		knownDateints: make(map[string]bool),
		sp:            sp,
		mdb:           mdb,
		cmgr:          cmgr,
		signalType:    signalType,
	}
}

// refreshPartitions discovers partitions and adds new work items to the scheduler
func (cm *CleanupManager) refreshPartitions(ctx context.Context, cdb configdb.QuerierFull) error {
	ll := logctx.FromContext(ctx)

	// Get current partitions
	var orgDateints []lrdb.OrgDateintInfo
	var err error

	switch cm.signalType {
	case "metric":
		orgDateints, err = cm.mdb.ParseMetricPartitions(ctx)
	case "log":
		orgDateints, err = cm.mdb.ParseLogPartitions(ctx)
	case "trace":
		orgDateints, err = cm.mdb.ParseTracePartitions(ctx)
	default:
		return fmt.Errorf("unknown signal type: %s", cm.signalType)
	}

	if err != nil {
		return fmt.Errorf("failed to parse %s partitions: %w", cm.signalType, err)
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

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Create new known dateints map
	newKnownDateints := make(map[string]bool)
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

	// Add new work items for discovered partitions
	for orgStr, dateints := range orgDateintMap {
		slices.SortFunc(dateints, func(a, b int32) int {
			return int(b - a) // Reverse order: larger dateint first
		})

		orgUUID, _ := uuid.Parse(orgStr)

		for i, dateInt := range dateints {
			key := makeOrgDateintKey(orgUUID, dateInt)
			newKnownDateints[key] = true

			// Add work item if it's new
			if !cm.knownDateints[key] {
				var nextRunTime time.Time
				if i < 2 {
					// Most recent 2 dateints start immediately
					nextRunTime = currentTime
				} else {
					// Older dateints start with longer delay
					nextRunTime = currentTime.Add(10 * time.Minute)
				}

				var item WorkItem
				switch cm.signalType {
				case "log":
					item = &LogCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               cm.sp,
						mdb:              cm.mdb,
						cmgr:             cm.cmgr,
					}
				case "metric":
					item = &MetricCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               cm.sp,
						mdb:              cm.mdb,
						cmgr:             cm.cmgr,
					}
				case "trace":
					item = &TraceCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               cm.sp,
						mdb:              cm.mdb,
						cmgr:             cm.cmgr,
					}
				}
				if item != nil {
					cm.scheduler.addWorkItem(item)
				}
			}
		}
	}

	// Update known dateints (this will mark deleted ones as unknown)
	cm.knownDateints = newKnownDateints

	ll.Info("Refreshed partition list via discovery",
		slog.String("signal_type", cm.signalType),
		slog.Int("total_combinations", len(newKnownDateints)))

	return nil
}

// isKnown checks if an org-dateint combination is still known
func (cm *CleanupManager) isKnown(key string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.knownDateints[key]
}

// rescheduleWorkItem reschedules a work item with domain-specific validation
func (cm *CleanupManager) rescheduleWorkItem(item WorkItem, rescheduleIn time.Duration) {
	// Only reschedule if the work item is still known, otherwise drop it
	if !cm.isKnown(item.GetKey()) {
		return // Drop unknown work items
	}

	cm.scheduler.rescheduleWorkItem(item, rescheduleIn)
}

// runScheduledCleanupLoop runs the unified cleanup loop using the heap-based scheduler
func runScheduledCleanupLoop(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cdb configdb.QuerierFull, cmgr cloudstorage.ClientProvider, signalType string) error {
	manager := newCleanupManager(sp, mdb, cmgr, signalType)
	ll := logctx.FromContext(ctx).With(slog.String("signal_type", signalType))
	ctx = logctx.WithLogger(ctx, ll)

	ll.Info("Starting heap-based cleanup loop")

	// Initial partition refresh
	if err := manager.refreshPartitions(ctx, cdb); err != nil {
		ll.Error("Failed initial partition refresh", slog.Any("error", err))
		return err
	}

	lastPartitionRefresh := time.Now()
	partitionRefreshInterval := time.Hour

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Periodic partition refresh
		if time.Since(lastPartitionRefresh) > partitionRefreshInterval {
			if err := manager.refreshPartitions(ctx, cdb); err != nil {
				ll.Error("Failed to refresh partitions", slog.Any("error", err))
			} else {
				lastPartitionRefresh = time.Now()
			}
		}

		// Get next work item
		workItem := manager.scheduler.popNextWorkItem()
		if workItem != nil {
			// Process the work item using its Perform method
			rescheduleIn := workItem.Perform(ctx)

			// Reschedule based on returned duration
			manager.rescheduleWorkItem(workItem, rescheduleIn)
			continue
		}

		// No work ready - calculate sleep time
		nextWakeup := manager.scheduler.getNextWakeupTime()
		if nextWakeup == nil {
			// No work scheduled, wait for partition refresh or new work
			sleepDuration := time.Until(lastPartitionRefresh.Add(partitionRefreshInterval))
			if sleepDuration <= 0 {
				sleepDuration = 10 * time.Second
			}

			ll.Debug("No work scheduled, sleeping", slog.Duration("duration", sleepDuration))
			timer := time.NewTimer(sleepDuration)
			select {
			case <-timer.C:
			case <-manager.scheduler.wakeupCh:
				timer.Stop()
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		} else {
			// Sleep until next work is ready
			sleepDuration := time.Until(*nextWakeup)
			if sleepDuration > 0 {
				ll.Debug("Sleeping until next work",
					slog.Duration("duration", sleepDuration),
					slog.Time("next_work", *nextWakeup))

				timer := time.NewTimer(sleepDuration)
				select {
				case <-timer.C:
				case <-manager.scheduler.wakeupCh:
					timer.Stop()
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
		}
	}
}

// makeOrgDateintKey creates a consistent key for org-dateint combinations
func makeOrgDateintKey(orgID uuid.UUID, dateInt int32) string {
	return fmt.Sprintf("%s-%d", orgID.String(), dateInt)
}
func deleteSegmentObject(ctx context.Context, sp storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, orgID uuid.UUID, instanceNum int16, objectKey string) error {
	ll := logctx.FromContext(ctx)

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, orgID, instanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	storageClient, err := cloudstorage.NewClient(ctx, cmgr, profile)
	if err != nil {
		ll.Error("Failed to get storage client", slog.Any("error", err))
		return err
	}

	if err := storageClient.DeleteObject(ctx, profile.Bucket, objectKey); err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404") {
			ll.Debug("S3 object already deleted", slog.String("object_key", objectKey))
		} else {
			ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("object_key", objectKey))
			return err
		}
	} else {
		ll.Debug("Successfully deleted S3 object", slog.String("object_key", objectKey))
	}

	return nil
}

// getHourFromTimestamp extracts the hour component from a timestamp in milliseconds
func getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}
