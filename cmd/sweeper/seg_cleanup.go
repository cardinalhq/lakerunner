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
	"container/heap"
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

// WorkResult indicates the outcome of performing work
type WorkResult int

const (
	WorkResultNoWork  WorkResult = iota // No work was done, increase backoff
	WorkResultLittle                    // Some work was done, moderate backoff
	WorkResultMaxWork                   // Maximum work was done, reset backoff
	WorkResultDropped                   // Work item should be dropped/removed
)

// WorkItem represents a cleanup task that can perform work
type WorkItem interface {
	// Perform executes the work and returns the result
	Perform(ctx context.Context) WorkResult

	// GetNextRunTime returns when this work should next be executed
	GetNextRunTime() time.Time

	// SetNextRunTime updates when this work should next be executed
	SetNextRunTime(t time.Time)

	// GetKey returns a unique key for this work item (for tracking)
	GetKey() string

	// UpdateBackoff adjusts the backoff based on work result
	UpdateBackoff(result WorkResult)
}

// WorkItemHeap implements heap.Interface for WorkItem
type WorkItemHeap []WorkItem

func (h WorkItemHeap) Len() int           { return len(h) }
func (h WorkItemHeap) Less(i, j int) bool { return h[i].GetNextRunTime().Before(h[j].GetNextRunTime()) }
func (h WorkItemHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *WorkItemHeap) Push(x any) {
	*h = append(*h, x.(WorkItem))
}

func (h *WorkItemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// WorkScheduler manages cleanup work items using separate heaps per signal type
type WorkScheduler struct {
	logHeap       WorkItemHeap
	metricHeap    WorkItemHeap
	traceHeap     WorkItemHeap
	knownDateints map[string]bool // Track known org-dateint combinations
	mu            sync.Mutex
	wakeupCh      chan struct{} // Signal when new work is added

	// Dependencies for creating work items
	sp   storageprofile.StorageProfileProvider
	mdb  lrdb.StoreFull
	cmgr cloudstorage.ClientProvider
}

// newWorkScheduler creates a new work scheduler
func newWorkScheduler(sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider) *WorkScheduler {
	return &WorkScheduler{
		logHeap:       make(WorkItemHeap, 0),
		metricHeap:    make(WorkItemHeap, 0),
		traceHeap:     make(WorkItemHeap, 0),
		knownDateints: make(map[string]bool),
		wakeupCh:      make(chan struct{}, 1),
		sp:            sp,
		mdb:           mdb,
		cmgr:          cmgr,
	}
}

// popNextWorkItem returns the next work item that's ready to run, or nil if none ready
func (s *WorkScheduler) popNextWorkItem() WorkItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check all heaps and find the earliest ready work item
	var earliestItem WorkItem
	var earliestHeap *WorkItemHeap

	heaps := []*WorkItemHeap{&s.logHeap, &s.metricHeap, &s.traceHeap}

	for _, h := range heaps {
		if h.Len() > 0 {
			item := (*h)[0]
			if time.Now().After(item.GetNextRunTime()) || time.Now().Equal(item.GetNextRunTime()) {
				if earliestItem == nil || item.GetNextRunTime().Before(earliestItem.GetNextRunTime()) {
					earliestItem = item
					earliestHeap = h
				}
			}
		}
	}

	if earliestItem == nil {
		return nil
	}

	// Pop the item and check if its org-dateint still exists
	item := heap.Pop(earliestHeap).(WorkItem)

	// If this org-dateint was deleted, don't return it
	if !s.knownDateints[item.GetKey()] {
		// Try the next item recursively
		return s.popNextWorkItem()
	}

	return item
}

// rescheduleWorkItem reschedules a work item based on processing results
func (s *WorkScheduler) rescheduleWorkItem(item WorkItem, result WorkResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only reschedule if the org-dateint still exists
	if !s.knownDateints[item.GetKey()] {
		return
	}

	// Drop items that should be dropped
	if result == WorkResultDropped {
		return
	}

	// Update backoff based on result
	item.UpdateBackoff(result)

	// Add back to appropriate heap
	switch item.(type) {
	case *LogCleanupWorkItem:
		heap.Push(&s.logHeap, item)
	case *MetricCleanupWorkItem:
		heap.Push(&s.metricHeap, item)
	case *TraceCleanupWorkItem:
		heap.Push(&s.traceHeap, item)
	}

	s.signalWakeup()
}

// getNextWakeupTime returns when the scheduler should next check for work
func (s *WorkScheduler) getNextWakeupTime() *time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	var earliestTime *time.Time

	heaps := []*WorkItemHeap{&s.logHeap, &s.metricHeap, &s.traceHeap}

	for _, h := range heaps {
		if h.Len() > 0 {
			itemTime := (*h)[0].GetNextRunTime()
			if earliestTime == nil || itemTime.Before(*earliestTime) {
				earliestTime = &itemTime
			}
		}
	}

	return earliestTime
}

// refreshPartitions updates the known partitions and adds new work items
func (s *WorkScheduler) refreshPartitions(ctx context.Context, mdb lrdb.StoreFull, cdb configdb.QuerierFull, signalType string) error {
	ll := logctx.FromContext(ctx)

	// Get current partitions
	var orgDateints []lrdb.OrgDateintInfo
	var err error

	switch signalType {
	case "metric":
		orgDateints, err = mdb.ParseMetricPartitions(ctx)
	case "log":
		orgDateints, err = mdb.ParseLogPartitions(ctx)
	case "trace":
		orgDateints, err = mdb.ParseTracePartitions(ctx)
	default:
		return fmt.Errorf("unknown signal type: %s", signalType)
	}

	if err != nil {
		return fmt.Errorf("failed to parse %s partitions: %w", signalType, err)
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

	s.mu.Lock()
	defer s.mu.Unlock()

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
			if !s.knownDateints[key] {
				var nextRunTime time.Time
				if i < 2 {
					// Most recent 2 dateints start immediately
					nextRunTime = currentTime
				} else {
					// Older dateints start with longer delay
					nextRunTime = currentTime.Add(10 * time.Minute)
				}

				var item WorkItem
				switch signalType {
				case "log":
					item = &LogCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               s.sp,
						mdb:              s.mdb,
						cmgr:             s.cmgr,
					}
					heap.Push(&s.logHeap, item)
				case "metric":
					item = &MetricCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               s.sp,
						mdb:              s.mdb,
						cmgr:             s.cmgr,
					}
					heap.Push(&s.metricHeap, item)
				case "trace":
					item = &TraceCleanupWorkItem{
						OrganizationID:   orgUUID,
						DateInt:          dateInt,
						NextRunTime:      nextRunTime,
						ConsecutiveEmpty: 0,
						sp:               s.sp,
						mdb:              s.mdb,
						cmgr:             s.cmgr,
					}
					heap.Push(&s.traceHeap, item)
				}
			}
		}
	}

	// Update known dateints (this will mark deleted ones as unknown)
	s.knownDateints = newKnownDateints

	ll.Info("Refreshed partition list via discovery",
		slog.String("signal_type", signalType),
		slog.Int("total_combinations", len(newKnownDateints)))

	s.signalWakeup()
	return nil
}

// signalWakeup signals the scheduler to wake up (non-blocking)
func (s *WorkScheduler) signalWakeup() {
	select {
	case s.wakeupCh <- struct{}{}:
	default:
		// Channel already has signal, no need to add another
	}
}

// makeOrgDateintKey creates a consistent key for org-dateint combinations
func makeOrgDateintKey(orgID uuid.UUID, dateInt int32) string {
	return fmt.Sprintf("%s-%d", orgID.String(), dateInt)
}

// runScheduledCleanupLoop runs the unified cleanup loop using the heap-based scheduler
func runScheduledCleanupLoop(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cdb configdb.QuerierFull, cmgr cloudstorage.ClientProvider, signalType string) error {
	scheduler := newWorkScheduler(sp, mdb, cmgr)
	ll := logctx.FromContext(ctx).With(slog.String("signal_type", signalType))
	ctx = logctx.WithLogger(ctx, ll)

	ll.Info("Starting heap-based cleanup loop")

	// Initial partition refresh
	if err := scheduler.refreshPartitions(ctx, mdb, cdb, signalType); err != nil {
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
			if err := scheduler.refreshPartitions(ctx, mdb, cdb, signalType); err != nil {
				ll.Error("Failed to refresh partitions", slog.Any("error", err))
			} else {
				lastPartitionRefresh = time.Now()
			}
		}

		// Get next work item
		workItem := scheduler.popNextWorkItem()
		if workItem != nil {
			// Process the work item using its Perform method
			result := workItem.Perform(ctx)

			// Reschedule based on results
			scheduler.rescheduleWorkItem(workItem, result)
			continue
		}

		// No work ready - calculate sleep time
		nextWakeup := scheduler.getNextWakeupTime()
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
			case <-scheduler.wakeupCh:
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
				case <-scheduler.wakeupCh:
					timer.Stop()
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
		}
	}
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

// updateBackoffTiming calculates the next run time based on consecutive empty results
func updateBackoffTiming(consecutiveEmpty int) time.Time {
	switch {
	case consecutiveEmpty <= 2:
		return time.Now().Add(time.Minute)
	case consecutiveEmpty <= 5:
		return time.Now().Add(5 * time.Minute)
	case consecutiveEmpty <= 10:
		return time.Now().Add(15 * time.Minute)
	case consecutiveEmpty <= 20:
		return time.Now().Add(30 * time.Minute)
	default:
		return time.Now().Add(time.Hour)
	}
}
