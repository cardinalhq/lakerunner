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

// orgDateintState tracks the scanning state for an organization-dateint combination
type orgDateintState struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	LastScanned      time.Time
	ConsecutiveEmpty int
	ScanInterval     time.Duration
}

// orgDateintTracker manages exponential backoff for organization-dateint scanning
type orgDateintTracker struct {
	mu          sync.RWMutex
	orgDateints map[string]*orgDateintState
}

// newOrgDateintTracker creates a new tracker
func newOrgDateintTracker() *orgDateintTracker {
	return &orgDateintTracker{
		orgDateints: make(map[string]*orgDateintState),
	}
}

// updateState updates the state after processing an org-dateint combination
func (t *orgDateintTracker) updateState(orgID uuid.UUID, dateInt int32, processed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := makeOrgDateintKey(orgID, dateInt)
	currentTime := time.Now()

	if currentState := t.orgDateints[key]; currentState != nil {
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
}

// getCandidates returns org-dateint states that are ready for processing
func (t *orgDateintTracker) getCandidates() []*orgDateintState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	currentTime := time.Now()
	var candidates []*orgDateintState
	for _, state := range t.orgDateints {
		if currentTime.Sub(state.LastScanned) >= state.ScanInterval {
			candidates = append(candidates, state)
		}
	}
	return candidates
}

// setOrgDateints replaces the entire org-dateint map with new states
func (t *orgDateintTracker) setOrgDateints(newOrgDateints map[string]*orgDateintState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.orgDateints = newOrgDateints
}

// makeOrgDateintKey creates a consistent key for org-dateint combinations
func makeOrgDateintKey(orgID uuid.UUID, dateInt int32) string {
	return fmt.Sprintf("%s-%d", orgID.String(), dateInt)
}

// refreshOrgDateints updates the tracker with current partition state for a signal type
func (t *orgDateintTracker) refreshOrgDateints(ctx context.Context, mdb lrdb.StoreFull, cdb configdb.QuerierFull, signalType string) error {
	ll := logctx.FromContext(ctx)

	// Parse partitions using the generic partition discovery
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
			key := makeOrgDateintKey(orgUUID, dateInt)

			// Get existing state from tracker
			t.mu.RLock()
			existing := t.orgDateints[key]
			t.mu.RUnlock()

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

	t.setOrgDateints(newOrgDateints)

	ll.Info("Refreshed org-dateint list via partition discovery",
		slog.String("signal_type", signalType),
		slog.Int("total_combinations", len(newOrgDateints)))

	return nil
}

// deleteSegmentObject handles the common pattern of deleting a segment's S3 object
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
