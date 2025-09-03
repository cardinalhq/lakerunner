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

package lrdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// QueueItem represents a work item that can be bundled
type QueueItem interface {
	GetID() int64
	GetRecordCount() int64
	GetQueueTs() time.Time
}

// QueueOperations defines the operations needed for bundle claiming
type QueueOperations[H any, C QueueItem] interface {
	// PickHead selects the next available work item
	PickHead(ctx context.Context) (H, error)

	// FetchCandidates fetches work items matching the head's grouping key
	FetchCandidates(ctx context.Context, head H, batchLimit int32) ([]C, error)

	// ClaimBundle marks the selected items as claimed by this worker
	ClaimBundle(ctx context.Context, workerID int64, ids []int64) error

	// DeferItems pushes items further into the future when we can't process them yet
	DeferItems(ctx context.Context, ids []int64, duration time.Duration) error

	// GetGroupingKey returns a string representation of the grouping key for debugging
	GetGroupingKey(head H) string
}

// ClaimBundle handles the generic bundle claiming logic
// Returns claimed items, estimated target, and error
func ClaimBundle[H any, C QueueItem](
	ctx context.Context,
	ops QueueOperations[H, C],
	params BundleParams,
	getEstimate func(ctx context.Context, head H) int64,
) ([]C, int64, error) {
	maxAttempts := params.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5 // reasonable default
	}

	var estimatedTarget int64 = 40000 // Default fallback

	// Try up to MaxAttempts different keys
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// 1) Pick head
		head, err := ops.PickHead(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, estimatedTarget, nil // no work available
		}
		if err != nil {
			return nil, estimatedTarget, fmt.Errorf("pick head (attempt %d): %w", attempt+1, err)
		}

		// Get dynamic target from estimator
		estimatedTarget = getEstimate(ctx, head)
		target := estimatedTarget
		over := int64(float64(target) * params.OverFactor)

		// 2) Fetch candidates for that key
		cands, err := ops.FetchCandidates(ctx, head, params.BatchLimit)
		if err != nil {
			return nil, estimatedTarget, fmt.Errorf("fetch candidates (attempt %d): %w", attempt+1, err)
		}
		if len(cands) == 0 {
			// This key was claimed by another worker, try next
			continue
		}

		// 3) Build bundle (big-single + greedy + tail rule)
		var bundleIDs []int64
		var sum int64

		if cands[0].GetRecordCount() >= target {
			// big single - take just this one item
			bundleIDs = []int64{cands[0].GetID()}
		} else {
			// Greedy accumulation with strict limits
			for _, x := range cands {
				// Check if adding this item would exceed our over limit
				nextSum := sum + x.GetRecordCount()
				if nextSum > over {
					// If we haven't reached target yet, check if this single item alone is acceptable
					if sum < target && len(bundleIDs) == 0 {
						// Take at least one item if we have nothing
						bundleIDs = append(bundleIDs, x.GetID())
						sum = nextSum
					}
					break
				}
				bundleIDs = append(bundleIDs, x.GetID())
				sum = nextSum
				// Stop as soon as we reach the target
				if sum >= target {
					break
				}
			}

			if sum < target {
				oldest := cands[0].GetQueueTs()
				// tail rule: if oldest candidate waited >= Grace, flush what we have
				if time.Since(oldest) >= params.Grace {
					if len(bundleIDs) == 0 && len(cands) > 0 {
						// Take at least the first item if we have nothing and it's old enough
						bundleIDs = append(bundleIDs, cands[0].GetID())
					}
				} else {
					// Not stale yet => defer only the items we fetched and try another key
					deferIDs := make([]int64, len(cands))
					for i, c := range cands {
						deferIDs[i] = c.GetID()
					}
					if err := ops.DeferItems(ctx, deferIDs, params.deferInterval()); err != nil {
						return nil, estimatedTarget, fmt.Errorf("defer items (attempt %d): %w", attempt+1, err)
					}
					// Try another key instead of returning
					continue
				}
			}
		}

		// 4) If we have a bundle, claim it and return
		if len(bundleIDs) > 0 {
			if err := ops.ClaimBundle(ctx, params.WorkerID, bundleIDs); err != nil {
				return nil, estimatedTarget, fmt.Errorf("claim bundle (attempt %d): %w", attempt+1, err)
			}

			// Return the claimed rows
			result := make([]C, 0, len(bundleIDs))
			for _, id := range bundleIDs {
				for _, c := range cands {
					if c.GetID() == id {
						result = append(result, c)
						break
					}
				}
			}
			return result, estimatedTarget, nil
		}
	}

	// No suitable bundle found after all attempts
	return nil, estimatedTarget, nil
}
