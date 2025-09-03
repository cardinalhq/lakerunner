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
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
)

// InqueueBundleParams contains parameters for claiming inqueue bundles
type InqueueBundleParams struct {
	WorkerID      int64
	Signal        string
	TargetSize    int64         // target total file size
	MaxSize       int64         // maximum total file size
	GracePeriod   time.Duration // how old before we accept partial batch
	DeferInterval time.Duration // base defer time
	Jitter        time.Duration // random jitter to add
	MaxAttempts   int           // max different groups to try
	MaxCandidates int32         // max items to fetch per group
}

// InqueueBundleResult contains the claimed bundle
type InqueueBundleResult struct {
	Items []InqueueGetBundleItemsRow
}

// deferInterval calculates defer time with jitter
func (p InqueueBundleParams) deferInterval() time.Duration {
	if p.Jitter <= 0 {
		return p.DeferInterval
	}
	return p.DeferInterval + time.Duration(rand.Int63n(p.Jitter.Nanoseconds()))
}

// ClaimInqueueBundleWithLock claims a bundle using advisory lock and custom logic
func (s *Store) ClaimInqueueBundleWithLock(ctx context.Context, params InqueueBundleParams) (InqueueBundleResult, error) {
	var result InqueueBundleResult

	// Set defaults
	if params.MaxAttempts <= 0 {
		params.MaxAttempts = 5
	}
	if params.MaxCandidates <= 0 {
		params.MaxCandidates = 100
	}
	if params.DeferInterval <= 0 {
		params.DeferInterval = 10 * time.Second
	}
	if params.GracePeriod <= 0 {
		params.GracePeriod = 1 * time.Minute
	}

	err := s.execTx(ctx, func(tx *Store) error {
		// Take advisory lock based on signal type
		// Use different lock keys for different signals
		var lockKey int64
		switch params.Signal {
		case "metrics":
			lockKey = 0x696e715f6d657472 // "inq_metr"
		case "logs":
			lockKey = 0x696e715f6c6f6773 // "inq_logs"
		case "traces":
			lockKey = 0x696e715f74726163 // "inq_trac"
		default:
			return fmt.Errorf("unknown signal type: %s", params.Signal)
		}

		_, err := tx.db.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey)
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		// Try up to MaxAttempts different groups
		for attempt := 0; attempt < params.MaxAttempts; attempt++ {
			// 1. Pick head (oldest eligible item)
			head, err := tx.InqueuePickHead(ctx, params.Signal)
			if errors.Is(err, sql.ErrNoRows) {
				return nil // No work available
			}
			if err != nil {
				return fmt.Errorf("pick head (attempt %d): %w", attempt+1, err)
			}

			// 2. Fetch candidates for this group
			candidates, err := tx.InqueueFetchCandidates(ctx, InqueueFetchCandidatesParams{
				Column1: params.Signal,
				Column2: head.OrganizationID,
				Column3: head.InstanceNum,
				Column4: params.MaxCandidates,
			})
			if err != nil {
				return fmt.Errorf("fetch candidates (attempt %d): %w", attempt+1, err)
			}
			if len(candidates) == 0 {
				// Someone else grabbed this group, try next
				continue
			}

			// 3. Build bundle (greedy accumulation)
			var bundleIDs []uuid.UUID
			var totalSize int64

			// Helper to calculate effective size
			effectiveSize := func(item InqueueFetchCandidatesRow) int64 {
				// .binpb files are protobuf and compress well - count as 1/10th size
				if strings.HasSuffix(item.ObjectID, ".binpb") {
					return item.FileSize / 10
				}
				return item.FileSize
			}

			// Check if items are old enough
			oldestAge := time.Since(candidates[0].QueueTs)
			isOldEnough := oldestAge >= params.GracePeriod
			// fmt.Printf("DEBUG: oldestAge=%v, gracePeriod=%v, isOldEnough=%v\n", oldestAge, params.GracePeriod, isOldEnough)

			// Check for single large file that exceeds target (only for fresh files)
			firstEffectiveSize := effectiveSize(candidates[0])
			if !isOldEnough && firstEffectiveSize >= params.TargetSize {
				// Take just this one large file (for fresh files only)
				bundleIDs = []uuid.UUID{candidates[0].ID}
			} else {
				// Greedy accumulation up to max size
				for _, item := range candidates {
					itemEffectiveSize := effectiveSize(item)
					nextSize := totalSize + itemEffectiveSize
					if nextSize > params.MaxSize {
						// Would exceed max, stop here
						break
					}
					bundleIDs = append(bundleIDs, item.ID)
					totalSize = nextSize

					// For fresh files, stop if we've reached target
					// For old files, continue accumulating up to max
					if !isOldEnough && totalSize >= params.TargetSize {
						break
					}
				}

				// 4. Check if we have enough or if items are old enough
				if totalSize < params.TargetSize && !isOldEnough {
					// Not enough and not old - defer these items
					deferIDs := make([]uuid.UUID, len(candidates))
					for i, c := range candidates {
						deferIDs[i] = c.ID
					}
					if err := tx.InqueueDeferItems(ctx, InqueueDeferItemsParams{
						Column2: deferIDs,
						Column1: params.deferInterval(),
					}); err != nil {
						return fmt.Errorf("defer items (attempt %d): %w", attempt+1, err)
					}
					// Try another group
					continue
				}
			}

			// 6. Claim the bundle if we have one
			if len(bundleIDs) > 0 {
				if err := tx.InqueueClaimBundle(ctx, InqueueClaimBundleParams{
					Column1: params.WorkerID,
					Column2: bundleIDs,
				}); err != nil {
					return fmt.Errorf("claim bundle (attempt %d): %w", attempt+1, err)
				}

				// Get full details for claimed items
				items, err := tx.InqueueGetBundleItems(ctx, bundleIDs)
				if err != nil {
					return fmt.Errorf("get bundle items: %w", err)
				}

				result.Items = items
				return nil
			}
		}

		// 7. No suitable bundle found after all attempts
		return nil
	})

	if err != nil {
		return InqueueBundleResult{}, err
	}
	return result, nil
}
