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
	"time"
)

type BundleParams struct {
	WorkerID      int64
	TargetRecords int64
	OverFactor    float64       // e.g., 1.20
	BatchLimit    int32         // candidates to inspect
	Grace         time.Duration // tail rule
	DeferBase     time.Duration // how long to defer when not enough (before jitter)
	Jitter        time.Duration // add random [0..Jitter)
	MaxAttempts   int           // how many different keys to try before giving up
}

func (p BundleParams) deferInterval() time.Duration {
	if p.Jitter <= 0 {
		return p.DeferBase
	}
	return p.DeferBase + time.Duration(rand.Int63n(p.Jitter.Nanoseconds()))
}

// CompactionBundleResult represents the result of claiming a compaction work bundle with estimation
type CompactionBundleResult struct {
	Items           []McqFetchCandidatesRow
	EstimatedTarget int64
}

// ClaimCompactionBundle locks and claims a bundle with dynamic estimation support
func (s *Store) ClaimCompactionBundle(ctx context.Context, p BundleParams) (CompactionBundleResult, error) {
	var result CompactionBundleResult

	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5 // reasonable default
	}

	err := s.execTx(ctx, func(tx *Store) error {
		// Take an advisory lock to serialize access to the compaction queue
		// Using a hash of "metric_compaction_queue" as the lock key
		// This will wait until the lock is available (released by other transactions)
		_, err := tx.db.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(0x6d63715f6c6f636b))
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		var headForEstimate *McqPickHeadRow

		// Try up to MaxAttempts different keys
		for attempt := 0; attempt < maxAttempts; attempt++ {
			// 1) Pick head
			head, err := tx.McqPickHead(ctx)
			if errors.Is(err, sql.ErrNoRows) {
				return nil // no work available
			}
			if err != nil {
				return fmt.Errorf("pick head (attempt %d): %w", attempt+1, err)
			}

			// Store the first head for estimation (even if we defer this key)
			if headForEstimate == nil {
				headForEstimate = &head
			}

			// Get dynamic target from estimator for this organization/frequency
			estimatedTarget := s.GetMetricEstimate(ctx, head.OrganizationID, head.FrequencyMs)
			target := estimatedTarget
			over := int64(float64(target) * p.OverFactor)

			// 2) Fetch candidates for that key
			cands, err := tx.McqFetchCandidates(ctx, McqFetchCandidatesParams{
				OrganizationID: head.OrganizationID,
				Dateint:        head.Dateint,
				FrequencyMs:    head.FrequencyMs,
				InstanceNum:    head.InstanceNum,
				MaxRows:        p.BatchLimit,
			})
			if err != nil {
				return fmt.Errorf("fetch candidates (attempt %d): %w", attempt+1, err)
			}
			if len(cands) == 0 {
				// This key was claimed by another worker, try next
				continue
			}

			// 3) Build bundle (big-single + greedy + tail rule)
			var bundleIDs []int64
			var sum int64

			if cands[0].RecordCount >= target {
				// big single
				bundleIDs = []int64{cands[0].ID}
			} else {
				for _, x := range cands {
					if sum+x.RecordCount > over {
						break
					}
					bundleIDs = append(bundleIDs, x.ID)
					sum += x.RecordCount
					if sum >= target {
						break
					}
				}
				if sum < target {
					// tail rule: if oldest candidate waited >= Grace, flush what we have (even if zero -> push one)
					oldest := cands[0].QueueTs
					if time.Since(oldest) >= p.Grace {
						if len(bundleIDs) == 0 {
							bundleIDs = append(bundleIDs, cands[0].ID)
						}
					} else {
						// Not stale yet => defer only the items we fetched and try another key
						deferIDs := make([]int64, len(cands))
						for i, c := range cands {
							deferIDs[i] = c.ID
						}
						if err := tx.McqDeferItems(ctx, McqDeferItemsParams{
							Push: p.deferInterval(),
							Ids:  deferIDs,
						}); err != nil {
							return fmt.Errorf("defer items (attempt %d): %w", attempt+1, err)
						}
						// Try another key instead of returning
						continue
					}
				}
			}

			// 4) If we have a bundle, claim it and return
			if len(bundleIDs) > 0 {
				if err := tx.McqClaimBundle(ctx, McqClaimBundleParams{
					WorkerID: p.WorkerID,
					Ids:      bundleIDs,
				}); err != nil {
					return fmt.Errorf("claim bundle (attempt %d): %w", attempt+1, err)
				}

				// Return the claimed rows (we already have record_count/segment_id in cands)
				// Filter to only claimed IDs (cands are a superset in order).
				keep := make(map[int64]struct{}, len(bundleIDs))
				for _, id := range bundleIDs {
					keep[id] = struct{}{}
				}
				for _, c := range cands {
					if _, ok := keep[c.ID]; ok {
						result.Items = append(result.Items, c)
					}
				}
				result.EstimatedTarget = estimatedTarget
				return nil // success, exit transaction
			}
		}

		// Exhausted all attempts without finding a suitable bundle
		// If we picked at least one head, use it for estimation
		if headForEstimate != nil {
			result.EstimatedTarget = s.GetMetricEstimate(ctx, headForEstimate.OrganizationID, headForEstimate.FrequencyMs)
		}
		return nil
	})

	if err != nil {
		return CompactionBundleResult{}, err
	}
	return result, nil
}
