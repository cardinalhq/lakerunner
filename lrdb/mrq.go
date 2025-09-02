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

// RollupBundleResult extends the basic bundle with estimate information
type RollupBundleResult struct {
	Items           []MrqFetchCandidatesRow
	EstimatedTarget int64
}

// ClaimRollupBundle claims a bundle and includes estimate information
// The goal:
// 1. if there is a single large candidate >= target, push it alone
// 2. else, greedily fill up to over (target * over_factor)
// 3. if we don't reach target, check the oldest candidate's age
//   - if >= grace, push what we have, which may be just the oldest candidate
//   - else, defer the whole key and try another key
//
// We try up to MaxAttempts different keys before giving up.
// We also limit the number of candidates fetched per key to BatchLimit.
func (s *Store) ClaimRollupBundle(ctx context.Context, p BundleParams) (*RollupBundleResult, error) {
	var out []MrqFetchCandidatesRow
	var estimatedTarget int64 = 40000 // Default fallback

	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5 // reasonable default
	}

	err := s.execTx(ctx, func(tx *Store) error {
		target := p.TargetRecords
		over := int64(float64(target) * p.OverFactor)

		// Try up to MaxAttempts different keys
		for attempt := 0; attempt < maxAttempts; attempt++ {
			// 1) Pick head
			head, err := tx.MrqPickHead(ctx)
			if errors.Is(err, sql.ErrNoRows) {
				return nil // no work available
			}
			if err != nil {
				return fmt.Errorf("pick head (attempt %d): %w", attempt+1, err)
			}

			// Get the estimate for this organization and target frequency
			// The head contains the source frequency, we need the target frequency
			targetFreq := head.FrequencyMs // This will be the target frequency for the rollup
			estimatedTarget = s.GetMetricEstimate(ctx, head.OrganizationID, targetFreq)

			// 2) Fetch candidates for that key
			cands, err := tx.MrqFetchCandidates(ctx, MrqFetchCandidatesParams{
				OrganizationID: head.OrganizationID,
				Dateint:        head.Dateint,
				FrequencyMs:    head.FrequencyMs,
				InstanceNum:    head.InstanceNum,
				SlotID:         head.SlotID,
				SlotCount:      head.SlotCount,
				RollupGroup:    head.RollupGroup,
				MaxRows:        p.BatchLimit,
			})
			if err != nil {
				return fmt.Errorf("fetch candidates (attempt %d): %w", attempt+1, err)
			}
			if len(cands) == 0 {
				// This key was claimed by another worker, try next
				continue
			}

			// Rest of the bundle logic is the same as the original ClaimRollupBundle
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
					oldest := cands[0].QueueTs
					// tail rule: if oldest candidate waited >= Grace, flush what we have (even if zero -> push one)
					if time.Since(oldest) >= p.Grace {
						if len(bundleIDs) == 0 {
							bundleIDs = append(bundleIDs, cands[0].ID)
						}
					} else {
						// Not stale yet => defer the *whole key* and try another key
						if err := tx.MrqDeferKey(ctx, MrqDeferKeyParams{
							OrganizationID: head.OrganizationID,
							Dateint:        head.Dateint,
							FrequencyMs:    head.FrequencyMs,
							InstanceNum:    head.InstanceNum,
							SlotID:         head.SlotID,
							SlotCount:      head.SlotCount,
							RollupGroup:    head.RollupGroup,
							Push:           p.deferInterval(),
						}); err != nil {
							return fmt.Errorf("defer key (attempt %d): %w", attempt+1, err)
						}
						// Try another key instead of returning
						continue
					}
				}
			}

			// 4) If we have a bundle, claim it and return
			if len(bundleIDs) > 0 {
				if err := tx.MrqClaimBundle(ctx, MrqClaimBundleParams{
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
						out = append(out, c)
					}
				}
				return nil // success, exit transaction
			}
		}

		// Exhausted all attempts without finding a suitable bundle
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &RollupBundleResult{
		Items:           out,
		EstimatedTarget: estimatedTarget,
	}, nil
}

func (s *Store) HeartbeatRollup(ctx context.Context, workerID int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return s.MrqHeartbeat(ctx, MrqHeartbeatParams{
		WorkerID: workerID,
		Ids:      ids,
	})
}

func (s *Store) CompleteRollup(ctx context.Context, workerID int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return s.MrqCompleteDelete(ctx, MrqCompleteDeleteParams{
		WorkerID: workerID,
		Ids:      ids,
	})
}

func (s *Store) ReclaimRollupTimeouts(ctx context.Context, timeout time.Duration, maxBatch int32) (int64, error) {
	rows, err := s.MrqReclaimTimeouts(ctx, MrqReclaimTimeoutsParams{
		MaxAge:  timeout,
		MaxRows: maxBatch,
	})
	return rows, err
}
