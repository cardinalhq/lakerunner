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
}

func (p BundleParams) deferInterval() time.Duration {
	if p.Jitter <= 0 {
		return p.DeferBase
	}
	return p.DeferBase + time.Duration(rand.Int63n(p.Jitter.Nanoseconds()))
}

// ClaimCompactionBundle locks and claims a bundle (or returns nil if none).
func (s *Store) ClaimCompactionBundle(ctx context.Context, p BundleParams) ([]McqFetchCandidatesRow, error) {
	var out []McqFetchCandidatesRow

	err := s.execTx(ctx, func(tx *Store) error {
		// 1) Pick head
		head, err := tx.McqPickHead(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // no work
		}
		if err != nil {
			return fmt.Errorf("pick head: %w", err)
		}

		// 2) Fetch candidates for that key
		cands, err := tx.McqFetchCandidates(ctx, McqFetchCandidatesParams{
			OrganizationID: head.OrganizationID,
			Dateint:        head.Dateint,
			FrequencyMs:    head.FrequencyMs,
			InstanceNum:    head.InstanceNum,
			MaxRows:        p.BatchLimit,
		})
		if err != nil {
			return fmt.Errorf("fetch candidates: %w", err)
		}
		if len(cands) == 0 {
			return nil
		}

		// 3) Build bundle (big-single + greedy + tail rule)
		target := p.TargetRecords
		over := int64(float64(target) * p.OverFactor)

		var bundleIDs []int64
		var sum int64

		if cands[0].RecordCount >= target {
			// big single
			bundleIDs = []int64{cands[0].ID}
			sum = cands[0].RecordCount
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
						sum = cands[0].RecordCount
					}
				} else {
					// Not stale yet => defer the *whole key* and bail
					if err := tx.McqDeferKey(ctx, McqDeferKeyParams{
						OrganizationID: head.OrganizationID,
						Dateint:        head.Dateint,
						FrequencyMs:    head.FrequencyMs,
						InstanceNum:    head.InstanceNum,
						Push:           p.deferInterval(),
					}); err != nil {
						return fmt.Errorf("defer key: %w", err)
					}
					return nil
				}
			}
		}

		// 4) Claim rows
		if len(bundleIDs) == 0 {
			return nil
		}
		if err := tx.McqClaimBundle(ctx, McqClaimBundleParams{
			WorkerID: p.WorkerID,
			Ids:      bundleIDs,
		}); err != nil {
			return fmt.Errorf("claim bundle: %w", err)
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
		return nil
	})

	if err != nil {
		return nil, err
	}
	return out, nil
}
