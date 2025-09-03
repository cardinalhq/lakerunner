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
	var result RollupBundleResult

	err := s.execTx(ctx, func(tx *Store) error {
		// Take an advisory lock to serialize access to the rollup queue
		// Using a hash of "metric_rollup_queue" as the lock key
		// This will wait until the lock is available (released by other transactions)
		_, err := tx.db.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(0x6d72715f6c6f636b))
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		adapter := &mrqAdapter{tx: tx}
		items, estimatedTarget, err := ClaimBundle(
			ctx,
			adapter,
			p,
			func(ctx context.Context, head MrqPickHeadRow) int64 {
				// The head contains the source frequency, we need the target frequency for the rollup
				return s.GetMetricEstimate(ctx, head.OrganizationID, head.FrequencyMs)
			},
		)

		result.Items = items
		result.EstimatedTarget = estimatedTarget
		return err
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
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
