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

	err := s.execTx(ctx, func(tx *Store) error {
		// Take an advisory lock to serialize access to the compaction queue
		// Using a hash of "metric_compaction_queue" as the lock key
		// This will wait until the lock is available (released by other transactions)
		_, err := tx.db.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(0x6d63715f6c6f636b))
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		adapter := &mcqAdapter{tx: tx}
		items, estimatedTarget, err := ClaimBundle(
			ctx,
			adapter,
			p,
			func(ctx context.Context, head McqPickHeadRow) int64 {
				return s.GetMetricEstimate(ctx, head.OrganizationID, head.FrequencyMs)
			},
		)

		result.Items = items
		result.EstimatedTarget = estimatedTarget
		return err
	})

	if err != nil {
		return CompactionBundleResult{}, err
	}
	return result, nil
}
