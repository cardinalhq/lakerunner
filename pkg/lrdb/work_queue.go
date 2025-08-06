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
)

func (q *Store) WorkQueueAdd(ctx context.Context, params WorkQueueAddParams) error {
	return q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		return s.WorkQueueAddDirect(ctx, params)
	})
}

func (q *Store) WorkQueueFail(ctx context.Context, params WorkQueueFailParams) error {
	return q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		return s.WorkQueueFailDirect(ctx, params)
	})
}

func (q *Store) WorkQueueComplete(ctx context.Context, params WorkQueueCompleteParams) error {
	return q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		return s.WorkQueueCompleteDirect(ctx, params)
	})
}

func (q *Store) WorkQueueHeartbeat(ctx context.Context, params WorkQueueHeartbeatParams) error {
	return q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		return s.WorkQueueHeartbeatDirect(ctx, params)
	})
}

func (q *Store) WorkQueueCleanup(ctx context.Context) ([]WorkQueueCleanupRow, error) {
	var result []WorkQueueCleanupRow
	err := q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		var err error
		result, err = s.WorkQueueCleanupDirect(ctx)
		return err
	})
	return result, err
}

func (q *Store) WorkQueueClaim(ctx context.Context, params WorkQueueClaimParams) (WorkQueueClaimRow, error) {
	var result WorkQueueClaimRow
	err := q.execTx(ctx, func(s *Store) error {
		if err := s.WorkQueueGlobalLock(ctx); err != nil {
			return err
		}
		var err error
		result, err = s.WorkQueueClaimDirect(ctx, params)
		return err
	})
	return result, err
}
