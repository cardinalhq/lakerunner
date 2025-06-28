// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
