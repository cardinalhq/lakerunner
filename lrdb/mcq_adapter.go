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

// Make McqFetchCandidatesRow implement QueueItem
func (r McqFetchCandidatesRow) GetID() int64          { return r.ID }
func (r McqFetchCandidatesRow) GetRecordCount() int64 { return r.RecordCount }
func (r McqFetchCandidatesRow) GetQueueTs() time.Time { return r.QueueTs }

// mcqAdapter adapts the compaction queue operations to the generic interface
type mcqAdapter struct {
	tx *Store
}

func (a *mcqAdapter) PickHead(ctx context.Context) (McqPickHeadRow, error) {
	return a.tx.McqPickHead(ctx)
}

func (a *mcqAdapter) FetchCandidates(ctx context.Context, head McqPickHeadRow, batchLimit int32) ([]McqFetchCandidatesRow, error) {
	return a.tx.McqFetchCandidates(ctx, McqFetchCandidatesParams{
		OrganizationID: head.OrganizationID,
		Dateint:        head.Dateint,
		FrequencyMs:    head.FrequencyMs,
		InstanceNum:    head.InstanceNum,
		MaxRows:        batchLimit,
	})
}

func (a *mcqAdapter) ClaimBundle(ctx context.Context, workerID int64, ids []int64) error {
	return a.tx.McqClaimBundle(ctx, McqClaimBundleParams{
		WorkerID: workerID,
		Ids:      ids,
	})
}

func (a *mcqAdapter) DeferItems(ctx context.Context, ids []int64, duration time.Duration) error {
	return a.tx.McqDeferItems(ctx, McqDeferItemsParams{
		Push: duration,
		Ids:  ids,
	})
}

func (a *mcqAdapter) GetGroupingKey(head McqPickHeadRow) string {
	return fmt.Sprintf("mcq:%s/%d/%d/%d",
		head.OrganizationID, head.Dateint, head.FrequencyMs, head.InstanceNum)
}
