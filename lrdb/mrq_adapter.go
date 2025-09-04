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

// Make MrqFetchCandidatesRow implement QueueItem
func (r MrqFetchCandidatesRow) GetID() int64          { return r.ID }
func (r MrqFetchCandidatesRow) GetRecordCount() int64 { return r.RecordCount }
func (r MrqFetchCandidatesRow) GetQueueTs() time.Time { return r.QueueTs }

// mrqAdapter adapts the rollup queue operations to the generic interface
type mrqAdapter struct {
	tx *Store
}

func (a *mrqAdapter) PickHead(ctx context.Context) (MrqPickHeadRow, error) {
	return a.tx.MrqPickHead(ctx)
}

func (a *mrqAdapter) FetchCandidates(ctx context.Context, head MrqPickHeadRow, batchLimit int32) ([]MrqFetchCandidatesRow, error) {
	return a.tx.MrqFetchCandidates(ctx, MrqFetchCandidatesParams{
		OrganizationID: head.OrganizationID,
		Dateint:        head.Dateint,
		FrequencyMs:    head.FrequencyMs,
		InstanceNum:    head.InstanceNum,
		SlotID:         head.SlotID,
		SlotCount:      head.SlotCount,
		RollupGroup:    head.RollupGroup,
		MaxRows:        batchLimit,
	})
}

func (a *mrqAdapter) ClaimBundle(ctx context.Context, workerID int64, ids []int64) error {
	return a.tx.MrqClaimBundle(ctx, MrqClaimBundleParams{
		WorkerID: workerID,
		Ids:      ids,
	})
}

func (a *mrqAdapter) DeferItems(ctx context.Context, ids []int64, duration time.Duration) error {
	return a.tx.MrqDeferItems(ctx, MrqDeferItemsParams{
		Push: duration,
		Ids:  ids,
	})
}

func (a *mrqAdapter) GetGroupingKey(head MrqPickHeadRow) string {
	return fmt.Sprintf("mrq:%s/%d/%d/%d/slot:%d/%d/group:%d",
		head.OrganizationID, head.Dateint, head.FrequencyMs, head.InstanceNum,
		head.SlotID, head.SlotCount, head.RollupGroup)
}
