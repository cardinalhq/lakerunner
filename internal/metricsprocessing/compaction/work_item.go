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

package compaction

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type WorkItem struct {
	db       lrdb.StoreFull
	workerID int64
	item     lrdb.MetricCompactionQueue
}

func NewWorkItem(db lrdb.StoreFull, workerID int64, item lrdb.MetricCompactionQueue) *WorkItem {
	return &WorkItem{
		db:       db,
		workerID: workerID,
		item:     item,
	}
}

func (w *WorkItem) Complete() error {
	ctx := context.Background()
	return w.db.DeleteMetricCompactionWork(ctx, lrdb.DeleteMetricCompactionWorkParams{
		ID:        w.item.ID,
		ClaimedBy: w.workerID,
	})
}

func (w *WorkItem) Fail() error {
	ctx := context.Background()
	return w.db.ReleaseMetricCompactionWork(ctx, lrdb.ReleaseMetricCompactionWorkParams{
		ID:        w.item.ID,
		ClaimedBy: w.workerID,
	})
}

func (w *WorkItem) Delete() error {
	return w.Complete()
}

func (w *WorkItem) ID() int64 {
	return w.item.ID
}

func (w *WorkItem) OrganizationID() uuid.UUID {
	return w.item.OrganizationID
}

func (w *WorkItem) InstanceNum() int16 {
	return w.item.InstanceNum
}

func (w *WorkItem) Dateint() int32 {
	return w.item.Dateint
}

func (w *WorkItem) FrequencyMs() int32 {
	return int32(w.item.FrequencyMs)
}

func (w *WorkItem) Signal() lrdb.SignalEnum {
	return lrdb.SignalEnumMetrics
}

func (w *WorkItem) Tries() int32 {
	return w.item.Tries
}

func (w *WorkItem) Action() lrdb.ActionEnum {
	return lrdb.ActionEnumCompact
}

func (w *WorkItem) TsRange() pgtype.Range[pgtype.Timestamptz] {
	return w.item.TsRange
}

func (w *WorkItem) Priority() int32 {
	return w.item.Priority
}

func (w *WorkItem) SlotId() int32 {
	return 0
}

func (w *WorkItem) AsMap() map[string]any {
	return map[string]any{
		"id":              w.item.ID,
		"organization_id": w.item.OrganizationID,
		"instance_num":    w.item.InstanceNum,
		"dateint":         w.item.Dateint,
		"frequency_ms":    w.item.FrequencyMs,
		"segment_id":      w.item.SegmentID,
		"ts_range":        w.item.TsRange,
		"record_count":    w.item.RecordCount,
		"tries":           w.item.Tries,
		"priority":        w.item.Priority,
	}
}

func (w *WorkItem) RunnableAt() time.Time {
	return w.item.QueueTs
}

var _ lockmgr.Workable = (*WorkItem)(nil)
