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

package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type InqueueHandler struct {
	ctx context.Context
	ll  *slog.Logger
	mdb lrdb.StoreFull
	inf lrdb.Inqueue
}

func NewInqueueHandler(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	inf lrdb.Inqueue,
) *InqueueHandler {
	return &InqueueHandler{ctx, ll, mdb, inf}
}

func (h *InqueueHandler) retryWork() {
	if err := h.mdb.ReleaseInqueueWork(h.ctx, lrdb.ReleaseInqueueWorkParams{
		ID:        h.inf.ID,
		ClaimedBy: h.inf.ClaimedBy,
	}); err != nil {
		h.ll.Error("release failed", slog.Any("error", err))
	}
}

func (h *InqueueHandler) deleteWork() {
	if err := h.mdb.DeleteInqueueWork(h.ctx, lrdb.DeleteInqueueWorkParams{
		ID:        h.inf.ID,
		ClaimedBy: h.inf.ClaimedBy,
	}); err != nil {
		h.ll.Error("delete failed", slog.Any("error", err))
	}
}

func (h *InqueueHandler) CompleteWork() {
	h.deleteWork()
}

func (h *InqueueHandler) RetryWork() {
	h.deleteJournal()
	if h.inf.Tries+1 > maxWorkRetries {
		h.ll.Info("too many retries, deleting", slog.Int("newTries", int(h.inf.Tries+1)))
		h.deleteWork()
	} else {
		h.retryWork()
	}
}

func (h *InqueueHandler) deleteJournal() {
	if err := h.mdb.InqueueJournalDelete(h.ctx, lrdb.InqueueJournalDeleteParams{
		OrganizationID: h.inf.OrganizationID,
		Bucket:         h.inf.Bucket,
		ObjectID:       h.inf.ObjectID,
	}); err != nil {
		h.ll.Error("journal delete failed", slog.Any("error", err))
	}
}

// IsNewWork returns true if the row is new.
func (h *InqueueHandler) IsNewWork() (isNew bool, err error) {
	isNew, err = h.mdb.InqueueJournalUpsert(h.ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: h.inf.OrganizationID,
		Bucket:         h.inf.Bucket,
		ObjectID:       h.inf.ObjectID,
	})
	if err != nil {
		return false, fmt.Errorf("upsert journal failed: %w", err)
	}
	return isNew, nil
}
