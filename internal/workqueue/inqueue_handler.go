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

package workqueue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type InqueueHandler struct {
	ctx            context.Context
	logger         *slog.Logger
	store          Store
	workItem       lrdb.Inqueue
	maxWorkRetries int
}

func NewInqueueHandler(
	ctx context.Context,
	store Store,
	workItem lrdb.Inqueue,
	maxWorkRetries int,
	opts ...HandlerOption,
) *InqueueHandler {
	options := &handlerOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(options)
	}

	return &InqueueHandler{
		ctx:            ctx,
		logger:         options.logger,
		store:          store,
		workItem:       workItem,
		maxWorkRetries: maxWorkRetries,
	}
}

func (h *InqueueHandler) retryWork() {
	if err := h.store.ReleaseInqueueWork(h.ctx, lrdb.ReleaseInqueueWorkParams{
		ID:        h.workItem.ID,
		ClaimedBy: h.workItem.ClaimedBy,
	}); err != nil {
		h.logger.Error("release failed", slog.Any("error", err))
	}
}

func (h *InqueueHandler) deleteWork() {
	if err := h.store.DeleteInqueueWork(h.ctx, lrdb.DeleteInqueueWorkParams{
		ID:        h.workItem.ID,
		ClaimedBy: h.workItem.ClaimedBy,
	}); err != nil {
		h.logger.Error("delete failed", slog.Any("error", err))
	}
}

func (h *InqueueHandler) CompleteWork() {
	h.deleteWork()
}

func (h *InqueueHandler) RetryWork() {
	h.deleteJournal()
	if h.workItem.Tries+1 > int32(h.maxWorkRetries) {
		h.logger.Info("too many retries, deleting", slog.Int("newTries", int(h.workItem.Tries+1)))
		h.deleteWork()
	} else {
		h.retryWork()
	}
}

func (h *InqueueHandler) deleteJournal() {
	if err := h.store.InqueueJournalDelete(h.ctx, lrdb.InqueueJournalDeleteParams{
		OrganizationID: h.workItem.OrganizationID,
		Bucket:         h.workItem.Bucket,
		ObjectID:       h.workItem.ObjectID,
	}); err != nil {
		h.logger.Error("journal delete failed", slog.Any("error", err))
	}
}

// IsNewWork returns true if the row is new.
func (h *InqueueHandler) IsNewWork() (isNew bool, err error) {
	isNew, err = h.store.InqueueJournalUpsert(h.ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: h.workItem.OrganizationID,
		Bucket:         h.workItem.Bucket,
		ObjectID:       h.workItem.ObjectID,
	})
	if err != nil {
		return false, fmt.Errorf("upsert journal failed: %w", err)
	}
	return isNew, nil
}