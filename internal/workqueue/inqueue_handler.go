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
)

type InqueueHandler struct {
	logger         *slog.Logger
	store          InqueueStore
	workItem       InqueueItem
	maxWorkRetries int
}

func NewInqueueHandler(
	workItem InqueueItem,
	store InqueueStore,
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
		logger:         options.logger,
		store:          store,
		workItem:       workItem,
		maxWorkRetries: maxWorkRetries,
	}
}

func (h *InqueueHandler) retryWork(ctx context.Context) error {
	if err := h.store.ReleaseWork(ctx, h.workItem.ID, h.workItem.ClaimedBy); err != nil {
		h.logger.Error("release failed", slog.Any("error", err))
		return err
	}
	return nil
}

func (h *InqueueHandler) deleteWork(ctx context.Context) error {
	if err := h.store.DeleteWork(ctx, h.workItem.ID, h.workItem.ClaimedBy); err != nil {
		h.logger.Error("delete failed", slog.Any("error", err))
		return err
	}
	return nil
}

func (h *InqueueHandler) CompleteWork(ctx context.Context) error {
	return h.deleteWork(ctx)
}

func (h *InqueueHandler) RetryWork(ctx context.Context) error {
	if err := h.deleteJournal(ctx); err != nil {
		return err
	}

	if h.workItem.Tries+1 > int32(h.maxWorkRetries) {
		h.logger.Info("too many retries, deleting", slog.Int("newTries", int(h.workItem.Tries+1)))
		return h.deleteWork(ctx)
	} else {
		return h.retryWork(ctx)
	}
}

func (h *InqueueHandler) deleteJournal(ctx context.Context) error {
	if err := h.store.DeleteJournal(ctx, h.workItem.OrganizationID, h.workItem.Bucket, h.workItem.ObjectID); err != nil {
		h.logger.Error("journal delete failed", slog.Any("error", err))
		return err
	}
	return nil
}

// IsNewWork returns true if the row is new.
func (h *InqueueHandler) IsNewWork(ctx context.Context) (isNew bool, err error) {
	isNew, err = h.store.UpsertJournal(ctx, h.workItem.OrganizationID, h.workItem.Bucket, h.workItem.ObjectID)
	if err != nil {
		return false, fmt.Errorf("upsert journal failed: %w", err)
	}
	return isNew, nil
}
