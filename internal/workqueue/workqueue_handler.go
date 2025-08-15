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
	"log/slog"
)

type WorkqueueHandler struct {
	logger   *slog.Logger
	store    WorkQueueStore
	workItem WorkItem
}

func NewWorkqueueHandler(
	workItem WorkItem,
	store WorkQueueStore,
	opts ...HandlerOption,
) *WorkqueueHandler {
	options := &handlerOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(options)
	}

	return &WorkqueueHandler{
		logger:   options.logger,
		store:    store,
		workItem: workItem,
	}
}

func (h *WorkqueueHandler) CompleteWork(ctx context.Context) error {
	if err := h.store.CompleteWork(ctx, h.workItem.ID, h.workItem.WorkerID); err != nil {
		h.logger.Error("CompleteWork failed", slog.Any("error", err))
		return err
	}
	return nil
}

func (h *WorkqueueHandler) RetryWork(ctx context.Context) error {
	if err := h.store.FailWork(ctx, h.workItem.ID, h.workItem.WorkerID); err != nil {
		h.logger.Error("FailWork failed", slog.Any("error", err))
		return err
	}
	return nil
}
