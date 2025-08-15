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

	"github.com/cardinalhq/lakerunner/lrdb"
)

type WorkqueueHandler struct {
	ctx          context.Context
	logger       *slog.Logger
	store        Store
	workItem     lrdb.WorkQueueClaimRow
	myInstanceID int64
}

func NewWorkqueueHandler(
	ctx context.Context,
	store Store,
	workItem lrdb.WorkQueueClaimRow,
	myInstanceID int64,
	opts ...HandlerOption,
) *WorkqueueHandler {
	options := &handlerOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(options)
	}

	return &WorkqueueHandler{
		ctx:          ctx,
		logger:       options.logger,
		store:        store,
		workItem:     workItem,
		myInstanceID: myInstanceID,
	}
}

func (h *WorkqueueHandler) CompleteWork() {
	if err := h.store.WorkQueueComplete(h.ctx, lrdb.WorkQueueCompleteParams{
		ID:       h.workItem.ID,
		WorkerID: h.myInstanceID,
	}); err != nil {
		h.logger.Error("WorkQueueComplete failed", slog.Any("error", err))
	}
}

func (h *WorkqueueHandler) RetryWork() {
	if err := h.store.WorkQueueFail(h.ctx, lrdb.WorkQueueFailParams{
		ID:       h.workItem.ID,
		WorkerID: h.myInstanceID,
	}); err != nil {
		h.logger.Error("WorkQueueFail failed", slog.Any("error", err))
	}
}