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
	"log/slog"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

type WorkqueueHandler struct {
	ctx context.Context
	ll  *slog.Logger
	mdb lrdb.StoreFull
	inf lrdb.WorkQueueClaimRow
}

func NewWorkqueueHandler(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	inf lrdb.WorkQueueClaimRow,
) *WorkqueueHandler {
	return &WorkqueueHandler{ctx, ll, mdb, inf}
}

func (h *WorkqueueHandler) CompleteWork() {
	if err := h.mdb.WorkQueueComplete(h.ctx, lrdb.WorkQueueCompleteParams{
		ID:       h.inf.ID,
		WorkerID: myInstanceID,
	}); err != nil {
		h.ll.Error("WorkQueueComplete failed", slog.Any("error", err))
	}
}

func (h *WorkqueueHandler) RetryWork() {
	if err := h.mdb.WorkQueueFail(h.ctx, lrdb.WorkQueueFailParams{
		ID:       h.inf.ID,
		WorkerID: myInstanceID,
	}); err != nil {
		h.ll.Error("WorkQueueFail failed", slog.Any("error", err))
	}
}
