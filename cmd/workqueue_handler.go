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
