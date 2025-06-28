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
	"fmt"
	"log/slog"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
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
