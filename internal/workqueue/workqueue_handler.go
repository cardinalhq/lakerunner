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
	"time"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"log/slog"
)

const (
	// DefaultHeartbeatInterval is the default interval for workqueue heartbeats
	DefaultHeartbeatInterval = 1 * time.Minute

	// MinHeartbeatInterval is the minimum allowed heartbeat interval
	MinHeartbeatInterval = 10 * time.Second

	// StaleExpiryMultiplier is the number of missed heartbeats before a work item is considered stale
	StaleExpiryMultiplier = 4
)

type WorkqueueHandler struct {
	config   *Config
	store    WorkQueueStore
	workItem WorkItem
}

func NewWorkqueueHandler(
	workItem WorkItem,
	store WorkQueueStore,
	opts ...HandlerOption,
) *WorkqueueHandler {
	options := &handlerOptions{
		config: &Config{
			MaxWorkRetries:     10,
			WorkFailRequeueTTL: 1 * time.Minute,
			LockTTL:            5 * time.Minute,
			LockTTLDead:        20 * time.Minute,
		}, // default values
	}
	for _, opt := range opts {
		opt(options)
	}

	return &WorkqueueHandler{
		config:   options.config,
		store:    store,
		workItem: workItem,
	}
}

func (h *WorkqueueHandler) CompleteWork(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	if err := h.store.CompleteWork(ctx, h.workItem.ID, h.workItem.WorkerID); err != nil {
		ll.Error("CompleteWork failed", slog.Any("error", err))
		return err
	}
	return nil
}

func (h *WorkqueueHandler) RetryWork(ctx context.Context) error {
	ll := logctx.FromContext(ctx)
	if err := h.store.FailWork(ctx, h.workItem.ID, h.workItem.WorkerID, int32(h.config.MaxWorkRetries), h.config.WorkFailRequeueTTL); err != nil {
		ll.Error("FailWork failed", slog.Any("error", err))
		return err
	}
	return nil
}
