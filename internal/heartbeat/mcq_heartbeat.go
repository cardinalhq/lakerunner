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

package heartbeat

import (
	"context"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// MCQHeartbeatStore defines the minimal interface needed for MCQ heartbeat operations
type MCQHeartbeatStore interface {
	TouchMetricCompactionWork(ctx context.Context, params lrdb.TouchMetricCompactionWorkParams) error
}

// MCQHeartbeater manages periodic heartbeating for claimed metric compaction queue items
type MCQHeartbeater struct {
	db       MCQHeartbeatStore
	workerID int64
	items    []int64
	ll       *slog.Logger
	interval time.Duration
}

// NewMCQHeartbeater creates a new heartbeater for the given claimed MCQ items
func NewMCQHeartbeater(db MCQHeartbeatStore, workerID int64, items []int64) *MCQHeartbeater {
	return &MCQHeartbeater{
		db:       db,
		workerID: workerID,
		items:    items,
		ll:       slog.Default().With("component", "mcq_heartbeater", "worker_id", workerID, "item_count", len(items)),
		interval: time.Minute, // Heartbeat every minute as requested
	}
}

// Start begins the heartbeat process in a goroutine and returns a cancel function
func (h *MCQHeartbeater) Start(ctx context.Context) context.CancelFunc {
	// Create a child context that we can cancel independently
	heartbeatCtx, cancel := context.WithCancel(ctx)

	go h.run(heartbeatCtx)

	return cancel
}

// run is the main heartbeat loop
func (h *MCQHeartbeater) run(ctx context.Context) {
	if len(h.items) == 0 {
		h.ll.Debug("No items to heartbeat, exiting")
		return
	}

	h.ll.Debug("Starting heartbeat loop", "interval", h.interval)

	// Send initial heartbeat immediately
	h.sendHeartbeat(ctx)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.ll.Debug("Context cancelled, stopping heartbeat loop")
			return
		case <-ticker.C:
			h.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat sends a heartbeat for all claimed items
func (h *MCQHeartbeater) sendHeartbeat(ctx context.Context) {
	if len(h.items) == 0 {
		return
	}

	h.ll.Debug("Sending heartbeat", "item_ids", h.items)

	err := h.db.TouchMetricCompactionWork(ctx, lrdb.TouchMetricCompactionWorkParams{
		Ids:       h.items,
		ClaimedBy: h.workerID,
	})
	if err != nil {
		h.ll.Error("Failed to send heartbeat (continuing)", "error", err)
		return
	}

	h.ll.Debug("Heartbeat sent successfully")
}
