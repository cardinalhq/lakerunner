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
)

// HeartbeatFunc is the function signature for heartbeat callbacks
type HeartbeatFunc func(ctx context.Context) error

// Heartbeater manages periodic execution of a heartbeat function
type Heartbeater struct {
	heartbeatFunc HeartbeatFunc
	ll            *slog.Logger
	interval      time.Duration
}

// New creates a new generic heartbeater with the given callback function
func New(heartbeatFunc HeartbeatFunc, interval time.Duration, logger *slog.Logger) *Heartbeater {
	if logger == nil {
		logger = slog.Default()
	}

	return &Heartbeater{
		heartbeatFunc: heartbeatFunc,
		ll:            logger.With("component", "heartbeater"),
		interval:      interval,
	}
}

// Start begins the heartbeat process in a goroutine and returns a cancel function
func (h *Heartbeater) Start(ctx context.Context) context.CancelFunc {
	// Create a child context that we can cancel independently
	heartbeatCtx, cancel := context.WithCancel(ctx)

	go h.run(heartbeatCtx)

	return cancel
}

// run is the main heartbeat loop
func (h *Heartbeater) run(ctx context.Context) {
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

// sendHeartbeat calls the configured heartbeat function
func (h *Heartbeater) sendHeartbeat(ctx context.Context) {
	h.ll.Debug("Sending heartbeat")

	err := h.heartbeatFunc(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.ll.Error("Failed to send heartbeat (continuing)", "error", err)
		return
	}

	h.ll.Debug("Heartbeat sent successfully")
}
