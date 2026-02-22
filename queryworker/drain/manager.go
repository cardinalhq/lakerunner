// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package drain

import (
	"context"
	"log/slog"
	"time"
)

const DefaultDrainTimeout = 60 * time.Second

// ControlStreamDrainer sets the control stream server's drain state.
type ControlStreamDrainer interface {
	SetDraining(draining bool)
}

// WorkDrainer controls the work manager's drain state.
type WorkDrainer interface {
	SetDraining(draining bool)
	WaitForDrain(ctx context.Context) error
}

// Manager orchestrates the drain sequence on the worker side.
// On SIGTERM: set draining on both controlstream and work manager,
// wait for in-flight work, then signal completion.
type Manager struct {
	controlStream ControlStreamDrainer
	workManager   WorkDrainer
	drainTimeout  time.Duration
}

// NewManager creates a drain manager.
func NewManager(cs ControlStreamDrainer, wm WorkDrainer) *Manager {
	return &Manager{
		controlStream: cs,
		workManager:   wm,
		drainTimeout:  DefaultDrainTimeout,
	}
}

// SetDrainTimeout overrides the default drain timeout.
func (m *Manager) SetDrainTimeout(d time.Duration) {
	m.drainTimeout = d
}

// Drain initiates the drain sequence. It:
// 1. Sets draining on the control stream server (broadcasts status to all API instances).
// 2. Sets draining on the work manager (rejects new work).
// 3. Waits for in-flight work to complete (or timeout).
func (m *Manager) Drain(ctx context.Context) error {
	slog.Info("Starting drain sequence")

	if m.controlStream != nil {
		m.controlStream.SetDraining(true)
	}
	if m.workManager != nil {
		m.workManager.SetDraining(true)
	}

	drainCtx, cancel := context.WithTimeout(ctx, m.drainTimeout)
	defer cancel()

	if m.workManager != nil {
		if err := m.workManager.WaitForDrain(drainCtx); err != nil {
			slog.Warn("Drain did not complete cleanly", slog.Any("error", err))
			return err
		}
	}

	slog.Info("Drain complete")
	return nil
}
