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

package workmanager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
	"github.com/cardinalhq/lakerunner/queryworker/controlstream"
)

// Executor is the interface for executing work items on the worker side.
// Implementations handle the actual DuckDB query execution and artifact creation.
type Executor interface {
	// Execute runs the work described by spec and returns the artifact metadata.
	// The artifact file should be written to disk before returning.
	Execute(ctx context.Context, workID string, spec []byte) (*ArtifactResult, error)
}

// ArtifactResult describes the completed artifact for a work item.
type ArtifactResult struct {
	ArtifactURL          string
	ArtifactSizeBytes    int64
	ArtifactChecksum     string
	RowCount             int64
	MinTs                int64
	MaxTs                int64
	ParquetSchemaVersion int32
}

// workItem tracks an in-flight work item on the worker side.
type workItem struct {
	workID    string
	sessionID string
	cancel    context.CancelFunc
}

// Manager processes work assignments on the worker side.
type Manager struct {
	executor    Executor
	maxInFlight int

	mu       sync.RWMutex
	items    map[string]*workItem // workID → item
	draining atomic.Bool

	// Semaphore for limiting concurrent work execution.
	sem chan struct{}
}

// NewManager creates a worker-side work manager.
func NewManager(executor Executor, maxInFlight int) *Manager {
	if maxInFlight <= 0 {
		maxInFlight = 4
	}
	m := &Manager{
		executor:    executor,
		maxInFlight: maxInFlight,
		items:       make(map[string]*workItem),
		sem:         make(chan struct{}, maxInFlight),
	}
	registerExecutionQueueDepthGauge(m)
	return m
}

// OnAssignWork handles an incoming AssignWork message from the control stream.
// Implements controlstream.WorkHandler.
func (m *Manager) OnAssignWork(session *controlstream.Session, msg *workcoordpb.AssignWork) {
	// Reject if draining.
	if m.draining.Load() {
		if !session.Send(&workcoordpb.WorkerMessage{
			Msg: &workcoordpb.WorkerMessage_WorkRejected{
				WorkRejected: &workcoordpb.WorkRejected{
					WorkId: msg.WorkId,
					Reason: "worker is draining",
				},
			},
		}) {
			slog.Warn("Failed to send WorkRejected", slog.String("work_id", msg.WorkId))
		}
		return
	}

	// Check capacity via non-blocking semaphore acquire.
	select {
	case m.sem <- struct{}{}:
	default:
		if !session.Send(&workcoordpb.WorkerMessage{
			Msg: &workcoordpb.WorkerMessage_WorkRejected{
				WorkRejected: &workcoordpb.WorkRejected{
					WorkId: msg.WorkId,
					Reason: "at capacity",
				},
			},
		}) {
			slog.Warn("Failed to send WorkRejected", slog.String("work_id", msg.WorkId))
		}
		return
	}

	// Create cancellable context for this work item.
	ctx, cancel := context.WithCancel(context.Background())

	wi := &workItem{
		workID:    msg.WorkId,
		sessionID: session.ID,
		cancel:    cancel,
	}

	m.mu.Lock()
	m.items[msg.WorkId] = wi
	m.mu.Unlock()

	// Accept the work. If the accept can't be delivered, the API won't know
	// we have it and completion delivery will likely also fail. Cancel early
	// to avoid wasted execution.
	if !session.Send(&workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkAccepted{
			WorkAccepted: &workcoordpb.WorkAccepted{
				WorkId: msg.WorkId,
			},
		},
	}) {
		slog.Error("Failed to send WorkAccepted, aborting work", slog.String("work_id", msg.WorkId))
		cancel()
		m.mu.Lock()
		delete(m.items, msg.WorkId)
		m.mu.Unlock()
		<-m.sem
		return
	}

	// Execute asynchronously.
	go func() {
		defer func() { <-m.sem }()
		defer func() {
			m.mu.Lock()
			delete(m.items, msg.WorkId)
			m.mu.Unlock()
		}()

		result, err := m.executor.Execute(ctx, msg.WorkId, msg.Spec)
		if err != nil {
			if ctx.Err() != nil {
				// Canceled, don't send failure.
				return
			}
			slog.Error("Work execution failed",
				slog.String("work_id", msg.WorkId),
				slog.Any("error", err))
			if !session.Send(&workcoordpb.WorkerMessage{
				Msg: &workcoordpb.WorkerMessage_WorkFailed{
					WorkFailed: &workcoordpb.WorkFailed{
						WorkId: msg.WorkId,
						Reason: err.Error(),
					},
				},
			}) {
				slog.Error("Failed to send WorkFailed", slog.String("work_id", msg.WorkId))
			}
			recordWorkFailed()
			return
		}

		if !session.Send(&workcoordpb.WorkerMessage{
			Msg: &workcoordpb.WorkerMessage_WorkReady{
				WorkReady: &workcoordpb.WorkReady{
					WorkId:               msg.WorkId,
					ArtifactUrl:          result.ArtifactURL,
					ArtifactSizeBytes:    result.ArtifactSizeBytes,
					ArtifactChecksum:     result.ArtifactChecksum,
					RowCount:             result.RowCount,
					MinTs:                result.MinTs,
					MaxTs:                result.MaxTs,
					ParquetSchemaVersion: result.ParquetSchemaVersion,
				},
			},
		}) {
			slog.Error("Failed to send WorkReady", slog.String("work_id", msg.WorkId))
		} else {
			recordWorkCompleted()
		}
	}()
}

// OnCancelWork cancels an in-flight work item.
func (m *Manager) OnCancelWork(_ *controlstream.Session, msg *workcoordpb.CancelWork) {
	m.mu.RLock()
	wi, exists := m.items[msg.WorkId]
	m.mu.RUnlock()

	if exists {
		wi.cancel()
		slog.Debug("Work canceled", slog.String("work_id", msg.WorkId))
	}
}

// OnArtifactAck acknowledges that the API has fetched the artifact.
// The artifact spool manager (Task #26) will handle cleanup.
func (m *Manager) OnArtifactAck(_ *controlstream.Session, msg *workcoordpb.ArtifactAck) {
	slog.Debug("Artifact acknowledged", slog.String("work_id", msg.WorkId))
}

// OnSessionClosed cancels all work items for the disconnected session.
func (m *Manager) OnSessionClosed(sessionID string) {
	m.mu.RLock()
	var toCancel []string
	for workID, wi := range m.items {
		if wi.sessionID == sessionID {
			toCancel = append(toCancel, workID)
		}
	}
	m.mu.RUnlock()

	for _, workID := range toCancel {
		m.mu.RLock()
		wi, exists := m.items[workID]
		m.mu.RUnlock()
		if exists {
			wi.cancel()
		}
	}

	if len(toCancel) > 0 {
		slog.Info("Canceled work for disconnected session",
			slog.String("session_id", sessionID),
			slog.Int("count", len(toCancel)))
	}
}

// SetDraining sets the drain state. When draining, new work is rejected.
func (m *Manager) SetDraining(draining bool) {
	m.draining.Store(draining)
}

// InFlightCount returns the number of currently executing work items.
func (m *Manager) InFlightCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

// WaitForDrain blocks until all in-flight work completes or ctx is canceled.
func (m *Manager) WaitForDrain(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.InFlightCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("drain timed out with %d items in flight", m.InFlightCount())
		case <-ticker.C:
		}
	}
}
