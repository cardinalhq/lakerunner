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
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/core/workcoord"
	"github.com/cardinalhq/lakerunner/core/workcoordpb"
	"github.com/cardinalhq/lakerunner/queryapi/streammanager"
)

// ArtifactFetcher retrieves a completed artifact from a worker.
type ArtifactFetcher interface {
	FetchArtifact(ctx context.Context, artifactURL string, expectedChecksum string) ([]byte, error)
}

// WorkResult represents a completed work item with its artifact data.
type WorkResult struct {
	WorkID           string
	QueryID          string
	LeafID           string
	ArtifactData     []byte
	ArtifactChecksum string
	RowCount         int64
	MinTs            int64
	MaxTs            int64
}

// uuidIDGen generates work IDs using UUIDs.
type uuidIDGen struct{}

func (u *uuidIDGen) NewWorkID() string { return uuid.New().String() }

// StreamSender abstracts the stream manager's Send method for testing.
type StreamSender interface {
	Send(workerID string, msg *workcoordpb.APIMessage) error
}

// Manager orchestrates query work dispatch and result collection on the API side.
type Manager struct {
	coord   *workcoord.Coordinator
	streams StreamSender
	fetcher ArtifactFetcher

	// Per-query state: queryID → *queryState.
	queries sync.Map
}

// queryState tracks the in-flight state for a single query execution.
type queryState struct {
	ctx       context.Context
	cancel    context.CancelFunc
	resultsCh chan WorkResult
	errCh     chan error
	pending   sync.WaitGroup
}

// NewManager creates an API work manager.
func NewManager(streams StreamSender, fetcher ArtifactFetcher) *Manager {
	m := &Manager{
		coord:   workcoord.NewCoordinator(&uuidIDGen{}),
		streams: streams,
		fetcher: fetcher,
	}
	registerQueueDepthGauge(m)
	return m
}

// Coordinator returns the underlying workcoord.Coordinator for direct access
// (e.g., worker registration from stream manager's MessageHandler).
func (m *Manager) Coordinator() *workcoord.Coordinator {
	return m.coord
}

// DispatchWork creates and assigns a work item to a worker, sending AssignWork
// on the control stream. The spec is the opaque work payload (serialized PushDownRequest).
func (m *Manager) DispatchWork(queryID, leafID, affinityKey string, spec []byte) (string, error) {
	workID := uuid.New().String()

	item, err := m.coord.AssignWork(queryID, leafID, workID, affinityKey, spec)
	if err != nil {
		return "", fmt.Errorf("assign work: %w", err)
	}

	msg := &workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_AssignWork{
			AssignWork: &workcoordpb.AssignWork{
				QueryId:     queryID,
				LeafId:      leafID,
				WorkId:      workID,
				AffinityKey: affinityKey,
				Spec:        spec,
			},
		},
	}

	if err := m.streams.Send(item.WorkerID, msg); err != nil {
		// Failed to send — cancel the work item.
		_ = m.coord.CancelWork(workID)
		return "", fmt.Errorf("send assign work to worker %s: %w", item.WorkerID, err)
	}

	recordWorkDispatched()
	return workID, nil
}

// StartQuery creates a query tracking context. Results will be sent to the
// returned channel. Call CancelQuery when done.
func (m *Manager) StartQuery(ctx context.Context, queryID string, bufferSize int) (<-chan WorkResult, <-chan error) {
	qctx, cancel := context.WithCancel(ctx)
	qs := &queryState{
		ctx:       qctx,
		cancel:    cancel,
		resultsCh: make(chan WorkResult, bufferSize),
		errCh:     make(chan error, 16),
	}
	m.queries.Store(queryID, qs)

	// Close results channel when all pending work is done or context is canceled.
	go func() {
		<-qctx.Done()
		// Give pending handlers a moment to finish.
		qs.pending.Wait()
		close(qs.resultsCh)
		close(qs.errCh)
	}()

	return qs.resultsCh, qs.errCh
}

// CancelQuery cancels all work for a query and cleans up tracking.
// It sends CancelWork messages to workers for all non-terminal work items
// before performing local state cleanup.
func (m *Manager) CancelQuery(queryID string) {
	// Send CancelWork to workers for all non-terminal items before local cleanup.
	items := m.coord.Work.WorkForQuery(queryID)
	for _, item := range items {
		if !item.State.IsTerminal() {
			_ = m.streams.Send(item.WorkerID, &workcoordpb.APIMessage{
				Msg: &workcoordpb.APIMessage_CancelWork{
					CancelWork: &workcoordpb.CancelWork{
						WorkId: item.WorkID,
					},
				},
			})
		}
	}

	if v, ok := m.queries.LoadAndDelete(queryID); ok {
		qs := v.(*queryState)
		qs.cancel()
	}
	m.coord.CancelQueryWork(queryID)
}

// --- MessageHandler implementation for streammanager ---

// HandleWorkAccepted processes a WorkAccepted message from a worker.
func (m *Manager) HandleWorkAccepted(_ string, msg *workcoordpb.WorkAccepted) {
	if err := m.coord.HandleWorkAccepted(msg.WorkId); err != nil {
		slog.Warn("HandleWorkAccepted failed",
			slog.String("work_id", msg.WorkId),
			slog.Any("error", err))
		return
	}
	recordWorkAccepted()
}

// HandleWorkRejected processes a WorkRejected message from a worker.
func (m *Manager) HandleWorkRejected(workerID string, msg *workcoordpb.WorkRejected) {
	recordWorkRejected()
	reassigned, err := m.coord.HandleWorkRejected(msg.WorkId)
	if err != nil {
		slog.Error("HandleWorkRejected failed",
			slog.String("work_id", msg.WorkId),
			slog.Any("error", err))
		return
	}
	if reassigned == nil {
		return
	}
	recordReassignment()

	sendMsg := &workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_AssignWork{
			AssignWork: &workcoordpb.AssignWork{
				QueryId:     reassigned.Item.QueryID,
				LeafId:      reassigned.Item.LeafID,
				WorkId:      reassigned.NewWorkID,
				AffinityKey: reassigned.Item.AffinityKey,
				Spec:        reassigned.Item.Spec,
			},
		},
	}

	if err := m.streams.Send(reassigned.NewWorkerID, sendMsg); err != nil {
		slog.Error("Failed to send reassigned work",
			slog.String("new_work_id", reassigned.NewWorkID),
			slog.String("new_worker_id", reassigned.NewWorkerID),
			slog.Any("error", err))
		_ = m.coord.CancelWork(reassigned.NewWorkID)
	}
}

// HandleWorkReady processes a WorkReady message: fetches artifact, sends ACK,
// delivers result to the query's result channel.
func (m *Manager) HandleWorkReady(workerID string, msg *workcoordpb.WorkReady) {
	artifact := workcoord.ArtifactInfo{
		ArtifactURL:          msg.ArtifactUrl,
		ArtifactSizeBytes:    msg.ArtifactSizeBytes,
		ArtifactChecksum:     msg.ArtifactChecksum,
		RowCount:             msg.RowCount,
		MinTs:                msg.MinTs,
		MaxTs:                msg.MaxTs,
		ParquetSchemaVersion: msg.ParquetSchemaVersion,
	}

	if err := m.coord.HandleWorkReady(msg.WorkId, artifact); err != nil {
		slog.Warn("HandleWorkReady transition failed",
			slog.String("work_id", msg.WorkId),
			slog.Any("error", err))
		return
	}

	// Look up which query this work belongs to.
	item, err := m.coord.Work.Get(msg.WorkId)
	if err != nil {
		slog.Error("HandleWorkReady: work item not found",
			slog.String("work_id", msg.WorkId))
		return
	}

	v, ok := m.queries.Load(item.QueryID)
	if !ok {
		// Query was already canceled/completed.
		return
	}
	qs := v.(*queryState)
	qs.pending.Add(1)

	go func() {
		defer qs.pending.Done()

		// Fetch artifact data.
		var data []byte
		if m.fetcher != nil {
			var fetchErr error
			data, fetchErr = m.fetcher.FetchArtifact(qs.ctx, msg.ArtifactUrl, msg.ArtifactChecksum)
			if fetchErr != nil {
				slog.Error("Artifact fetch failed",
					slog.String("work_id", msg.WorkId),
					slog.Any("error", fetchErr))
				select {
				case qs.errCh <- fmt.Errorf("artifact fetch for work %s: %w", msg.WorkId, fetchErr):
				default:
				}
				return
			}
		}

		// Send ACK.
		_ = m.streams.Send(workerID, &workcoordpb.APIMessage{
			Msg: &workcoordpb.APIMessage_ArtifactAck{
				ArtifactAck: &workcoordpb.ArtifactAck{WorkId: msg.WorkId},
			},
		})

		result := WorkResult{
			WorkID:           msg.WorkId,
			QueryID:          item.QueryID,
			LeafID:           item.LeafID,
			ArtifactData:     data,
			ArtifactChecksum: msg.ArtifactChecksum,
			RowCount:         msg.RowCount,
			MinTs:            msg.MinTs,
			MaxTs:            msg.MaxTs,
		}

		select {
		case qs.resultsCh <- result:
		case <-qs.ctx.Done():
		}
	}()
}

// HandleWorkFailed processes a WorkFailed message from a worker.
func (m *Manager) HandleWorkFailed(_ string, msg *workcoordpb.WorkFailed) {
	if err := m.coord.HandleWorkFailed(msg.WorkId); err != nil {
		slog.Warn("HandleWorkFailed transition failed",
			slog.String("work_id", msg.WorkId),
			slog.Any("error", err))
		return
	}

	item, err := m.coord.Work.Get(msg.WorkId)
	if err != nil {
		return
	}

	v, ok := m.queries.Load(item.QueryID)
	if !ok {
		return
	}
	qs := v.(*queryState)

	select {
	case qs.errCh <- fmt.Errorf("work %s failed: %s", msg.WorkId, msg.Reason):
	default:
	}
}

// HandleWorkerDisconnected handles a worker disconnect by reassigning work.
func (m *Manager) HandleWorkerDisconnected(workerID string) {
	reassigned, err := m.coord.DisconnectWorker(workerID)
	if err != nil {
		slog.Error("DisconnectWorker failed",
			slog.String("worker_id", workerID),
			slog.Any("error", err))
		return
	}

	for _, ra := range reassigned {
		recordReassignment()
		sendMsg := &workcoordpb.APIMessage{
			Msg: &workcoordpb.APIMessage_AssignWork{
				AssignWork: &workcoordpb.AssignWork{
					QueryId:     ra.Item.QueryID,
					LeafId:      ra.Item.LeafID,
					WorkId:      ra.NewWorkID,
					AffinityKey: ra.Item.AffinityKey,
					Spec:        ra.Item.Spec,
				},
			},
		}

		if err := m.streams.Send(ra.NewWorkerID, sendMsg); err != nil {
			slog.Error("Failed to send reassigned work after disconnect",
				slog.String("new_work_id", ra.NewWorkID),
				slog.String("new_worker_id", ra.NewWorkerID),
				slog.Any("error", err))
			_ = m.coord.CancelWork(ra.NewWorkID)
		}
	}
}

// HandleWorkerStatus processes a WorkerStatus message from a worker.
func (m *Manager) HandleWorkerStatus(workerID string, msg *workcoordpb.WorkerStatus) {
	if err := m.coord.RegisterWorker(workerID); err != nil {
		// Already registered. If disconnected, restore Alive so work can be scheduled.
		w, werr := m.coord.Workers.Get(workerID)
		if werr == nil && !w.Alive {
			_ = m.coord.ReconnectWorker(workerID)
		}
	}

	if msg.Draining {
		if _, err := m.coord.DrainWorker(workerID); err != nil {
			slog.Debug("DrainWorker failed", slog.String("worker_id", workerID), slog.Any("error", err))
		}
	} else if msg.AcceptingWork {
		_ = m.coord.Workers.SetAcceptingWork(workerID, true)
	} else {
		_ = m.coord.Workers.SetAcceptingWork(workerID, false)
	}
}

// MakeStreamHandler returns a streammanager.MessageHandler that delegates to this manager.
func (m *Manager) MakeStreamHandler() streammanager.MessageHandler {
	return &streamHandlerAdapter{mgr: m}
}

type streamHandlerAdapter struct {
	mgr *Manager
}

func (a *streamHandlerAdapter) OnWorkerStatus(workerID string, msg *workcoordpb.WorkerStatus) {
	a.mgr.HandleWorkerStatus(workerID, msg)
}

func (a *streamHandlerAdapter) OnWorkAccepted(workerID string, msg *workcoordpb.WorkAccepted) {
	a.mgr.HandleWorkAccepted(workerID, msg)
}

func (a *streamHandlerAdapter) OnWorkRejected(workerID string, msg *workcoordpb.WorkRejected) {
	a.mgr.HandleWorkRejected(workerID, msg)
}

func (a *streamHandlerAdapter) OnWorkReady(workerID string, msg *workcoordpb.WorkReady) {
	a.mgr.HandleWorkReady(workerID, msg)
}

func (a *streamHandlerAdapter) OnWorkFailed(workerID string, msg *workcoordpb.WorkFailed) {
	a.mgr.HandleWorkFailed(workerID, msg)
}

func (a *streamHandlerAdapter) OnWorkerDisconnected(workerID string) {
	a.mgr.HandleWorkerDisconnected(workerID)
}

// SpecFromPushDownRequest serializes a PushDownRequest-like struct to JSON for the spec field.
func SpecFromPushDownRequest(v any) ([]byte, error) {
	return json.Marshal(v)
}
