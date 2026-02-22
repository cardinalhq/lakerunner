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

	// Per-work waiters: workID → *workWaiter.
	// Used by DispatchAndWait for synchronous per-leaf blocking.
	waiters sync.Map
}

// workWaiter holds the caller's context and result channel for DispatchAndWait.
type workWaiter struct {
	ctx context.Context
	ch  chan workWaiterResult
}

// workWaiterResult delivers either a successful result or an error to a waiter.
type workWaiterResult struct {
	result *WorkResult
	err    error
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

// SetStreams sets the StreamSender after construction, allowing circular
// wiring between Manager and StreamManager.
func (m *Manager) SetStreams(streams StreamSender) {
	m.streams = streams
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
		m.resolveWaiter(msg.WorkId, nil, fmt.Errorf("work %s rejected, reassignment failed: %w", msg.WorkId, err))
		return
	}
	if reassigned == nil {
		// No worker available for reassignment.
		m.resolveWaiter(msg.WorkId, nil, fmt.Errorf("work %s rejected, no workers available for reassignment", msg.WorkId))
		return
	}
	recordReassignment()

	// Transfer per-work waiter to the new work ID so DispatchAndWait resolves
	// when the reassigned work completes.
	m.transferWaiter(msg.WorkId, reassigned.NewWorkID)

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
		m.resolveWaiter(reassigned.NewWorkID, nil, fmt.Errorf("reassigned work %s send failed: %w", reassigned.NewWorkID, err))
	}
}

// HandleWorkReady processes a WorkReady message: fetches artifact, sends ACK,
// delivers result to the per-work waiter or query result channel.
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

	// Determine fetch context and delivery target. Per-work waiters
	// (DispatchAndWait) carry their own context; query state (StartQuery)
	// provides context and channels for the streaming path.
	waiter := m.loadWaiter(msg.WorkId)
	var qs *queryState
	if v, ok := m.queries.Load(item.QueryID); ok {
		qs = v.(*queryState)
	}

	if waiter == nil && qs == nil {
		// No listener for this result.
		slog.Warn("HandleWorkReady: no waiter or query state",
			slog.String("work_id", msg.WorkId),
			slog.String("query_id", item.QueryID))
		return
	}

	// Pick the fetch context: prefer query state (longer-lived), fall back
	// to the waiter's caller context.
	var fetchCtx context.Context
	if qs != nil {
		fetchCtx = qs.ctx
		qs.pending.Add(1)
	} else {
		fetchCtx = waiter.ctx
	}

	go func() {
		if qs != nil {
			defer qs.pending.Done()
		}

		// Fetch artifact data.
		var data []byte
		if m.fetcher != nil {
			var fetchErr error
			data, fetchErr = m.fetcher.FetchArtifact(fetchCtx, msg.ArtifactUrl, msg.ArtifactChecksum)
			if fetchErr != nil {
				slog.Error("Artifact fetch failed",
					slog.String("work_id", msg.WorkId),
					slog.Any("error", fetchErr))
				fetchErrWrapped := fmt.Errorf("artifact fetch for work %s: %w", msg.WorkId, fetchErr)
				if !m.resolveWaiter(msg.WorkId, nil, fetchErrWrapped) && qs != nil {
					select {
					case qs.errCh <- fetchErrWrapped:
					default:
					}
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

		// Deliver to per-work waiter if registered, else to query channel.
		if !m.resolveWaiter(msg.WorkId, &result, nil) && qs != nil {
			select {
			case qs.resultsCh <- result:
			case <-qs.ctx.Done():
			}
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

	failErr := fmt.Errorf("work %s failed: %s", msg.WorkId, msg.Reason)

	// Deliver to per-work waiter if registered.
	if m.resolveWaiter(msg.WorkId, nil, failErr) {
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
	case qs.errCh <- failErr:
	default:
	}
}

// HandleWorkerDisconnected handles a worker disconnect by reassigning work.
func (m *Manager) HandleWorkerDisconnected(workerID string) {
	// Resolve any waiters for work items that won't be reassigned (canceled
	// by the coordinator because no other workers are available). We need
	// to identify these before DisconnectWorker mutates state.
	preItems := m.coord.Work.WorkForWorker(workerID)

	reassigned, err := m.coord.DisconnectWorker(workerID)
	if err != nil {
		slog.Error("DisconnectWorker failed",
			slog.String("worker_id", workerID),
			slog.Any("error", err))
		// Resolve any waiters for this worker's work — they won't complete.
		for _, item := range preItems {
			m.resolveWaiter(item.WorkID, nil, fmt.Errorf("worker %s disconnected: %w", workerID, err))
		}
		return
	}

	// Build set of reassigned old IDs for quick lookup.
	reassignedOldIDs := make(map[string]string, len(reassigned)) // oldWorkID → newWorkID
	for _, ra := range reassigned {
		reassignedOldIDs[ra.OldWorkID] = ra.NewWorkID
	}

	// Resolve waiters for work items that were canceled (not reassigned).
	for _, item := range preItems {
		if _, ok := reassignedOldIDs[item.WorkID]; !ok {
			m.resolveWaiter(item.WorkID, nil, fmt.Errorf("worker %s disconnected, no workers available for reassignment", workerID))
		}
	}

	for _, ra := range reassigned {
		recordReassignment()

		// Transfer per-work waiter to the new work ID.
		m.transferWaiter(ra.OldWorkID, ra.NewWorkID)

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
			m.resolveWaiter(ra.NewWorkID, nil, fmt.Errorf("reassigned work %s send failed: %w", ra.NewWorkID, err))
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

// DispatchAndWait dispatches a single work item and blocks until it completes,
// fails, or the context is canceled. The waiter is registered before the
// dispatch send so that a fast WorkReady response is never missed.
func (m *Manager) DispatchAndWait(ctx context.Context, queryID, leafID, affinityKey string, spec []byte) (*WorkResult, error) {
	workID := uuid.New().String()

	// Register waiter BEFORE dispatch to prevent the race where WorkReady
	// arrives before we start waiting.
	waiter := &workWaiter{ctx: ctx, ch: make(chan workWaiterResult, 1)}
	m.waiters.Store(workID, waiter)

	item, err := m.coord.AssignWork(queryID, leafID, workID, affinityKey, spec)
	if err != nil {
		m.waiters.Delete(workID)
		return nil, fmt.Errorf("assign work: %w", err)
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
		m.waiters.Delete(workID)
		_ = m.coord.CancelWork(workID)
		return nil, fmt.Errorf("send assign work to worker %s: %w", item.WorkerID, err)
	}

	recordWorkDispatched()

	select {
	case wr := <-waiter.ch:
		return wr.result, wr.err
	case <-ctx.Done():
		m.waiters.Delete(workID)
		_ = m.coord.CancelWork(workID)
		return nil, ctx.Err()
	}
}

// resolveWaiter attempts to deliver a result to a per-work waiter.
// Returns true if a waiter was found and resolved.
func (m *Manager) resolveWaiter(workID string, result *WorkResult, err error) bool {
	v, ok := m.waiters.LoadAndDelete(workID)
	if !ok {
		return false
	}
	w := v.(*workWaiter)
	w.ch <- workWaiterResult{result: result, err: err}
	return true
}

// loadWaiter returns the workWaiter for a given workID without removing it.
func (m *Manager) loadWaiter(workID string) *workWaiter {
	v, ok := m.waiters.Load(workID)
	if !ok {
		return nil
	}
	return v.(*workWaiter)
}

// transferWaiter moves a waiter from oldWorkID to newWorkID. This is needed
// when work is reassigned (reject/disconnect) and the waiter must track the
// new work item. Returns true if a waiter was transferred.
func (m *Manager) transferWaiter(oldWorkID, newWorkID string) bool {
	v, ok := m.waiters.LoadAndDelete(oldWorkID)
	if !ok {
		return false
	}
	m.waiters.Store(newWorkID, v)
	return true
}

// SpecFromPushDownRequest serializes a PushDownRequest-like struct to JSON for the spec field.
func SpecFromPushDownRequest(v any) ([]byte, error) {
	return json.Marshal(v)
}
