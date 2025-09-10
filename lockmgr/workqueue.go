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

package lockmgr

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// WorkQueueDB defines the database operations needed by the work queue manager.
type WorkQueueDB interface {
	WorkQueueClaim(ctx context.Context, params lrdb.WorkQueueClaimParams) (lrdb.WorkQueueClaimRow, error)
	WorkQueueComplete(ctx context.Context, params lrdb.WorkQueueCompleteParams) error
	WorkQueueFail(ctx context.Context, params lrdb.WorkQueueFailParams) error
	WorkQueueDelete(ctx context.Context, params lrdb.WorkQueueDeleteParams) error
	WorkQueueHeartbeat(ctx context.Context, params lrdb.WorkQueueHeartbeatParams) error
}

// Ensure lrdb.StoreFull satisfies WorkQueueDB interface.
var _ WorkQueueDB = (lrdb.StoreFull)(nil)

// wqManager drives fetching items from the DB and heartbeating them.
type wqManager struct {
	// constructor parameters
	db              WorkQueueDB
	workerID        int64
	signal          lrdb.SignalEnum
	action          lrdb.ActionEnum
	frequencies     []int32
	minimumPriority int32

	// defaults but set via options
	heartbeatInterval time.Duration

	// currently acquired work item IDs
	acquiredIDs []int64

	// work tracking for graceful shutdown
	outstandingWork sync.WaitGroup
	shutdownOnce    sync.Once
	done            chan struct{}

	// channels for internal processing
	getWork      chan *workRequest
	completeWork chan *workCompleteRequest
	failWork     chan *workFailRequest
	deleteWork   chan *workDeleteRequest
}

type workCompleteRequest struct {
	WorkItem *WorkItem
	resp     chan error
}

func (m *wqManager) completeWorkItem(ctx context.Context, w *WorkItem) error {
	defer m.outstandingWork.Done() // Mark work as completed

	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})

	err := m.db.WorkQueueComplete(ctx, lrdb.WorkQueueCompleteParams{
		ID:       w.id,
		WorkerID: m.workerID,
	})
	return err
}

type workFailRequest struct {
	WorkItem *WorkItem
	resp     chan error
}

type workDeleteRequest struct {
	WorkItem *WorkItem
	resp     chan error
}

func (m *wqManager) failWorkItem(ctx context.Context, w *WorkItem) error {
	defer m.outstandingWork.Done() // Mark work as failed (completed)

	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})
	err := m.db.WorkQueueFail(ctx, lrdb.WorkQueueFailParams{
		ID:       w.id,
		WorkerID: m.workerID,
	})
	return err
}

func (m *wqManager) deleteWorkItem(ctx context.Context, w *WorkItem) error {
	defer m.outstandingWork.Done() // Mark work as deleted (completed)

	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})
	err := m.db.WorkQueueDelete(ctx, lrdb.WorkQueueDeleteParams{
		ID:       w.id,
		WorkerID: m.workerID,
	})
	return err
}

// NewWorkQueueManager constructs a manager to manage obtaining and completing work items.
// The work items are automatically heartbeated to retain ownership of the work items.
func NewWorkQueueManager(
	db WorkQueueDB,
	workerID int64,
	sig lrdb.SignalEnum,
	act lrdb.ActionEnum,
	frequencies []int32,
	minimumPriority int32,
	opts ...Options,
) WorkQueueManager {
	m := &wqManager{
		db:                db,
		workerID:          workerID,
		signal:            sig,
		action:            act,
		frequencies:       frequencies,
		minimumPriority:   minimumPriority,
		done:              make(chan struct{}),
		getWork:           make(chan *workRequest, 5),
		completeWork:      make(chan *workCompleteRequest, 5),
		failWork:          make(chan *workFailRequest, 5),
		deleteWork:        make(chan *workDeleteRequest, 5),
		heartbeatInterval: time.Minute,
	}
	for _, opt := range opts {
		opt.apply(m)
	}
	return m
}

type WorkQueueManager interface {
	// Run starts the background goroutine that processes work requests.
	Run(ctx context.Context)
	// RequestWork requests the next work item, returning nil if no work is available.
	// Returns an error if the context is cancelled or the manager is shutting down.
	RequestWork(ctx context.Context) (*WorkItem, error)
	// WaitForOutstandingWork waits for all outstanding work items to complete.
	// Returns when all work is done or the context is cancelled.
	WaitForOutstandingWork(ctx context.Context) error
}

var _ WorkQueueManager = (*wqManager)(nil)

// Run starts a background goroutine that listens for work‐requests (on getWork) and
// periodically heartbeats all acquired IDs.  When ctx is canceled, the loop exits.
func (m *wqManager) Run(ctx context.Context) {
	go m.runLoop(ctx)
}

// runLoop is the internal processing loop.
func (m *wqManager) runLoop(ctx context.Context) {
	defer m.shutdownOnce.Do(func() {
		close(m.done) // Signal that manager is shut down
	})

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-m.getWork:
			work, err := m.getWorkItem(ctx)
			req.resp <- &workRequestResponse{
				work: work,
				err:  err,
			}

		case req := <-m.completeWork:
			req.resp <- m.completeWorkItem(ctx, req.WorkItem)

		case req := <-m.failWork:
			req.resp <- m.failWorkItem(ctx, req.WorkItem)

		case req := <-m.deleteWork:
			req.resp <- m.deleteWorkItem(ctx, req.WorkItem)

		case <-time.Tick(m.heartbeatInterval):
			m.heartbeat(ctx)
		}
	}
}

// GetWork queries the DB for the next row and records its ID in acquiredIDs.
func (m *wqManager) getWorkItem(ctx context.Context) (*WorkItem, error) {
	row, err := m.db.WorkQueueClaim(ctx, lrdb.WorkQueueClaimParams{
		WorkerID:    m.workerID,
		Signal:      m.signal,
		Action:      m.action,
		TargetFreqs: m.frequencies,
		MinPriority: m.minimumPriority,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if strings.Contains(err.Error(), "23P01") { // constraint violation, ignore for now
			return nil, nil
		}
		return nil, err
	}

	work := &WorkItem{
		id:          row.ID,
		orgId:       row.OrganizationID,
		instanceNum: row.InstanceNum,
		dateint:     row.Dateint,
		frequencyMs: row.FrequencyMs,
		signal:      row.Signal,
		tries:       row.Tries,
		action:      row.Action,
		tsRange:     row.TsRange,
		priority:    row.Priority,
		runnableAt:  row.RunnableAt,
		mgr:         m,
		slotId:      row.SlotID,
	}

	m.acquiredIDs = append(m.acquiredIDs, work.id)
	return work, nil
}

func (m *wqManager) heartbeat(ctx context.Context) {
	err := m.db.WorkQueueHeartbeat(ctx, lrdb.WorkQueueHeartbeatParams{
		Ids:      m.acquiredIDs,
		WorkerID: m.workerID,
	})
	if err != nil {
		ll := logctx.FromContext(ctx)
		ll.Error("failed to heartbeat work queue (continuing)", slog.Any("error", err))
		return
	}
}

// workRequest is used internally to request a work item and receive the result.
type workRequest struct {
	resp chan *workRequestResponse
}

type workRequestResponse struct {
	work *WorkItem
	err  error
}

// RequestWork requests the next work item, returning nil if no work is available.
// Returns an error if the context is cancelled or the manager is shutting down.
func (m *wqManager) RequestWork(ctx context.Context) (*WorkItem, error) {
	// Check if manager is already shut down
	select {
	case <-m.done:
		return nil, errors.New("work queue manager is shut down")
	default:
	}

	req := &workRequest{
		resp: make(chan *workRequestResponse, 1),
	}

	// Try to send request with context cancellation
	select {
	case m.getWork <- req:
		// Request sent successfully
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.done:
		return nil, errors.New("work queue manager is shut down")
	}

	// Wait for response with context cancellation
	select {
	case work := <-req.resp:
		if work.work != nil {
			// Track this work item for graceful shutdown
			m.outstandingWork.Add(1)
		}
		return work.work, work.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.done:
		return nil, errors.New("work queue manager is shut down")
	}
}

// WaitForOutstandingWork waits for all outstanding work items to complete.
// Returns when all work is done or the context is cancelled.
func (m *wqManager) WaitForOutstandingWork(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.outstandingWork.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
