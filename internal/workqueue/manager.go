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
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// DB defines the database operations needed by the work queue manager.
type DB interface {
	WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error)
	WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error
	WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error)
	WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error
	WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error)
}

// Manager drives fetching items from the DB and heartbeating them.
type Manager struct {
	db       DB
	workerID int64
	taskName string

	heartbeatInterval time.Duration
	maxRetries        int32

	acquiredIDs []int64

	outstandingWork sync.WaitGroup
	shutdownOnce    sync.Once
	done            chan struct{}

	getWork      chan *workRequest
	completeWork chan *workCompleteRequest
	failWork     chan *workFailRequest
}

type workCompleteRequest struct {
	workItem *WorkItem
	resp     chan error
}

func (m *Manager) completeWorkItem(ctx context.Context, w *WorkItem) error {
	defer w.markDone()

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
	workItem     *WorkItem
	failedReason *string
	resp         chan error
}

func (m *Manager) failWorkItem(ctx context.Context, w *WorkItem, reason *string) error {
	defer w.markDone()

	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})

	_, err := m.db.WorkQueueFail(ctx, lrdb.WorkQueueFailParams{
		MaxRetries:   m.maxRetries,
		FailedReason: reason,
		ID:           w.id,
		WorkerID:     m.workerID,
	})
	return err
}

// NewManager constructs a manager to handle obtaining and completing work items.
// Work items are automatically heartbeated to retain ownership.
func NewManager(db DB, workerID int64, taskName string, opts ...Option) *Manager {
	m := &Manager{
		db:                db,
		workerID:          workerID,
		taskName:          taskName,
		done:              make(chan struct{}),
		getWork:           make(chan *workRequest, 5),
		completeWork:      make(chan *workCompleteRequest, 5),
		failWork:          make(chan *workFailRequest, 5),
		heartbeatInterval: time.Minute,
		maxRetries:        5,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Option configures a Manager.
type Option func(*Manager)

// WithHeartbeatInterval sets the heartbeat interval for the manager.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.heartbeatInterval = interval
	}
}

// WithMaxRetries sets the maximum number of retries before a work item is marked as permanently failed.
func WithMaxRetries(maxRetries int32) Option {
	return func(m *Manager) {
		m.maxRetries = maxRetries
	}
}

// Run starts a background goroutine that listens for work requests and
// periodically heartbeats all acquired IDs. When ctx is canceled, the loop exits.
func (m *Manager) Run(ctx context.Context) {
	go m.runLoop(ctx)
}

func (m *Manager) runLoop(ctx context.Context) {
	defer m.shutdownOnce.Do(func() {
		close(m.done)
	})

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

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
			req.resp <- m.completeWorkItem(ctx, req.workItem)

		case req := <-m.failWork:
			req.resp <- m.failWorkItem(ctx, req.workItem, req.failedReason)

		case <-ticker.C:
			m.heartbeat(ctx)
		}
	}
}

func (m *Manager) getWorkItem(ctx context.Context) (*WorkItem, error) {
	row, err := m.db.WorkQueueClaim(ctx, lrdb.WorkQueueClaimParams{
		WorkerID: m.workerID,
		TaskName: m.taskName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No work available - sleep to avoid hammering the database
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
			}
			return nil, nil
		}
		if strings.Contains(err.Error(), "23P01") {
			// Serialization failure - sleep to avoid hammering the database
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
			}
			return nil, nil
		}
		return nil, err
	}

	work := &WorkItem{
		id:             row.ID,
		taskName:       row.TaskName,
		organizationID: row.OrganizationID,
		instanceNum:    row.InstanceNum,
		spec:           row.Spec,
		tries:          row.Tries,
		mgr:            m,
	}

	m.acquiredIDs = append(m.acquiredIDs, work.id)
	return work, nil
}

func (m *Manager) heartbeat(ctx context.Context) {
	if len(m.acquiredIDs) == 0 {
		return
	}

	err := m.db.WorkQueueHeartbeat(ctx, lrdb.WorkQueueHeartbeatParams{
		Ids:      m.acquiredIDs,
		WorkerID: m.workerID,
	})
	if err != nil {
		ll := logctx.FromContext(ctx)
		ll.Error("failed to heartbeat work queue", slog.Any("error", err))
	}
}

type workRequest struct {
	resp chan *workRequestResponse
}

type workRequestResponse struct {
	work *WorkItem
	err  error
}

// RequestWork requests the next work item, returning nil if no work is available.
// Returns an error if the context is cancelled or the manager is shutting down.
func (m *Manager) RequestWork(ctx context.Context) (*WorkItem, error) {
	select {
	case <-m.done:
		return nil, errors.New("work queue manager is shut down")
	default:
	}

	req := &workRequest{
		resp: make(chan *workRequestResponse, 1),
	}

	select {
	case m.getWork <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.done:
		return nil, errors.New("work queue manager is shut down")
	}

	select {
	case work := <-req.resp:
		if work.work != nil {
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
func (m *Manager) WaitForOutstandingWork(ctx context.Context) error {
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
