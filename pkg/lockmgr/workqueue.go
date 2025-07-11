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

package lockmgr

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

// wqManager drives fetching items from the DB and heartbeating them.
type wqManager struct {
	// constructor parameters
	mdb             lrdb.StoreFull
	workerID        int64
	signal          lrdb.SignalEnum
	action          lrdb.ActionEnum
	frequencies     []int32
	minimumPriority int32

	// defaults but set via options
	heartbeatInterval time.Duration
	ll                *slog.Logger

	// currently acquired work item IDs
	acquiredIDs []int64

	// channels for internal processing
	getWork      chan *workRequest
	completeWork chan *workCompleteRequest
	failWork     chan *workFailRequest
}

type workCompleteRequest struct {
	WorkItem *WorkItem
	resp     chan error
}

func (m *wqManager) completeWorkItem(ctx context.Context, w *WorkItem) error {
	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})

	err := m.mdb.WorkQueueComplete(ctx, lrdb.WorkQueueCompleteParams{
		ID:       w.id,
		WorkerID: m.workerID,
	})
	return err
}

type workFailRequest struct {
	WorkItem *WorkItem
	resp     chan error
}

func (m *wqManager) failWorkItem(ctx context.Context, w *WorkItem) error {
	m.acquiredIDs = slices.DeleteFunc(m.acquiredIDs, func(id int64) bool {
		return id == w.id
	})
	err := m.mdb.WorkQueueFail(ctx, lrdb.WorkQueueFailParams{
		ID:       w.id,
		WorkerID: m.workerID,
	})
	return err
}

// NewWorkQueueManager constructs a manager to manage obtaining and completing work items.
// The work items are automatically heartbeated to retain ownership of the work items.
func NewWorkQueueManager(
	mdb lrdb.StoreFull,
	workerID int64,
	sig lrdb.SignalEnum,
	act lrdb.ActionEnum,
	frequencies []int32,
	minimumPriority int32,
	opts ...Options,
) WorkQueueManager {
	m := &wqManager{
		mdb:               mdb,
		workerID:          workerID,
		signal:            sig,
		action:            act,
		frequencies:       frequencies,
		minimumPriority:   minimumPriority,
		getWork:           make(chan *workRequest, 5),
		completeWork:      make(chan *workCompleteRequest, 5),
		failWork:          make(chan *workFailRequest, 5),
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
	RequestWork() (*WorkItem, error)
}

var _ WorkQueueManager = (*wqManager)(nil)

// Run starts a background goroutine that listens for work‐requests (on getWork) and
// periodically heartbeats all acquired IDs.  When ctx is canceled, the loop exits.
func (m *wqManager) Run(ctx context.Context) {
	go m.runLoop(ctx)
}

// runLoop is the internal processing loop.
func (m *wqManager) runLoop(ctx context.Context) {
	if m.ll == nil {
		m.ll = slog.Default()
	}

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

		case <-time.Tick(m.heartbeatInterval):
			m.heartbeat(ctx)
		}
	}
}

// GetWork queries the DB for the next row and records its ID in acquiredIDs.
func (m *wqManager) getWorkItem(ctx context.Context) (*WorkItem, error) {
	row, err := m.mdb.WorkQueueClaim(ctx, lrdb.WorkQueueClaimParams{
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
	}

	m.acquiredIDs = append(m.acquiredIDs, work.id)
	return work, nil
}

func (m *wqManager) heartbeat(ctx context.Context) {
	err := m.mdb.WorkQueueHeartbeat(ctx, lrdb.WorkQueueHeartbeatParams{
		Ids:      m.acquiredIDs,
		WorkerID: m.workerID,
	})
	if err != nil {
		m.ll.Error("failed to heartbeat work queue (continuing)", "error", err)
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
func (m *wqManager) RequestWork() (*WorkItem, error) {
	req := &workRequest{
		resp: make(chan *workRequestResponse, 1),
	}

	m.getWork <- req
	work := <-req.resp
	return work.work, work.err
}
