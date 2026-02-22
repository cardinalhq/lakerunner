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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
	"github.com/cardinalhq/lakerunner/queryworker/controlstream"
)

// mockExecutor records calls and returns configurable results.
type mockExecutor struct {
	mu       sync.Mutex
	calls    []executorCall
	resultFn func(workID string, spec []byte) (*ArtifactResult, error)
	blockCh  chan struct{} // if set, block until closed
}

type executorCall struct {
	workID string
	spec   []byte
}

func (m *mockExecutor) Execute(ctx context.Context, workID string, spec []byte) (*ArtifactResult, error) {
	m.mu.Lock()
	m.calls = append(m.calls, executorCall{workID: workID, spec: spec})
	blockCh := m.blockCh
	fn := m.resultFn
	m.mu.Unlock()

	if blockCh != nil {
		select {
		case <-blockCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if fn != nil {
		return fn(workID, spec)
	}
	return &ArtifactResult{
		ArtifactURL:       "http://localhost/artifacts/" + workID,
		ArtifactSizeBytes: 1024,
		ArtifactChecksum:  "sha256:test",
		RowCount:          42,
		MinTs:             1000,
		MaxTs:             2000,
	}, nil
}

// newTestSession creates a Session suitable for testing, with a buffered send channel.
func newTestSession(id string) *controlstream.Session {
	_, cancel := context.WithCancel(context.Background())
	return controlstream.NewTestSession(id, cancel)
}

func waitForMessage(t *testing.T, sess *controlstream.Session, timeout time.Duration) *workcoordpb.WorkerMessage {
	t.Helper()
	select {
	case msg := <-sess.SendCh():
		return msg
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func TestNewManager_Defaults(t *testing.T) {
	mgr := NewManager(nil, 0, nil)
	assert.Equal(t, 4, mgr.maxInFlight)
	assert.Equal(t, 0, mgr.InFlightCount())
}

func TestNewManager_CustomInFlight(t *testing.T) {
	mgr := NewManager(nil, 8, nil)
	assert.Equal(t, 8, mgr.maxInFlight)
}

func TestOnAssignWork_AcceptsAndExecutes(t *testing.T) {
	exec := &mockExecutor{}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{
		WorkId: "w1",
		Spec:   []byte("test-spec"),
	})

	// Should get WorkAccepted.
	msg := waitForMessage(t, sess, 5*time.Second)
	accepted := msg.GetWorkAccepted()
	require.NotNil(t, accepted)
	assert.Equal(t, "w1", accepted.WorkId)

	// Should eventually get WorkReady.
	msg = waitForMessage(t, sess, 5*time.Second)
	ready := msg.GetWorkReady()
	require.NotNil(t, ready)
	assert.Equal(t, "w1", ready.WorkId)
	assert.Equal(t, int64(42), ready.RowCount)
	assert.Equal(t, "sha256:test", ready.ArtifactChecksum)

	// Work item should be cleaned up.
	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 0
	}, 3*time.Second, 50*time.Millisecond)
}

func TestOnAssignWork_RejectsWhenDraining(t *testing.T) {
	mgr := NewManager(nil, 4, nil)
	mgr.SetDraining(true)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1"})

	msg := waitForMessage(t, sess, 5*time.Second)
	rejected := msg.GetWorkRejected()
	require.NotNil(t, rejected)
	assert.Equal(t, "w1", rejected.WorkId)
	assert.Contains(t, rejected.Reason, "draining")
}

func TestOnAssignWork_RejectsAtCapacity(t *testing.T) {
	blockCh := make(chan struct{})
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 1, nil)
	sess := newTestSession("sess-1")

	// Fill the single slot.
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("s1")})
	msg := waitForMessage(t, sess, 5*time.Second)
	require.NotNil(t, msg.GetWorkAccepted())

	// Second assignment should be rejected.
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w2", Spec: []byte("s2")})
	msg = waitForMessage(t, sess, 5*time.Second)
	rejected := msg.GetWorkRejected()
	require.NotNil(t, rejected)
	assert.Equal(t, "w2", rejected.WorkId)
	assert.Contains(t, rejected.Reason, "capacity")

	// Unblock and let first work complete.
	close(blockCh)
}

func TestOnAssignWork_ExecutionFailure(t *testing.T) {
	exec := &mockExecutor{
		resultFn: func(string, []byte) (*ArtifactResult, error) {
			return nil, fmt.Errorf("OOM killed")
		},
	}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("spec")})

	// WorkAccepted first.
	msg := waitForMessage(t, sess, 5*time.Second)
	require.NotNil(t, msg.GetWorkAccepted())

	// Then WorkFailed.
	msg = waitForMessage(t, sess, 5*time.Second)
	failed := msg.GetWorkFailed()
	require.NotNil(t, failed)
	assert.Equal(t, "w1", failed.WorkId)
	assert.Contains(t, failed.Reason, "OOM killed")
}

func TestOnCancelWork_CancelsInFlight(t *testing.T) {
	blockCh := make(chan struct{})
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("spec")})

	// Consume WorkAccepted.
	msg := waitForMessage(t, sess, 5*time.Second)
	require.NotNil(t, msg.GetWorkAccepted())

	assert.Equal(t, 1, mgr.InFlightCount())

	// Cancel the work.
	mgr.OnCancelWork(sess, &workcoordpb.CancelWork{WorkId: "w1"})

	// Should clean up (the canceled context unblocks the executor).
	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestOnCancelWork_UnknownWorkID(t *testing.T) {
	mgr := NewManager(nil, 4, nil)
	sess := newTestSession("sess-1")
	// Should not panic.
	mgr.OnCancelWork(sess, &workcoordpb.CancelWork{WorkId: "nonexistent"})
}

func TestOnSessionClosed_CancelsSessionWork(t *testing.T) {
	blockCh := make(chan struct{})
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	// Assign two work items to this session.
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("s1")})
	waitForMessage(t, sess, 5*time.Second) // consume accepted
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w2", Spec: []byte("s2")})
	waitForMessage(t, sess, 5*time.Second) // consume accepted

	assert.Equal(t, 2, mgr.InFlightCount())

	// Close the session.
	mgr.OnSessionClosed("sess-1")

	// Both should be canceled.
	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestOnSessionClosed_OnlyCancelsMatchingSession(t *testing.T) {
	blockCh := make(chan struct{})
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 4, nil)
	sess1 := newTestSession("sess-1")
	sess2 := newTestSession("sess-2")

	mgr.OnAssignWork(sess1, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("s1")})
	waitForMessage(t, sess1, 5*time.Second)
	mgr.OnAssignWork(sess2, &workcoordpb.AssignWork{WorkId: "w2", Spec: []byte("s2")})
	waitForMessage(t, sess2, 5*time.Second)

	assert.Equal(t, 2, mgr.InFlightCount())

	// Close only sess-1.
	mgr.OnSessionClosed("sess-1")

	// w1 should be canceled, w2 should still be running.
	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 1
	}, 5*time.Second, 50*time.Millisecond)

	// Unblock w2 to clean up.
	close(blockCh)
	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestSetDraining(t *testing.T) {
	mgr := NewManager(nil, 4, nil)
	assert.False(t, mgr.draining.Load())

	mgr.SetDraining(true)
	assert.True(t, mgr.draining.Load())

	mgr.SetDraining(false)
	assert.False(t, mgr.draining.Load())
}

func TestWaitForDrain_ReturnsImmediately(t *testing.T) {
	mgr := NewManager(nil, 4, nil)
	err := mgr.WaitForDrain(t.Context())
	assert.NoError(t, err)
}

func TestWaitForDrain_WaitsForCompletion(t *testing.T) {
	blockCh := make(chan struct{})
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("spec")})
	waitForMessage(t, sess, 5*time.Second) // consume accepted

	// Start draining in background.
	done := make(chan error, 1)
	go func() {
		done <- mgr.WaitForDrain(t.Context())
	}()

	// Should not finish yet.
	select {
	case <-done:
		t.Fatal("WaitForDrain returned too early")
	case <-time.After(200 * time.Millisecond):
	}

	// Unblock the work.
	close(blockCh)

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForDrain timed out")
	}
}

func TestWaitForDrain_Timeout(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)
	exec := &mockExecutor{blockCh: blockCh}
	mgr := NewManager(exec, 4, nil)
	sess := newTestSession("sess-1")

	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("spec")})
	waitForMessage(t, sess, 5*time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	err := mgr.WaitForDrain(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "drain timed out")
}

func TestOnArtifactAck(t *testing.T) {
	mgr := NewManager(nil, 4, nil)
	sess := newTestSession("sess-1")
	// Should not panic.
	mgr.OnArtifactAck(sess, &workcoordpb.ArtifactAck{WorkId: "w1"})
}

func TestSemaphoreReleasedOnFailure(t *testing.T) {
	callCount := 0
	exec := &mockExecutor{
		resultFn: func(string, []byte) (*ArtifactResult, error) {
			callCount++
			if callCount == 1 {
				return nil, fmt.Errorf("fail first")
			}
			return &ArtifactResult{ArtifactURL: "url", RowCount: 1}, nil
		},
	}
	mgr := NewManager(exec, 1, nil)
	sess := newTestSession("sess-1")

	// First work item will fail.
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w1", Spec: []byte("spec")})
	waitForMessage(t, sess, 5*time.Second) // accepted
	waitForMessage(t, sess, 5*time.Second) // failed

	require.Eventually(t, func() bool {
		return mgr.InFlightCount() == 0
	}, 3*time.Second, 50*time.Millisecond)

	// Second work item should succeed (semaphore was released).
	mgr.OnAssignWork(sess, &workcoordpb.AssignWork{WorkId: "w2", Spec: []byte("spec2")})
	waitForMessage(t, sess, 5*time.Second) // accepted
	msg := waitForMessage(t, sess, 5*time.Second)
	require.NotNil(t, msg.GetWorkReady())
	assert.Equal(t, "w2", msg.GetWorkReady().WorkId)
}
